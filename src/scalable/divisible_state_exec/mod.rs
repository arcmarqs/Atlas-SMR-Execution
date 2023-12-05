use std::sync::Arc;
use std::time::Instant;

use atlas_common::maybe_vec::MaybeVec;
use atlas_smr_application::{ExecutionRequest, ExecutorHandle};
use atlas_smr_application::app::{Request, AppData, Application, BatchReplies, Reply};
use atlas_smr_application::state::divisible_state::{DivisibleState, InstallStateMessage, AppStateMessage, AppState};
use scoped_threadpool::Pool;

use atlas_common::channel;
use atlas_common::channel::{ChannelSyncRx, ChannelSyncTx};
use atlas_common::error::*;

use atlas_common::ordering::{Orderable, SeqNo};
use atlas_core::smr::exec::ReplyNode;
use atlas_metrics::metrics::metric_duration;

use crate::ExecutorReplier;
use crate::metric::{EXECUTION_LATENCY_TIME_ID, EXECUTION_TIME_TAKEN_ID};
use crate::scalable::{CRUDState, scalable_execution, scalable_unordered_execution, ScalableApp, THREAD_POOL_THREADS};

const EXECUTING_BUFFER: usize = 16384;
const STATE_BUFFER: usize = 128;

const PARTS_PER_DELIVERY: usize = 4;

pub struct ScalableDivisibleStateExecutor<S, A, NT>
    where S: DivisibleState + CRUDState + 'static + Send + Sync,
          A: ScalableApp<S> + 'static,
          NT: 'static {
    application: A,
    state: S,

    work_rx: ChannelSyncRx<ExecutionRequest<Request<A, S>>>,
    state_rx: ChannelSyncRx<InstallStateMessage<S>>,
    checkpoint_tx: ChannelSyncTx<AppStateMessage<S>>,

    thread_pool: Pool,

    send_node: Arc<NT>,
}

impl<S, A, NT> ScalableDivisibleStateExecutor<S, A, NT>
    where S: DivisibleState + CRUDState + 'static + Send + Sync,
          A: ScalableApp<S> + 'static + Send {
    pub fn init_handle() -> (ExecutorHandle<AppData<A, S>>, ChannelSyncRx<ExecutionRequest<Request<A, S>>>) {
        let (tx, rx) = channel::new_bounded_sync(EXECUTING_BUFFER,
                                                 Some("Scalable Work Handle"));

        (ExecutorHandle::new(tx), rx)
    }

    pub fn init<T>(
        handle: ChannelSyncRx<ExecutionRequest<Request<A, S>>>,
        initial_state: Option<(S, Vec<Request<A, S>>)>,
        mut service: A,
        send_node: Arc<NT>)
        -> Result<(ChannelSyncTx<InstallStateMessage<S>>, ChannelSyncRx<AppStateMessage<S>>)>
        where T: ExecutorReplier + 'static,
              NT: ReplyNode<AppData<A, S>> + 'static {
        let (state, requests) = if let Some(state) = initial_state {
            state
        } else {
            (<A as Application<S>>::initial_state()?, vec![])
        };

        let (state_tx, state_rx) = channel::new_bounded_sync(STATE_BUFFER,
                                                             Some("Install State Work Handle"));

        let (checkpoint_tx, checkpoint_rx) = channel::new_bounded_sync(STATE_BUFFER,
                                                                       Some("App State Checkpoint Work Handle"));

        let mut executor = ScalableDivisibleStateExecutor {
            application: service,
            state,
            work_rx: handle,
            state_rx,
            checkpoint_tx,
            thread_pool: Pool::new(THREAD_POOL_THREADS),
            send_node,
        };

        for request in requests {
            executor.application.update(&mut executor.state, request);
        }

        executor.run::<T>();

        Ok((state_tx, checkpoint_rx))
    }

    fn run<T>(mut self)
        where T: ExecutorReplier + 'static,
              NT: ReplyNode<AppData<A, S>> + 'static {
        std::thread::Builder::new()
            .name(format!("Executor thread"))
            .spawn(move || {
                while let Ok(exec_req) = self.work_rx.recv() {
                    match exec_req {
                        ExecutionRequest::PollStateChannel => {
                            // Receive all state updates that are available
                            while let Ok(state_recvd) = self.state_rx.recv() {
                                match state_recvd {
                                    InstallStateMessage::StateDescriptor(descriptor) => {}
                                    InstallStateMessage::StatePart(state_part) => {
                                        self.state.accept_parts(state_part.into_vec().into_boxed_slice()).expect("Failed to install state parts into executor");
                                    }
                                    InstallStateMessage::Done => {
                                        self.state.finalize_transfer();
                                        break
                                    }
                                    InstallStateMessage::StateDescriptor(_) => todo!(),
                                }
                            }
                        }
                        ExecutionRequest::CatchUp(requests) => {
                            for batch in requests.into_iter() {
                                let seq_no = batch.sequence_number();

                                let start = Instant::now();

                                let reply_batch = scalable_execution(&mut self.thread_pool, &self.application, &mut self.state, batch);

                                metric_duration(EXECUTION_TIME_TAKEN_ID, start.elapsed());

                                self.execution_finished::<T>(Some(seq_no), reply_batch);
                            }
                        }
                        ExecutionRequest::Update((batch, instant)) => {
                            let seq_no = batch.sequence_number();

                            metric_duration(EXECUTION_LATENCY_TIME_ID, instant.elapsed());

                            let start = Instant::now();

                            let reply_batch = scalable_execution(&mut self.thread_pool, &self.application, &mut self.state, batch);

                            metric_duration(EXECUTION_TIME_TAKEN_ID, start.elapsed());

                            // deliver replies
                            self.execution_finished::<T>(Some(seq_no), reply_batch);
                        }
                        ExecutionRequest::UpdateAndGetAppstate((batch, instant)) => {
                            let seq_no = batch.sequence_number();

                            metric_duration(EXECUTION_LATENCY_TIME_ID, instant.elapsed());

                            let start = Instant::now();

                            let reply_batch = scalable_execution(&mut self.thread_pool, &self.application, &mut self.state, batch);

                            metric_duration(EXECUTION_TIME_TAKEN_ID, start.elapsed());

                            // deliver replies
                            self.execution_finished::<T>(Some(seq_no), reply_batch);

                            // deliver checkpoint state to the replica
                            self.deliver_checkpoint_state(seq_no);
                        }
                        ExecutionRequest::Read(_peer_id) => {
                            todo!()
                        }
                        ExecutionRequest::ExecuteUnordered(batch) => {
                            let reply = scalable_unordered_execution(&mut self.thread_pool, &self.application, &self.state, batch);

                            self.execution_finished::<T>(None, reply);
                        }
                    }
                }
            }).expect("Failed to start executor thread");
    }


    ///Clones the current state and delivers it to the application
    /// Takes a sequence number, which corresponds to the last executed consensus instance before we performed the checkpoint
    fn deliver_checkpoint_state(&mut self, seq: SeqNo) {
        let parts = self.state.get_parts().expect("Failed to get necessary parts");
        let desc: AppState<S> = AppState::StateDescriptor(self.state.get_descriptor());
        let state = AppState::StatePart(MaybeVec::from_many(parts));

        self.checkpoint_tx.send_return(AppStateMessage::new(seq, desc));

        self.checkpoint_tx.send_return(AppStateMessage::new(seq, state));

        self.checkpoint_tx.send_return(AppStateMessage::new(seq, AppState::Done)).expect("Failed to send checkpoint");
    }

    fn execution_finished<T>(&self, seq: Option<SeqNo>, batch: BatchReplies<Reply<A, S>>)
        where NT: ReplyNode<AppData<A, S>> + 'static,
              T: ExecutorReplier + 'static {
        let send_node = self.send_node.clone();

        /*{
            if let Some(seq) = seq {
                if let Some(observer_handle) = &self.observer_handle {
                    //Do not notify of unordered events
                    let observe_event = MessageType::Event(ObserveEventKind::Executed(seq));

                    if let Err(err) = observer_handle.tx().send(observe_event) {
                        error!("{:?}", err);
                    }
                }
            }
        }*/

        T::execution_finished::<AppData<A, S>, NT>(send_node, seq, batch);
    }
}