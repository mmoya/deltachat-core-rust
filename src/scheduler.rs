use async_std::prelude::*;
use async_std::sync::{channel, Receiver, Sender};
use async_std::task;

use crate::context::Context;
use crate::imap::Imap;
use crate::job::{self, Thread};
use crate::{config::Config, message::MsgId, smtp::Smtp};

pub(crate) struct StopToken;

/// Job and connection scheduler.
#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub(crate) enum Scheduler {
    Stopped,
    Running {
        inbox: ImapConnectionState,
        inbox_handle: Option<task::JoinHandle<()>>,
        mvbox: ImapConnectionState,
        mvbox_handle: Option<task::JoinHandle<()>>,
        sentbox: ImapConnectionState,
        sentbox_handle: Option<task::JoinHandle<()>>,
        smtp: SmtpConnectionState,
        smtp_handle: Option<task::JoinHandle<()>>,
    },
}

impl Context {
    /// Indicate that the network likely has come back.
    pub async fn maybe_network(&self) {
        self.scheduler.read().await.maybe_network().await;
    }

    pub(crate) async fn interrupt_inbox(&self, info: InterruptInfo) {
        self.scheduler.read().await.interrupt_inbox(info).await;
    }

    pub(crate) async fn interrupt_sentbox(&self, info: InterruptInfo) {
        self.scheduler.read().await.interrupt_sentbox(info).await;
    }

    pub(crate) async fn interrupt_mvbox(&self, info: InterruptInfo) {
        self.scheduler.read().await.interrupt_mvbox(info).await;
    }

    pub(crate) async fn interrupt_smtp(&self, info: InterruptInfo) {
        self.scheduler.read().await.interrupt_smtp(info).await;
    }
}

async fn inbox_loop(ctx: Context, started: Sender<()>, inbox_handlers: ImapConnectionHandlers) {
    use futures::future::FutureExt;

    info!(ctx, "starting inbox loop");
    let ImapConnectionHandlers {
        mut connection,
        stop_receiver,
        shutdown_sender,
    } = inbox_handlers;

    let ctx1 = ctx.clone();
    let fut = async move {
        let ctx = ctx1;
        if let Err(err) = connection.connect_configured(&ctx).await {
            error!(ctx, "{}", err);
            return;
        }

        started.send(()).await;

        // track number of continously executed jobs
        let mut jobs_loaded: i32 = 0;
        let mut info: InterruptInfo = Default::default();
        loop {
            match job::load_next(&ctx, Thread::Imap, &info).await {
                Some(job) if jobs_loaded <= 20 => {
                    jobs_loaded += 1;
                    job::perform_job(&ctx, job::Connection::Inbox(&mut connection), job).await;
                    info = Default::default();
                }
                Some(job) => {
                    // Let the fetch run, but return back to the job afterwards.
                    info!(ctx, "postponing imap-job {} to run fetch...", job);
                    jobs_loaded = 0;
                    fetch(&ctx, &mut connection).await;
                }
                None => {
                    jobs_loaded = 0;
                    info = fetch_idle(&ctx, &mut connection, Config::ConfiguredInboxFolder).await;
                }
            }
        }
    };

    stop_receiver
        .recv()
        .map(|_| {
            info!(ctx, "shutting down inbox loop");
        })
        .race(fut)
        .await;
    shutdown_sender.send(()).await;
}

async fn fetch(ctx: &Context, connection: &mut Imap) {
    match ctx.get_config(Config::ConfiguredInboxFolder).await {
        Some(watch_folder) => {
            // fetch
            if let Err(err) = connection.fetch(&ctx, &watch_folder).await {
                connection.trigger_reconnect();
                error!(ctx, "{}", err);
            }
        }
        None => {
            warn!(ctx, "Can not fetch inbox folder, not set");
            connection.fake_idle(&ctx, None).await;
        }
    }
}

async fn fetch_idle(ctx: &Context, connection: &mut Imap, folder: Config) -> InterruptInfo {
    match ctx.get_config(folder).await {
        Some(watch_folder) => {
            // fetch
            if let Err(err) = connection.fetch(&ctx, &watch_folder).await {
                connection.trigger_reconnect();
                error!(ctx, "{}", err);
            }

            // idle
            if connection.can_idle() {
                connection
                    .idle(&ctx, Some(watch_folder))
                    .await
                    .unwrap_or_else(|err| {
                        connection.trigger_reconnect();
                        error!(ctx, "{}", err);
                        InterruptInfo::new(false, None)
                    })
            } else {
                connection.fake_idle(&ctx, Some(watch_folder)).await
            }
        }
        None => {
            warn!(ctx, "Can not watch inbox folder, not set");
            connection.fake_idle(&ctx, None).await
        }
    }
}

async fn simple_imap_loop(
    ctx: Context,
    started: Sender<()>,
    inbox_handlers: ImapConnectionHandlers,
    folder: Config,
) {
    use futures::future::FutureExt;

    info!(ctx, "starting simple loop for {}", folder.as_ref());
    let ImapConnectionHandlers {
        mut connection,
        stop_receiver,
        shutdown_sender,
    } = inbox_handlers;

    let ctx1 = ctx.clone();

    let fut = async move {
        let ctx = ctx1;
        if let Err(err) = connection.connect_configured(&ctx).await {
            error!(ctx, "{}", err);
            return;
        }

        started.send(()).await;

        loop {
            fetch_idle(&ctx, &mut connection, folder).await;
        }
    };

    stop_receiver
        .recv()
        .map(|_| {
            info!(ctx, "shutting down simple loop");
        })
        .race(fut)
        .await;
    shutdown_sender.send(()).await;
}

async fn smtp_loop(ctx: Context, started: Sender<()>, smtp_handlers: SmtpConnectionHandlers) {
    use futures::future::FutureExt;

    info!(ctx, "starting smtp loop");
    let SmtpConnectionHandlers {
        mut connection,
        stop_receiver,
        shutdown_sender,
        idle_interrupt_receiver,
    } = smtp_handlers;

    let ctx1 = ctx.clone();
    let fut = async move {
        started.send(()).await;
        let ctx = ctx1;

        let mut interrupt_info = Default::default();
        loop {
            match job::load_next(&ctx, Thread::Smtp, &interrupt_info).await {
                Some(job) => {
                    info!(ctx, "executing smtp job");
                    job::perform_job(&ctx, job::Connection::Smtp(&mut connection), job).await;
                    interrupt_info = Default::default();
                }
                None => {
                    // Fake Idle
                    info!(ctx, "smtp fake idle - started");
                    interrupt_info = idle_interrupt_receiver.recv().await.unwrap_or_default();
                    info!(ctx, "smtp fake idle - interrupted")
                }
            }
        }
    };

    stop_receiver
        .recv()
        .map(|_| {
            info!(ctx, "shutting down smtp loop");
        })
        .race(fut)
        .await;
    shutdown_sender.send(()).await;
}

impl Scheduler {
    /// Start the scheduler, panics if it is already running.
    pub async fn start(&mut self, ctx: Context) {
        let (mvbox, mvbox_handlers) = ImapConnectionState::new();
        let (sentbox, sentbox_handlers) = ImapConnectionState::new();
        let (smtp, smtp_handlers) = SmtpConnectionState::new();
        let (inbox, inbox_handlers) = ImapConnectionState::new();

        *self = Scheduler::Running {
            inbox,
            mvbox,
            sentbox,
            smtp,
            inbox_handle: None,
            mvbox_handle: None,
            sentbox_handle: None,
            smtp_handle: None,
        };

        let (inbox_start_send, inbox_start_recv) = channel(1);
        if let Scheduler::Running { inbox_handle, .. } = self {
            let ctx1 = ctx.clone();
            *inbox_handle = Some(task::spawn(async move {
                inbox_loop(ctx1, inbox_start_send, inbox_handlers).await
            }));
        }

        let (mvbox_start_send, mvbox_start_recv) = channel(1);
        if let Scheduler::Running { mvbox_handle, .. } = self {
            let ctx1 = ctx.clone();
            *mvbox_handle = Some(task::spawn(async move {
                simple_imap_loop(
                    ctx1,
                    mvbox_start_send,
                    mvbox_handlers,
                    Config::ConfiguredMvboxFolder,
                )
                .await
            }));
        }

        let (sentbox_start_send, sentbox_start_recv) = channel(1);
        if let Scheduler::Running { sentbox_handle, .. } = self {
            let ctx1 = ctx.clone();
            *sentbox_handle = Some(task::spawn(async move {
                simple_imap_loop(
                    ctx1,
                    sentbox_start_send,
                    sentbox_handlers,
                    Config::ConfiguredSentboxFolder,
                )
                .await
            }));
        }

        let (smtp_start_send, smtp_start_recv) = channel(1);
        if let Scheduler::Running { smtp_handle, .. } = self {
            let ctx1 = ctx.clone();
            *smtp_handle = Some(task::spawn(async move {
                smtp_loop(ctx1, smtp_start_send, smtp_handlers).await
            }));
        }

        // wait for all loops to be started
        if let Err(err) = inbox_start_recv
            .recv()
            .try_join(mvbox_start_recv.recv())
            .try_join(sentbox_start_recv.recv())
            .try_join(smtp_start_recv.recv())
            .await
        {
            error!(ctx, "failed to start scheduler: {}", err);
        }

        info!(ctx, "scheduler is running");
    }

    async fn maybe_network(&self) {
        if !self.is_running() {
            return;
        }

        self.interrupt_inbox(InterruptInfo::new(true, None))
            .join(self.interrupt_mvbox(InterruptInfo::new(true, None)))
            .join(self.interrupt_sentbox(InterruptInfo::new(true, None)))
            .join(self.interrupt_smtp(InterruptInfo::new(true, None)))
            .await;
    }

    async fn interrupt_inbox(&self, info: InterruptInfo) {
        if let Scheduler::Running { ref inbox, .. } = self {
            inbox.interrupt(info).await;
        }
    }

    async fn interrupt_mvbox(&self, info: InterruptInfo) {
        if let Scheduler::Running { ref mvbox, .. } = self {
            mvbox.interrupt(info).await;
        }
    }

    async fn interrupt_sentbox(&self, info: InterruptInfo) {
        if let Scheduler::Running { ref sentbox, .. } = self {
            sentbox.interrupt(info).await;
        }
    }

    async fn interrupt_smtp(&self, info: InterruptInfo) {
        if let Scheduler::Running { ref smtp, .. } = self {
            smtp.interrupt(info).await;
        }
    }

    /// Halts the scheduler, must be called first, and then `stop`.
    pub(crate) async fn pre_stop(&self) -> StopToken {
        match self {
            Scheduler::Stopped => {
                panic!("WARN: already stopped");
            }
            Scheduler::Running {
                inbox,
                mvbox,
                sentbox,
                smtp,
                ..
            } => {
                inbox
                    .stop()
                    .join(mvbox.stop())
                    .join(sentbox.stop())
                    .join(smtp.stop())
                    .await;

                StopToken
            }
        }
    }

    /// Halt the scheduler, must only be called after pre_stop.
    pub(crate) async fn stop(&mut self, _t: StopToken) {
        match self {
            Scheduler::Stopped => {
                panic!("WARN: already stopped");
            }
            Scheduler::Running {
                inbox_handle,
                mvbox_handle,
                sentbox_handle,
                smtp_handle,
                ..
            } => {
                inbox_handle.take().expect("inbox not started").await;
                mvbox_handle.take().expect("mvbox not started").await;
                sentbox_handle.take().expect("sentbox not started").await;
                smtp_handle.take().expect("smtp not started").await;

                *self = Scheduler::Stopped;
            }
        }
    }

    /// Check if the scheduler is running.
    pub fn is_running(&self) -> bool {
        match self {
            Scheduler::Running { .. } => true,
            _ => false,
        }
    }
}

/// Connection state logic shared between imap and smtp connections.
#[derive(Debug)]
struct ConnectionState {
    /// Channel to notify that shutdown has completed.
    shutdown_receiver: Receiver<()>,
    /// Channel to interrupt the whole connection.
    stop_sender: Sender<()>,
    /// Channel to interrupt idle.
    idle_interrupt_sender: Sender<InterruptInfo>,
}

impl ConnectionState {
    /// Shutdown this connection completely.
    async fn stop(&self) {
        // Trigger shutdown of the run loop.
        self.stop_sender.send(()).await;
        // Wait for a notification that the run loop has been shutdown.
        self.shutdown_receiver.recv().await.ok();
    }

    async fn interrupt(&self, info: InterruptInfo) {
        // Use try_send to avoid blocking on interrupts.
        self.idle_interrupt_sender.try_send(info).ok();
    }
}

#[derive(Debug)]
pub(crate) struct SmtpConnectionState {
    state: ConnectionState,
}

impl SmtpConnectionState {
    fn new() -> (Self, SmtpConnectionHandlers) {
        let (stop_sender, stop_receiver) = channel(1);
        let (shutdown_sender, shutdown_receiver) = channel(1);
        let (idle_interrupt_sender, idle_interrupt_receiver) = channel(1);

        let handlers = SmtpConnectionHandlers {
            connection: Smtp::new(),
            stop_receiver,
            shutdown_sender,
            idle_interrupt_receiver,
        };

        let state = ConnectionState {
            idle_interrupt_sender,
            shutdown_receiver,
            stop_sender,
        };

        let conn = SmtpConnectionState { state };

        (conn, handlers)
    }

    /// Interrupt any form of idle.
    async fn interrupt(&self, info: InterruptInfo) {
        self.state.interrupt(info).await;
    }

    /// Shutdown this connection completely.
    async fn stop(&self) {
        self.state.stop().await;
    }
}

#[derive(Debug)]
struct SmtpConnectionHandlers {
    connection: Smtp,
    stop_receiver: Receiver<()>,
    shutdown_sender: Sender<()>,
    idle_interrupt_receiver: Receiver<InterruptInfo>,
}

#[derive(Debug)]
pub(crate) struct ImapConnectionState {
    state: ConnectionState,
}

impl ImapConnectionState {
    /// Construct a new connection.
    fn new() -> (Self, ImapConnectionHandlers) {
        let (stop_sender, stop_receiver) = channel(1);
        let (shutdown_sender, shutdown_receiver) = channel(1);
        let (idle_interrupt_sender, idle_interrupt_receiver) = channel(1);

        let handlers = ImapConnectionHandlers {
            connection: Imap::new(idle_interrupt_receiver),
            stop_receiver,
            shutdown_sender,
        };

        let state = ConnectionState {
            idle_interrupt_sender,
            shutdown_receiver,
            stop_sender,
        };

        let conn = ImapConnectionState { state };

        (conn, handlers)
    }

    /// Interrupt any form of idle.
    async fn interrupt(&self, info: InterruptInfo) {
        self.state.interrupt(info).await;
    }

    /// Shutdown this connection completely.
    async fn stop(&self) {
        self.state.stop().await;
    }
}

#[derive(Debug)]
struct ImapConnectionHandlers {
    connection: Imap,
    stop_receiver: Receiver<()>,
    shutdown_sender: Sender<()>,
}

#[derive(Default, Debug)]
pub struct InterruptInfo {
    pub probe_network: bool,
    pub msg_id: Option<MsgId>,
}

impl InterruptInfo {
    pub fn new(probe_network: bool, msg_id: Option<MsgId>) -> Self {
        Self {
            probe_network,
            msg_id,
        }
    }
}
