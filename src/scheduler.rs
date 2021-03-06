use async_std::prelude::*;
use async_std::sync::{channel, Receiver, Sender};
use async_std::task;

use crate::context::Context;
use crate::imap::Imap;
use crate::job::{self, Thread};
use crate::smtp::Smtp;

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

    pub(crate) async fn interrupt_inbox(&self, probe_network: bool) {
        self.scheduler
            .read()
            .await
            .interrupt_inbox(probe_network)
            .await;
    }

    pub(crate) async fn interrupt_sentbox(&self, probe_network: bool) {
        self.scheduler
            .read()
            .await
            .interrupt_sentbox(probe_network)
            .await;
    }

    pub(crate) async fn interrupt_mvbox(&self, probe_network: bool) {
        self.scheduler
            .read()
            .await
            .interrupt_mvbox(probe_network)
            .await;
    }

    pub(crate) async fn interrupt_smtp(&self, probe_network: bool) {
        self.scheduler
            .read()
            .await
            .interrupt_smtp(probe_network)
            .await;
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
        let mut jobs_loaded = 0;
        let mut probe_network = false;
        loop {
            match job::load_next(&ctx, Thread::Imap, probe_network).await {
                Some(job) if jobs_loaded <= 20 => {
                    jobs_loaded += 1;
                    job::perform_job(&ctx, job::Connection::Inbox(&mut connection), job).await;
                    probe_network = false;
                }
                Some(job) => {
                    // Let the fetch run, but return back to the job afterwards.
                    info!(ctx, "postponing imap-job {} to run fetch...", job);
                    jobs_loaded = 0;
                    fetch(&ctx, &mut connection).await;
                }
                None => {
                    jobs_loaded = 0;
                    probe_network =
                        fetch_idle(&ctx, &mut connection, "configured_inbox_folder").await;
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
    match get_watch_folder(&ctx, "configured_inbox_folder").await {
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

async fn fetch_idle(ctx: &Context, connection: &mut Imap, folder: &str) -> bool {
    match get_watch_folder(&ctx, folder).await {
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
                        false
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
    folder: impl AsRef<str>,
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
            fetch_idle(&ctx, &mut connection, folder.as_ref()).await;
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

        let mut probe_network = false;
        loop {
            match job::load_next(&ctx, Thread::Smtp, probe_network).await {
                Some(job) => {
                    info!(ctx, "executing smtp job");
                    job::perform_job(&ctx, job::Connection::Smtp(&mut connection), job).await;
                    probe_network = false;
                }
                None => {
                    // Fake Idle
                    info!(ctx, "smtp fake idle - started");
                    probe_network = idle_interrupt_receiver.recv().await.unwrap_or_default();
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
                    "configured_mvbox_folder",
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
                    "configured_sentbox_folder",
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

        self.interrupt_inbox(true)
            .join(self.interrupt_mvbox(true))
            .join(self.interrupt_sentbox(true))
            .join(self.interrupt_smtp(true))
            .await;
    }

    async fn interrupt_inbox(&self, probe_network: bool) {
        if let Scheduler::Running { ref inbox, .. } = self {
            inbox.interrupt(probe_network).await;
        }
    }

    async fn interrupt_mvbox(&self, probe_network: bool) {
        if let Scheduler::Running { ref mvbox, .. } = self {
            mvbox.interrupt(probe_network).await;
        }
    }

    async fn interrupt_sentbox(&self, probe_network: bool) {
        if let Scheduler::Running { ref sentbox, .. } = self {
            sentbox.interrupt(probe_network).await;
        }
    }

    async fn interrupt_smtp(&self, probe_network: bool) {
        if let Scheduler::Running { ref smtp, .. } = self {
            smtp.interrupt(probe_network).await;
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
    idle_interrupt_sender: Sender<bool>,
}

impl ConnectionState {
    /// Shutdown this connection completely.
    async fn stop(&self) {
        // Trigger shutdown of the run loop.
        self.stop_sender.send(()).await;
        // Wait for a notification that the run loop has been shutdown.
        self.shutdown_receiver.recv().await.ok();
    }

    async fn interrupt(&self, probe_network: bool) {
        // Use try_send to avoid blocking on interrupts.
        self.idle_interrupt_sender.try_send(probe_network).ok();
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
    async fn interrupt(&self, probe_network: bool) {
        self.state.interrupt(probe_network).await;
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
    idle_interrupt_receiver: Receiver<bool>,
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
    async fn interrupt(&self, probe_network: bool) {
        self.state.interrupt(probe_network).await;
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

async fn get_watch_folder(context: &Context, config_name: impl AsRef<str>) -> Option<String> {
    match context
        .sql
        .get_raw_config(context, config_name.as_ref())
        .await
    {
        Some(name) => Some(name),
        None => {
            if config_name.as_ref() == "configured_inbox_folder" {
                // initialized with old version, so has not set configured_inbox_folder
                Some("INBOX".to_string())
            } else {
                None
            }
        }
    }
}
