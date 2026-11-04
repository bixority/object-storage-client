//! Messages and the pure reducer: no I/O, no async, no egui.

mod close;
mod conn;
mod job;
mod nav;
#[cfg(test)]
mod tests;

use super::location::Location;
use super::model::{
    AppState, Batch, Closing, Dialog, Encryption, EntryKey, JobId, OnExisting, OpKind, Outcome,
    Page, PanelState, Plan, Side,
};
use crate::connection::{Connection, Discovery};
use std::sync::Arc;

/// A change to the selection of one panel. Rows are named by their key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum SelectOp {
    Only(EntryKey),
    Toggle(EntryKey),
    Range(EntryKey),
    All,
    Clear,
    /// Keyboard motion of the cursor; with `extend`, the selection grows
    /// from the anchor to the new row.
    Move {
        motion: Motion,
        extend: bool,
    },
}

/// Where keyboard motion takes the cursor. The page variants carry the
/// number of visible rows (at least 1).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Motion {
    Up,
    Down,
    PageUp(usize),
    PageDown(usize),
    Home,
    End,
}

/// What the user did.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Action {
    Navigate(Side, String),
    AddressEdited(Side, String),
    Up(Side),
    Open(Side, String),
    Refresh(Side),
    LoadMore(Side),
    Retry(Side),
    Select(Side, SelectOp),
    Activate(Side),
    SwitchPanel,
    Copy,
    Move,
    StartRename,
    Delete,
    DialogText(String),
    DialogAnswer(Answer),
    CancelJob,
    /// The panel switches to the connection `id` and shows its root.
    SelectConnection {
        side: Side,
        id: String,
    },
    /// Search for connections again.
    Rescan,
    /// Log in to the SSO connection of the panel.
    LogIn {
        side: Side,
    },
    CloseRequested,
    /// Ctrl+L: focus the panel's address field. Handled by the window; the
    /// reducer ignores it.
    FocusAddress(Side),
}

/// The user's answer to the open dialog.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Answer {
    Proceed,
    Overwrite,
    Skip,
    Cancel,
}

/// Work for the worker, decided by the reducer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Command {
    List {
        side: Side,
        seq: u64,
        conn: Arc<Connection>,
        url: String,
        token: Option<String>,
    },
    Plan {
        job: JobId,
        batch: Batch,
        src_conn: Arc<Connection>,
        dst_conn: Arc<Connection>,
    },
    Execute {
        job: JobId,
        src_conn: Arc<Connection>,
        dst_conn: Arc<Connection>,
        plan: Plan,
        on_existing: OnExisting,
    },
    Abort(JobId),
    /// Search for connections; reported by `Event::Connections`.
    Discover,
    /// Log in to the connection, then report `LoginCode` and `LoginDone`.
    Login {
        conn: Arc<Connection>,
    },
    CancelLogin,
    Shutdown,
}

/// What the worker reports back.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Event {
    Page {
        side: Side,
        seq: u64,
        first: bool,
        result: Result<Page, String>,
    },
    Planned {
        job: JobId,
        plan: Plan,
    },
    Progress {
        job: JobId,
        done: usize,
        total: usize,
        current: String,
    },
    Finished {
        job: JobId,
        outcome: Outcome,
    },
    /// What a scan found: first the local result, then (`done`) the final one
    /// with the Azure accounts.
    Connections {
        discovery: Discovery,
        done: bool,
    },
    /// The device login of `conn_id` waits for the user to approve `user_code`
    /// at `url`.
    LoginCode {
        conn_id: String,
        user_code: String,
        url: String,
    },
    LoginDone {
        conn_id: String,
        result: Result<(), String>,
    },
    ShutdownDone,
    /// SIGINT or SIGTERM: close without asking.
    Interrupted,
    WorkerGone,
}

/// An input to the reducer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Msg {
    Action(Action),
    Event(Event),
}

impl AppState {
    /// Both panels start on Local, loading their first page (`seq` 1), and a
    /// scan for connections starts; returns those commands.
    pub(super) fn new(encryption: Encryption) -> (Self, Vec<Command>) {
        let local = Arc::new(Connection::local());
        // The current directory, or the file system root if it is gone.
        let start = Location::parse(".")
            .or_else(|_| Location::parse("file:///"))
            .unwrap_or_else(|err| unreachable!("`file:///` parses: {err}"));
        let panels = [
            PanelState::new(Arc::clone(&local), start.clone()),
            PanelState::new(Arc::clone(&local), start),
        ];
        let mut commands: Vec<Command> = [Side::Left, Side::Right]
            .into_iter()
            .map(|side| nav::list(side, &panels[side.index()], None))
            .collect();
        commands.push(Command::Discover);
        let state = Self {
            panels,
            connections: vec![local],
            notes: Vec::new(),
            discovering: true,
            active: Side::Left,
            status: String::new(),
            encryption,
            closing: Closing::Open,
            next_job: 1,
            worker_alive: true,
            job: None,
            dialog: None,
            deferred: None,
        };
        (state, commands)
    }
}

#[cfg(test)]
impl AppState {
    /// A fresh state whose panels start on Local at `left` and `right`.
    pub(crate) fn at(left: Location, right: Location, encryption: Encryption) -> Self {
        let mut state = Self::new(encryption).0;
        for (panel, location) in state.panels.iter_mut().zip([left, right]) {
            panel.address_text = location.display();
            panel.location = location;
        }
        state
    }
}

/// Applies `msg` to `state` and returns the work to start.
pub(super) fn update(state: &mut AppState, msg: Msg) -> Vec<Command> {
    let mut commands = match msg {
        Msg::Action(action) => match action {
            Action::Navigate(side, text) => nav::navigate(state, side, &text),
            Action::AddressEdited(side, text) => nav::address_edited(state, side, text),
            Action::Up(side) => nav::up(state, side),
            Action::Open(side, name) => nav::open(state, side, &name),
            Action::Refresh(side) => nav::refresh(state, side),
            Action::LoadMore(side) => nav::load_more(state, side),
            Action::Retry(side) => nav::retry(state, side),
            Action::Select(side, op) => nav::select(state, side, op),
            Action::Activate(side) => {
                state.active = side;
                Vec::new()
            }
            Action::SwitchPanel => {
                state.active = state.active.other();
                Vec::new()
            }
            Action::Copy => job::transfer(state, OpKind::Copy),
            Action::Move => job::transfer(state, OpKind::Move),
            Action::StartRename => job::start_rename(state),
            Action::Delete => job::delete(state),
            Action::DialogText(text) => job::dialog_text(state, text),
            Action::DialogAnswer(answer) => match state.dialog {
                Some(Dialog::Quit) => close::quit_answer(state, answer),
                Some(Dialog::Login { .. }) if answer == Answer::Cancel => {
                    state.dialog = None;
                    vec![Command::CancelLogin]
                }
                _ => job::answer(state, answer),
            },
            Action::CancelJob => job::cancel(state),
            Action::SelectConnection { side, id } => conn::select(state, side, &id),
            Action::Rescan => conn::rescan(state),
            Action::LogIn { side } => conn::log_in(state, side),
            Action::CloseRequested => close::close_requested(state),
            Action::FocusAddress(_) => Vec::new(),
        },
        Msg::Event(Event::Page {
            side,
            seq,
            first,
            result,
        }) => nav::page(state, side, seq, first, result),
        Msg::Event(Event::Planned { job, plan }) => job::planned(state, job, plan),
        Msg::Event(Event::Progress {
            job,
            done,
            total,
            current,
        }) => job::progress(state, job, done, total, current),
        Msg::Event(Event::Finished { job, outcome }) => job::finished(state, job, outcome),
        Msg::Event(Event::Connections { discovery, done }) => {
            conn::connections(state, discovery, done)
        }
        Msg::Event(Event::LoginCode {
            conn_id,
            user_code,
            url,
        }) => conn::login_code(state, &conn_id, user_code, url),
        Msg::Event(Event::LoginDone { conn_id, result }) => {
            conn::login_done(state, &conn_id, result)
        }
        Msg::Event(Event::ShutdownDone) => close::shutdown_done(state),
        Msg::Event(Event::Interrupted) => close::interrupt(state),
        Msg::Event(Event::WorkerGone) => close::worker_gone(state),
    };
    if state.closing != Closing::Open {
        commands.retain(|command| !matches!(command, Command::List { .. }));
    }
    commands
}
