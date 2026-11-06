//! Connection rules: choosing a panel's connection, scanning, SSO login.

use super::{Command, nav};
use crate::gui::connection::{Connection, Discovery, Login};
use crate::gui::location::Location;
use crate::gui::model::{AppState, Closing, Dialog, Side};
use std::sync::Arc;

/// Switches `side` to the connection `id` and lists its root.
pub(super) fn select(state: &mut AppState, side: Side, id: &str) -> Vec<Command> {
    let Some(conn) = state.connections.iter().find(|conn| conn.id == id) else {
        return Vec::new();
    };
    let conn = Arc::clone(conn);
    match Location::parse(conn.provider.root()) {
        Ok(root) => {
            state.panels[side.index()].connection = conn;
            nav::go(state, side, root)
        }
        Err(err) => {
            state.panels[side.index()].address_error = Some(err);
            Vec::new()
        }
    }
}

/// Starts a new scan, which replaces one in flight.
pub(super) fn rescan(state: &mut AppState) -> Vec<Command> {
    if !state.worker_alive || state.closing != Closing::Open {
        return Vec::new();
    }
    state.discovering = true;
    vec![Command::Discover]
}

/// A scan result: the list of connections, and the panels' connections
/// replaced by their new definitions. A panel whose connection is not in the
/// list (an Azure account before the final result) keeps it.
pub(super) fn connections(state: &mut AppState, found: Discovery, done: bool) -> Vec<Command> {
    let list: Vec<Arc<Connection>> = found.connections.into_iter().map(Arc::new).collect();
    for panel in &mut state.panels {
        if let Some(new) = list.iter().find(|conn| conn.id == panel.connection.id) {
            panel.connection = Arc::clone(new);
        }
    }
    state.connections = list;
    state.notes = found.notes;
    state.discovering = !done;
    Vec::new()
}

/// Opens the login dialog for the SSO connection of `side`; not during a job,
/// whose dialogs would replace it.
pub(super) fn log_in(state: &mut AppState, side: Side) -> Vec<Command> {
    let conn = Arc::clone(&state.panels[side.index()].connection);
    if !matches!(conn.login, Some(Login::AwsSso(_)))
        || !state.worker_alive
        || state.closing != Closing::Open
        || state.dialog.is_some()
        || state.job.is_some()
    {
        return Vec::new();
    }
    state.dialog = Some(Dialog::Login {
        conn_id: conn.id.clone(),
        label: crate::gui::display::text(&conn.label),
        code: None,
        error: None,
    });
    vec![Command::Login { conn }]
}

/// The login dialog of `conn_id`, if it is the open one.
fn dialog_of<'a>(state: &'a mut AppState, conn_id: &str) -> Option<&'a mut Dialog> {
    state
        .dialog
        .as_mut()
        .filter(|dialog| matches!(dialog, Dialog::Login { conn_id: id, .. } if id == conn_id))
}

/// The device code to show.
pub(super) fn login_code(
    state: &mut AppState,
    conn_id: &str,
    user_code: String,
    url: String,
) -> Vec<Command> {
    if let Some(Dialog::Login { code, .. }) = dialog_of(state, conn_id) {
        *code = Some((user_code, url));
    }
    Vec::new()
}

/// A finished login: reload the panels on the connection or its login, or show the error.
pub(super) fn login_done(
    state: &mut AppState,
    conn_id: &str,
    result: Result<(), String>,
) -> Vec<Command> {
    match result {
        Ok(()) => {
            if dialog_of(state, conn_id).is_some() {
                state.dialog = None;
            }
            // Profiles that share a login (one sso-session or start URL) share
            // its token, so every panel on that login retries.
            let login = state
                .panels
                .iter()
                .map(|panel| &panel.connection)
                .chain(&state.connections)
                .find(|c| c.id == conn_id)
                .and_then(|c| c.login.clone());
            let mut commands = Vec::new();
            for side in [Side::Left, Side::Right] {
                let connection = &state.panels[side.index()].connection;
                if connection.id == conn_id || (login.is_some() && connection.login == login) {
                    commands.extend(nav::reload(state, side));
                }
            }
            commands
        }
        Err(message) => {
            if let Some(Dialog::Login { error, .. }) = dialog_of(state, conn_id) {
                *error = Some(message);
            }
            Vec::new()
        }
    }
}
