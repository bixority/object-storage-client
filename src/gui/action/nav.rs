//! Navigation, paging and selection rules.

use super::{Command, Motion, SelectOp};
use crate::gui::display;
use crate::gui::location::Location;
use crate::gui::model::{AppState, Entry, EntryKey, MAX_ENTRIES, Page, PanelState, Side};
use std::sync::Arc;

/// The listing command for `panel` at its current `seq`.
pub(super) fn list(side: Side, panel: &PanelState, token: Option<String>) -> Command {
    Command::List {
        side,
        seq: panel.seq,
        conn: Arc::clone(&panel.connection),
        url: panel.location.as_str().to_owned(),
        token,
    }
}

/// Starts a fresh listing of `location`.
pub(super) fn go(state: &mut AppState, side: Side, location: Location) -> Vec<Command> {
    let panel = &mut state.panels[side.index()];
    panel.seq += 1;
    panel.loading = true;
    panel.error = None;
    panel.address_error = None;
    panel.next = None;
    panel.truncated = false;
    panel.selection.clear();
    panel.anchor = None;
    panel.cursor = None;
    // The address bar shows where the panel is, even if the listing fails.
    panel.address_text = location.display();
    panel.location = location;
    vec![list(side, panel, None)]
}

pub(super) fn navigate(state: &mut AppState, side: Side, text: &str) -> Vec<Command> {
    let panel = &mut state.panels[side.index()];
    let parsed = Location::parse(text).and_then(|location| {
        if panel.connection.provider.serves(location.scheme()) {
            Ok(location)
        } else {
            Err(format!(
                "{} cannot open `{}://` addresses: choose a matching connection in the list",
                display::text(&panel.connection.label),
                display::text(location.scheme())
            ))
        }
    });
    match parsed {
        Ok(location) => go(state, side, location),
        Err(err) => {
            panel.address_error = Some(err);
            Vec::new()
        }
    }
}

pub(super) fn address_edited(state: &mut AppState, side: Side, text: String) -> Vec<Command> {
    let panel = &mut state.panels[side.index()];
    panel.address_text = text;
    panel.address_error = None;
    Vec::new()
}

pub(super) fn up(state: &mut AppState, side: Side) -> Vec<Command> {
    let parent = state.panels[side.index()].location.parent();
    parent.map_or_else(Vec::new, |parent| go(state, side, parent))
}

pub(super) fn open(state: &mut AppState, side: Side, name: &str) -> Vec<Command> {
    let panel = &state.panels[side.index()];
    // The rows are the previous folder's: a name would resolve against the new one.
    if panel.stale() {
        return Vec::new();
    }
    let is_folder = panel
        .entries
        .iter()
        .any(|entry| entry.is_folder() && entry.name() == name);
    // A name like `..` has no child folder of its own: ignored.
    match panel.location.child_dir(name) {
        Some(child) if is_folder => go(state, side, child),
        _ => Vec::new(),
    }
}

pub(super) fn refresh(state: &mut AppState, side: Side) -> Vec<Command> {
    let location = state.panels[side.index()].location.clone();
    go(state, side, location)
}

/// Lists the panel's location again and keeps the text being typed in the
/// address bar.
pub(super) fn reload(state: &mut AppState, side: Side) -> Vec<Command> {
    let panel = &state.panels[side.index()];
    let (text, error) = (panel.address_text.clone(), panel.address_error.clone());
    let commands = refresh(state, side);
    let panel = &mut state.panels[side.index()];
    panel.address_text = text;
    panel.address_error = error;
    commands
}

pub(super) fn load_more(state: &mut AppState, side: Side) -> Vec<Command> {
    let panel = &mut state.panels[side.index()];
    if panel.loading || panel.truncated {
        return Vec::new();
    }
    let Some(token) = panel.next.clone() else {
        return Vec::new();
    };
    panel.loading = true;
    panel.error = None;
    vec![list(side, panel, Some(token))]
}

pub(super) fn retry(state: &mut AppState, side: Side) -> Vec<Command> {
    if state.panels[side.index()].entries.is_empty() {
        refresh(state, side)
    } else {
        load_more(state, side)
    }
}

pub(super) fn page(
    state: &mut AppState,
    side: Side,
    seq: u64,
    first: bool,
    result: Result<Page, String>,
) -> Vec<Command> {
    let panel = &mut state.panels[side.index()];
    if seq != panel.seq {
        return Vec::new();
    }
    panel.loading = false;
    match result {
        Ok(page) => {
            panel.error = None;
            if first {
                panel.entries = page.entries;
            } else {
                panel.entries.extend(page.entries);
            }
            panel.entries.sort_by(Entry::listing_order);
            panel.next = page.next;
            let len = panel.entries.len();
            if len > MAX_ENTRIES || (len == MAX_ENTRIES && panel.next.is_some()) {
                panel.entries.truncate(MAX_ENTRIES);
                panel.truncated = true;
                panel.next = None;
            }
        }
        Err(message) => {
            panel.error = Some(message);
            if first {
                panel.entries.clear();
                panel.next = None;
            }
        }
    }
    Vec::new()
}

pub(super) fn select(state: &mut AppState, side: Side, op: SelectOp) -> Vec<Command> {
    let panel = &mut state.panels[side.index()];
    // Rows of a stale panel are inert; only clearing is safe.
    if panel.stale() && !matches!(op, SelectOp::Clear) {
        return Vec::new();
    }
    match op {
        SelectOp::Only(key) => {
            panel.selection.clear();
            panel.selection.insert(key.clone());
            panel.anchor = Some(key.clone());
            panel.cursor = Some(key);
        }
        SelectOp::Toggle(key) => {
            if !panel.selection.remove(&key) {
                panel.selection.insert(key.clone());
            }
            panel.anchor = Some(key.clone());
            panel.cursor = Some(key);
        }
        SelectOp::Range(key) => {
            range(panel, key.clone());
            panel.cursor = Some(key);
        }
        SelectOp::All => {
            panel.selection = panel.entries.iter().map(Entry::key).collect();
        }
        SelectOp::Clear => panel.selection.clear(),
        SelectOp::Move { motion, extend } => move_cursor(panel, motion, extend),
    }
    Vec::new()
}

/// Moves the cursor by `motion`; selects the new row alone, or with `extend`
/// the rows from the anchor to it. Rows of an empty or stale panel are inert.
fn move_cursor(panel: &mut PanelState, motion: Motion, extend: bool) {
    if panel.entries.is_empty() || panel.stale() {
        return;
    }
    let last = panel.entries.len() - 1;
    let position = |wanted: &EntryKey| panel.entries.iter().position(|e| e.is(wanted));
    let current = panel
        .cursor
        .as_ref()
        .and_then(position)
        .or_else(|| panel.anchor.as_ref().and_then(position));
    let new = match (current, motion) {
        (_, Motion::Home) | (None, Motion::Down | Motion::PageDown(_)) => 0,
        (_, Motion::End) | (None, Motion::Up | Motion::PageUp(_)) => last,
        (Some(at), Motion::Up) => at.saturating_sub(1),
        (Some(at), Motion::Down) => at.saturating_add(1).min(last),
        (Some(at), Motion::PageUp(rows)) => at.saturating_sub(rows.max(1)),
        (Some(at), Motion::PageDown(rows)) => at.saturating_add(rows.max(1)).min(last),
    };
    let key = panel.entries[new].key();
    if extend {
        let anchor = panel.anchor.as_ref().and_then(position);
        let from = anchor.or(current).unwrap_or(new);
        if anchor.is_none() {
            panel.anchor = Some(panel.entries[from].key());
        }
        let (low, high) = (from.min(new), from.max(new));
        panel.selection = panel.entries[low..=high].iter().map(Entry::key).collect();
    } else {
        panel.selection.clear();
        panel.selection.insert(key.clone());
        panel.anchor = Some(key.clone());
    }
    panel.cursor = Some(key);
    panel.cursor_moves += 1;
}

/// Selects the rows between the anchor and `key`, in display order.
fn range(panel: &mut PanelState, key: EntryKey) {
    let position = |wanted: &EntryKey| panel.entries.iter().position(|e| e.is(wanted));
    let bounds = panel.anchor.as_ref().and_then(position).zip(position(&key));
    let Some((from, to)) = bounds else {
        panel.selection.clear();
        panel.selection.insert(key.clone());
        panel.anchor = Some(key);
        return;
    };
    let (low, high) = (from.min(to), from.max(to));
    panel.selection = panel.entries[low..=high].iter().map(Entry::key).collect();
}
