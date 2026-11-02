//! Navigation, paging and selection rules.

use super::{Command, SelectOp};
use crate::gui::location::Location;
use crate::gui::model::{AppState, Entry, EntryKey, MAX_ENTRIES, Page, PanelState, Side};

/// The listing command for `panel` at its current `seq`.
pub(super) fn list(side: Side, panel: &PanelState, token: Option<String>) -> Command {
    Command::List {
        side,
        seq: panel.seq,
        url: panel.location.as_str().to_owned(),
        token,
    }
}

/// Starts a fresh listing of `location`.
fn go(state: &mut AppState, side: Side, location: Location) -> Vec<Command> {
    let panel = &mut state.panels[side.index()];
    panel.seq += 1;
    panel.loading = true;
    panel.error = None;
    panel.address_error = None;
    panel.next = None;
    panel.truncated = false;
    panel.selection.clear();
    panel.anchor = None;
    // The address bar shows where the panel is, even if the listing fails.
    panel.address_text = location.display();
    panel.location = location;
    vec![list(side, panel, None)]
}

pub(super) fn navigate(state: &mut AppState, side: Side, text: &str) -> Vec<Command> {
    match Location::parse(text) {
        Ok(location) => go(state, side, location),
        Err(err) => {
            state.panels[side.index()].address_error = Some(err);
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
                panel.address_text = panel.location.display();
            } else {
                panel.entries.extend(page.entries);
            }
            panel
                .entries
                .sort_by(|a, b| (!a.is_folder(), a.name()).cmp(&(!b.is_folder(), b.name())));
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
    match op {
        SelectOp::Only(key) => {
            panel.selection.clear();
            panel.selection.insert(key.clone());
            panel.anchor = Some(key);
        }
        SelectOp::Toggle(key) => {
            if !panel.selection.remove(&key) {
                panel.selection.insert(key.clone());
            }
            panel.anchor = Some(key);
        }
        SelectOp::Range(key) => range(panel, key),
        SelectOp::All => {
            panel.selection = panel.entries.iter().map(Entry::key).collect();
        }
        SelectOp::Clear => panel.selection.clear(),
    }
    Vec::new()
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
