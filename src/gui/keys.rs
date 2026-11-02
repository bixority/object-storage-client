//! Keyboard shortcuts.

use super::action::{Action, SelectOp};
use super::model::AppState;
use eframe::egui::{Key, Modifiers};

/// The action for `key` with `mods`, if it is a shortcut.
///
/// Nothing is a shortcut while a text field has focus (`text_focus`) or a
/// dialog is open: dialogs handle Enter and Esc themselves.
pub(super) fn map_key(
    key: Key,
    mods: Modifiers,
    state: &AppState,
    text_focus: bool,
) -> Option<Action> {
    if text_focus || state.dialog.is_some() {
        return None;
    }
    let side = state.active;
    let panel = &state.panels[side.index()];
    let ctrl = (mods.ctrl || mods.command) && !mods.alt && !mods.shift;
    let plain = mods.is_none();
    match key {
        Key::F5 if plain => Some(Action::Copy),
        Key::F6 if plain => Some(Action::Move),
        Key::F2 if plain => Some(Action::StartRename),
        Key::Delete if plain => Some(Action::Delete),
        Key::Backspace if plain => Some(Action::Up(side)),
        Key::Tab if plain => Some(Action::SwitchPanel),
        Key::Escape if plain => Some(Action::Select(side, SelectOp::Clear)),
        Key::R if ctrl => Some(Action::Refresh(side)),
        Key::L if ctrl => Some(Action::FocusAddress(side)),
        // The rows of a panel still loading its first page are inert.
        Key::A if ctrl && !panel.stale() => Some(Action::Select(side, SelectOp::All)),
        // Enter opens the selected folder (the anchor of the selection).
        Key::Enter if plain && !panel.stale() => {
            let anchor = panel.anchor.as_ref()?;
            let (folder, name) = anchor;
            let listed = panel.entries.iter().any(|entry| entry.is(anchor));
            (*folder && listed && panel.selection.contains(anchor))
                .then(|| Action::Open(side, name.clone()))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gui::action::{Event, Msg, update};
    use crate::gui::location::Location;
    use crate::gui::model::{Dialog, Encryption, Entry, Page, Side};

    fn state() -> AppState {
        let loc = |url: &str| Location::parse(url).expect("test url parses");
        AppState::new(loc("file:///l/"), loc("file:///r/"), Encryption::Off).0
    }

    fn loaded(entries: Vec<Entry>) -> AppState {
        let mut s = state();
        update(
            &mut s,
            Msg::Event(Event::Page {
                side: Side::Left,
                seq: 1,
                first: true,
                result: Ok(Page {
                    entries,
                    next: None,
                }),
            }),
        );
        s
    }

    fn key(state: &AppState, key: Key, mods: Modifiers) -> Option<Action> {
        map_key(key, mods, state, false)
    }

    #[test]
    fn keys_map_shortcuts() {
        let mut s = state();
        assert_eq!(key(&s, Key::F5, Modifiers::NONE), Some(Action::Copy));
        assert_eq!(key(&s, Key::F6, Modifiers::NONE), Some(Action::Move));
        assert_eq!(key(&s, Key::F2, Modifiers::NONE), Some(Action::StartRename));
        assert_eq!(key(&s, Key::Delete, Modifiers::NONE), Some(Action::Delete));
        assert_eq!(
            key(&s, Key::Tab, Modifiers::NONE),
            Some(Action::SwitchPanel)
        );
        s.active = Side::Right;
        assert_eq!(
            key(&s, Key::R, Modifiers::CTRL),
            Some(Action::Refresh(Side::Right))
        );
        assert_eq!(
            key(&s, Key::R, Modifiers::COMMAND),
            Some(Action::Refresh(Side::Right))
        );
        assert_eq!(
            key(&s, Key::Backspace, Modifiers::NONE),
            Some(Action::Up(Side::Right))
        );
        assert_eq!(
            key(&s, Key::L, Modifiers::CTRL),
            Some(Action::FocusAddress(Side::Right))
        );
        assert_eq!(key(&s, Key::R, Modifiers::NONE), None);
        assert_eq!(key(&s, Key::F5, Modifiers::SHIFT), None);
    }

    #[test]
    fn keys_suppressed_in_text_and_dialogs() {
        let mut s = state();
        assert_eq!(map_key(Key::F5, Modifiers::NONE, &s, true), None);
        assert_eq!(map_key(Key::Tab, Modifiers::NONE, &s, true), None);
        s.dialog = Some(Dialog::ConfirmDelete {
            count: 1,
            folders: 0,
        });
        assert_eq!(map_key(Key::F5, Modifiers::NONE, &s, false), None);
        assert_eq!(map_key(Key::Enter, Modifiers::NONE, &s, false), None);
        assert_eq!(map_key(Key::Escape, Modifiers::NONE, &s, false), None);
    }

    #[test]
    fn select_all_and_enter_need_current_rows() {
        let folder = Entry::Folder {
            name: "d".to_owned(),
        };
        let mut s = loaded(vec![folder]);
        assert_eq!(
            key(&s, Key::A, Modifiers::CTRL),
            Some(Action::Select(Side::Left, SelectOp::All))
        );
        assert_eq!(key(&s, Key::Enter, Modifiers::NONE), None, "no selection");
        update(
            &mut s,
            Msg::Action(Action::Select(
                Side::Left,
                SelectOp::Only((true, "d".to_owned())),
            )),
        );
        assert_eq!(
            key(&s, Key::Enter, Modifiers::NONE),
            Some(Action::Open(Side::Left, "d".to_owned()))
        );
        // Navigating away leaves the old rows on screen, inert.
        update(&mut s, Msg::Action(Action::Refresh(Side::Left)));
        assert!(s.panels[0].stale());
        assert_eq!(key(&s, Key::A, Modifiers::CTRL), None);
        assert_eq!(key(&s, Key::Enter, Modifiers::NONE), None);
    }

    #[test]
    fn enter_ignores_an_object_named_like_a_folder() {
        let object = Entry::Object {
            name: "x".to_owned(),
            size: 1,
            modified: chrono::DateTime::UNIX_EPOCH,
        };
        let folder = Entry::Folder {
            name: "x".to_owned(),
        };
        let mut s = loaded(vec![folder.clone(), object.clone()]);
        update(
            &mut s,
            Msg::Action(Action::Select(Side::Left, SelectOp::Only(object.key()))),
        );
        assert_eq!(key(&s, Key::Enter, Modifiers::NONE), None);
        update(
            &mut s,
            Msg::Action(Action::Select(Side::Left, SelectOp::Only(folder.key()))),
        );
        assert_eq!(
            key(&s, Key::Enter, Modifiers::NONE),
            Some(Action::Open(Side::Left, "x".to_owned()))
        );
    }
}
