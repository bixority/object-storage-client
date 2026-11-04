//! Keyboard shortcuts.

use super::action::{Action, Motion, SelectOp};
use super::model::AppState;
use eframe::egui::{Key, Modifiers};

/// The action for `key` with `mods`, if it is a shortcut.
///
/// Nothing is a shortcut while a text field has focus or a popup is open
/// (`input_busy`) or a dialog is open: dialogs handle Enter and Esc themselves. `page_rows` is
/// how many rows Page Up and Page Down move.
pub(super) fn map_key(
    key: Key,
    mods: Modifiers,
    state: &AppState,
    input_busy: bool,
    page_rows: usize,
) -> Option<Action> {
    if input_busy || state.dialog.is_some() {
        return None;
    }
    let side = state.active;
    let panel = &state.panels[side.index()];
    let ctrl = (mods.ctrl || mods.command) && !mods.alt && !mods.shift;
    let plain = mods.is_none();
    // Motion keys: Shift extends; any other modifier is not ours.
    let motion_mods = !mods.ctrl && !mods.alt && !mods.command;
    let page = page_rows.max(1);
    let motion = match key {
        Key::ArrowUp => Some(Motion::Up),
        Key::ArrowDown => Some(Motion::Down),
        Key::PageUp => Some(Motion::PageUp(page)),
        Key::PageDown => Some(Motion::PageDown(page)),
        Key::Home => Some(Motion::Home),
        Key::End => Some(Motion::End),
        _ => None,
    };
    if let Some(motion) = motion {
        // The rows of a panel still loading its first page are inert.
        return (motion_mods && !panel.stale()).then_some(Action::Select(
            side,
            SelectOp::Move {
                motion,
                extend: mods.shift,
            },
        ));
    }
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
        // Enter opens the folder under the cursor; a click moves the cursor.
        Key::Enter if plain && !panel.stale() => {
            let cursor = panel.cursor.as_ref()?;
            let (folder, name) = cursor;
            let listed = panel.entries.iter().any(|entry| entry.is(cursor));
            (*folder && listed).then(|| Action::Open(side, name.clone()))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gui::action::{Event, Motion, Msg, update};
    use crate::gui::location::Location;
    use crate::gui::model::{Dialog, Encryption, Entry, Page, Side};

    fn state() -> AppState {
        let loc = |url: &str| Location::parse(url).expect("test url parses");
        AppState::at(loc("file:///l/"), loc("file:///r/"), Encryption::Off)
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

    const PAGE: usize = 7;

    fn key(state: &AppState, key: Key, mods: Modifiers) -> Option<Action> {
        map_key(key, mods, state, false, PAGE)
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
        assert_eq!(map_key(Key::F5, Modifiers::NONE, &s, true, PAGE), None);
        assert_eq!(map_key(Key::Tab, Modifiers::NONE, &s, true, PAGE), None);
        s.dialog = Some(Dialog::ConfirmDelete {
            count: 1,
            folders: 0,
        });
        assert_eq!(map_key(Key::F5, Modifiers::NONE, &s, false, PAGE), None);
        assert_eq!(map_key(Key::Enter, Modifiers::NONE, &s, false, PAGE), None);
        assert_eq!(map_key(Key::Escape, Modifiers::NONE, &s, false, PAGE), None);
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

    fn motion(motion: Motion, extend: bool) -> Action {
        Action::Select(Side::Left, SelectOp::Move { motion, extend })
    }

    #[test]
    fn arrows_and_paging_move_the_cursor() {
        let s = loaded(vec![Entry::Folder {
            name: "d".to_owned(),
        }]);
        let shift = Modifiers::SHIFT;
        assert_eq!(
            key(&s, Key::ArrowDown, Modifiers::NONE),
            Some(motion(Motion::Down, false))
        );
        assert_eq!(
            key(&s, Key::ArrowDown, shift),
            Some(motion(Motion::Down, true))
        );
        assert_eq!(
            key(&s, Key::ArrowUp, Modifiers::NONE),
            Some(motion(Motion::Up, false))
        );
        assert_eq!(key(&s, Key::ArrowUp, shift), Some(motion(Motion::Up, true)));
        assert_eq!(
            key(&s, Key::PageDown, Modifiers::NONE),
            Some(motion(Motion::PageDown(PAGE), false))
        );
        assert_eq!(
            key(&s, Key::PageUp, shift),
            Some(motion(Motion::PageUp(PAGE), true))
        );
        assert_eq!(
            key(&s, Key::Home, Modifiers::NONE),
            Some(motion(Motion::Home, false))
        );
        assert_eq!(key(&s, Key::End, shift), Some(motion(Motion::End, true)));
        // A page is at least one row.
        assert_eq!(
            map_key(Key::PageDown, Modifiers::NONE, &s, false, 0),
            Some(motion(Motion::PageDown(1), false))
        );
        for mods in [
            Modifiers::CTRL,
            Modifiers::ALT,
            Modifiers::COMMAND,
            Modifiers::CTRL | Modifiers::SHIFT,
        ] {
            assert_eq!(key(&s, Key::ArrowDown, mods), None, "{mods:?}");
            assert_eq!(key(&s, Key::End, mods), None, "{mods:?}");
        }
    }

    #[test]
    fn motion_suppressed_in_text_dialogs_and_stale_panels() {
        let mut s = loaded(vec![Entry::Folder {
            name: "d".to_owned(),
        }]);
        assert_eq!(
            map_key(Key::ArrowDown, Modifiers::NONE, &s, true, PAGE),
            None
        );
        assert_eq!(map_key(Key::Home, Modifiers::NONE, &s, true, PAGE), None);
        s.dialog = Some(Dialog::ConfirmDelete {
            count: 1,
            folders: 0,
        });
        assert_eq!(key(&s, Key::ArrowDown, Modifiers::NONE), None);
        assert_eq!(key(&s, Key::PageDown, Modifiers::NONE), None);
        s.dialog = None;
        update(&mut s, Msg::Action(Action::Refresh(Side::Left)));
        assert!(s.panels[0].stale());
        assert_eq!(key(&s, Key::ArrowDown, Modifiers::NONE), None);
        assert_eq!(key(&s, Key::End, Modifiers::SHIFT), None);
    }

    #[test]
    fn enter_opens_the_cursor_folder() {
        let folder = Entry::Folder {
            name: "d".to_owned(),
        };
        let object = Entry::Object {
            name: "o".to_owned(),
            size: 1,
            modified: chrono::DateTime::UNIX_EPOCH,
        };
        let mut s = loaded(vec![folder, object]);
        let down = Action::Select(
            Side::Left,
            SelectOp::Move {
                motion: Motion::Down,
                extend: false,
            },
        );
        update(&mut s, Msg::Action(down.clone()));
        assert_eq!(
            key(&s, Key::Enter, Modifiers::NONE),
            Some(Action::Open(Side::Left, "d".to_owned()))
        );
        // The selection does not matter: only the cursor.
        update(
            &mut s,
            Msg::Action(Action::Select(Side::Left, SelectOp::Clear)),
        );
        assert_eq!(
            key(&s, Key::Enter, Modifiers::NONE),
            Some(Action::Open(Side::Left, "d".to_owned()))
        );
        update(&mut s, Msg::Action(down));
        assert_eq!(key(&s, Key::Enter, Modifiers::NONE), None, "object cursor");
    }
}
