//! Keyboard motion and the cursor.

use super::{act, dir, obj, page, state};
use crate::gui::action::{Action, Motion, SelectOp};
use crate::gui::model::{AppState, Entry, EntryKey, Side};
use std::collections::BTreeSet;

/// Rows `r0` … `r{n-1}`, already in display order.
fn rows(n: usize) -> Vec<Entry> {
    (0..n).map(|i| obj(&format!("r{i}"))).collect()
}

fn row(i: usize) -> EntryKey {
    obj(&format!("r{i}")).key()
}

/// The left panel lists `n` rows.
fn listed(n: usize) -> AppState {
    let mut s = state();
    crate::gui::action::update(&mut s, page(Side::Left, 1, true, rows(n), None));
    s
}

fn go(s: &mut AppState, motion: Motion) {
    act(
        s,
        Action::Select(
            Side::Left,
            SelectOp::Move {
                motion,
                extend: false,
            },
        ),
    );
}

fn extend(s: &mut AppState, motion: Motion) {
    act(
        s,
        Action::Select(
            Side::Left,
            SelectOp::Move {
                motion,
                extend: true,
            },
        ),
    );
}

/// Selection, anchor and cursor of the left panel are all row `i`.
fn assert_at(s: &AppState, i: usize) {
    let panel = &s.panels[0];
    assert_eq!(panel.selection, BTreeSet::from([row(i)]));
    assert_eq!(panel.anchor, Some(row(i)));
    assert_eq!(panel.cursor, Some(row(i)));
}

#[test]
fn down_without_cursor_starts_at_the_first_row() {
    let mut s = listed(5);
    go(&mut s, Motion::Down);
    assert_at(&s, 0);
}

#[test]
fn up_without_cursor_starts_at_the_last_row() {
    let mut s = listed(5);
    go(&mut s, Motion::Up);
    assert_at(&s, 4);
    let mut s = listed(5);
    go(&mut s, Motion::PageUp(3));
    assert_at(&s, 4);
    let mut s = listed(5);
    go(&mut s, Motion::PageDown(3));
    assert_at(&s, 0);
}

#[test]
fn down_down_up_lands_on_the_second_row() {
    let mut s = listed(5);
    // The cursor starts on row 0 (a click there).
    act(&mut s, Action::Select(Side::Left, SelectOp::Only(row(0))));
    go(&mut s, Motion::Down);
    go(&mut s, Motion::Down);
    go(&mut s, Motion::Up);
    assert_at(&s, 1);
}

#[test]
fn motion_clamps_at_both_ends() {
    let mut s = listed(3);
    go(&mut s, Motion::Down);
    go(&mut s, Motion::Up);
    assert_at(&s, 0);
    go(&mut s, Motion::End);
    go(&mut s, Motion::Down);
    assert_at(&s, 2);
}

#[test]
fn page_motion_moves_by_the_page_and_clamps() {
    let mut s = listed(5);
    go(&mut s, Motion::Down);
    go(&mut s, Motion::PageDown(3));
    assert_at(&s, 3);
    go(&mut s, Motion::PageDown(3));
    assert_at(&s, 4);
    go(&mut s, Motion::PageUp(3));
    assert_at(&s, 1);
    go(&mut s, Motion::PageUp(3));
    assert_at(&s, 0);
}

#[test]
fn home_and_end() {
    let mut s = listed(5);
    go(&mut s, Motion::End);
    assert_at(&s, 4);
    go(&mut s, Motion::Home);
    assert_at(&s, 0);
}

#[test]
fn shift_motion_extends_from_the_anchor() {
    let mut s = listed(5);
    go(&mut s, Motion::Down);
    go(&mut s, Motion::Down);
    extend(&mut s, Motion::Down);
    extend(&mut s, Motion::Down);
    let panel = &s.panels[0];
    assert_eq!(panel.selection, BTreeSet::from([row(1), row(2), row(3)]));
    assert_eq!(panel.anchor, Some(row(1)));
    assert_eq!(panel.cursor, Some(row(3)));
    extend(&mut s, Motion::Up);
    let panel = &s.panels[0];
    assert_eq!(panel.selection, BTreeSet::from([row(1), row(2)]));
    assert_eq!(panel.anchor, Some(row(1)));
    assert_eq!(panel.cursor, Some(row(2)));
}

#[test]
fn shift_motion_without_anchor_starts_at_the_cursor() {
    let mut s = listed(5);
    extend(&mut s, Motion::Down);
    let panel = &s.panels[0];
    assert_eq!(panel.selection, BTreeSet::from([row(0)]));
    assert_eq!(panel.anchor, Some(row(0)));
    assert_eq!(panel.cursor, Some(row(0)));
}

#[test]
fn motion_ignores_empty_and_stale_panels() {
    let mut s = listed(0);
    go(&mut s, Motion::Down);
    extend(&mut s, Motion::End);
    let panel = &s.panels[0];
    assert_eq!(panel.selection, BTreeSet::new());
    assert_eq!(panel.cursor, None);
    assert_eq!(panel.cursor_moves, 0);

    let mut s = listed(3);
    act(&mut s, Action::Select(Side::Left, SelectOp::Only(row(2))));
    act(&mut s, Action::Refresh(Side::Left));
    assert!(s.panels[0].stale());
    let moves = s.panels[0].cursor_moves;
    let selection = s.panels[0].selection.clone();
    let cursor = s.panels[0].cursor.clone();
    go(&mut s, Motion::Down);
    extend(&mut s, Motion::Home);
    let panel = &s.panels[0];
    assert_eq!(panel.selection, selection);
    assert_eq!(panel.cursor, cursor);
    assert_eq!(panel.cursor_moves, moves);
}

#[test]
fn stale_rows_cannot_be_selected() {
    let mut s = listed(3);
    act(&mut s, Action::Up(Side::Left));
    assert!(s.panels[0].stale());
    act(&mut s, Action::Select(Side::Left, SelectOp::All));
    assert_eq!(s.panels[0].selection, BTreeSet::new());
    act(&mut s, Action::Select(Side::Left, SelectOp::Toggle(row(1))));
    act(&mut s, Action::Select(Side::Left, SelectOp::Range(row(1))));
    assert_eq!(s.panels[0].selection, BTreeSet::new());
    assert_eq!(s.panels[0].cursor, None);
}

#[test]
fn stale_rows_cannot_be_opened() {
    let mut s = state();
    crate::gui::action::update(&mut s, page(Side::Left, 1, true, vec![dir("d")], None));
    act(&mut s, Action::Up(Side::Left));
    let parent = s.panels[0].location.clone();
    assert_eq!(
        act(&mut s, Action::Open(Side::Left, "d".to_owned())),
        vec![]
    );
    assert_eq!(s.panels[0].location, parent);
}

#[test]
fn opening_twice_in_one_frame_stays_in_the_first_folder() {
    let mut s = state();
    crate::gui::action::update(&mut s, page(Side::Left, 1, true, vec![dir("d")], None));
    assert_eq!(
        act(&mut s, Action::Open(Side::Left, "d".to_owned())).len(),
        1
    );
    assert_eq!(
        act(&mut s, Action::Open(Side::Left, "d".to_owned())),
        vec![]
    );
    assert_eq!(s.panels[0].location.as_str(), "file:///l/d/");
}

#[test]
fn navigation_clears_the_cursor() {
    let mut s = listed(3);
    go(&mut s, Motion::Down);
    assert!(s.panels[0].cursor.is_some());
    act(&mut s, Action::Refresh(Side::Left));
    assert_eq!(s.panels[0].cursor, None);
    assert_eq!(s.panels[0].anchor, None);
}

#[test]
fn clicks_set_the_cursor() {
    let mut s = state();
    let entries = vec![dir("d"), obj("a"), obj("b"), obj("c")];
    crate::gui::action::update(&mut s, page(Side::Left, 1, true, entries, None));
    act(
        &mut s,
        Action::Select(Side::Left, SelectOp::Only(obj("a").key())),
    );
    assert_eq!(s.panels[0].cursor, Some(obj("a").key()));
    assert_eq!(s.panels[0].anchor, Some(obj("a").key()));

    act(
        &mut s,
        Action::Select(Side::Left, SelectOp::Range(obj("c").key())),
    );
    assert_eq!(s.panels[0].cursor, Some(obj("c").key()));
    assert_eq!(s.panels[0].anchor, Some(obj("a").key()));

    act(
        &mut s,
        Action::Select(Side::Left, SelectOp::Toggle(obj("b").key())),
    );
    assert_eq!(s.panels[0].cursor, Some(obj("b").key()));
    assert_eq!(s.panels[0].anchor, Some(obj("b").key()));
    assert!(!s.panels[0].selection.contains(&obj("b").key()));

    act(&mut s, Action::Select(Side::Left, SelectOp::All));
    assert_eq!(s.panels[0].cursor, Some(obj("b").key()));
    act(&mut s, Action::Select(Side::Left, SelectOp::Clear));
    assert_eq!(s.panels[0].selection, BTreeSet::new());
    assert_eq!(s.panels[0].cursor, Some(obj("b").key()));
    assert_eq!(s.panels[0].anchor, Some(obj("b").key()));

    // Motion continues from the clicked row; clicks never count as moves.
    assert_eq!(s.panels[0].cursor_moves, 0);
    go(&mut s, Motion::Down);
    assert_eq!(s.panels[0].cursor, Some(obj("c").key()));
}

#[test]
fn every_move_counts_even_in_place() {
    let mut s = listed(2);
    go(&mut s, Motion::Down);
    assert_eq!(s.panels[0].cursor_moves, 1);
    go(&mut s, Motion::Up);
    assert_eq!(s.panels[0].cursor_moves, 2);
    extend(&mut s, Motion::Home);
    assert_eq!(s.panels[0].cursor_moves, 3);
}

#[test]
fn motion_falls_back_to_the_anchor() {
    let mut s = listed(5);
    act(&mut s, Action::Select(Side::Left, SelectOp::Only(row(2))));
    s.panels[0].cursor = Some(obj("gone").key());
    go(&mut s, Motion::Down);
    assert_at(&s, 3);
}
