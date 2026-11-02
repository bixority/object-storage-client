use super::*;

mod close;
mod jobs;
use crate::gui::model::{Entry, MAX_ENTRIES};
use chrono::DateTime;

fn loc(url: &str) -> Location {
    Location::parse(url).expect("test url parses")
}

fn state() -> AppState {
    AppState::new(loc("file:///l/"), loc("file:///r/"), Encryption::Off).0
}

fn obj(name: &str) -> Entry {
    Entry::Object {
        name: name.to_owned(),
        size: 1,
        modified: DateTime::UNIX_EPOCH,
    }
}

fn dir(name: &str) -> Entry {
    Entry::Folder {
        name: name.to_owned(),
    }
}

fn page(side: Side, seq: u64, first: bool, entries: Vec<Entry>, next: Option<&str>) -> Msg {
    Msg::Event(Event::Page {
        side,
        seq,
        first,
        result: Ok(Page {
            entries,
            next: next.map(str::to_owned),
        }),
    })
}

fn act(state: &mut AppState, action: Action) -> Vec<Command> {
    update(state, Msg::Action(action))
}

fn names(state: &AppState, side: Side) -> Vec<String> {
    state.panels[side.index()]
        .entries
        .iter()
        .map(|e| e.name().to_owned())
        .collect()
}

#[test]
fn new_state_lists_both_panels() {
    let (_, commands) = AppState::new(loc("file:///l/"), loc("file:///r/"), Encryption::Off);
    assert_eq!(
        commands,
        vec![
            Command::List {
                side: Side::Left,
                seq: 1,
                url: "file:///l/".to_owned(),
                token: None
            },
            Command::List {
                side: Side::Right,
                seq: 1,
                url: "file:///r/".to_owned(),
                token: None
            },
        ]
    );
}

#[test]
fn navigate_bumps_seq_and_lists() {
    let mut s = state();
    act(
        &mut s,
        Action::Select(Side::Left, SelectOp::Only(obj("x").key())),
    );
    let commands = act(&mut s, Action::Navigate(Side::Left, "s3://b/p".into()));
    assert_eq!(
        commands,
        vec![Command::List {
            side: Side::Left,
            seq: 2,
            url: "s3://b/p/".to_owned(),
            token: None
        }]
    );
    assert!(s.panels[0].loading);
    assert_eq!(s.panels[0].selection, BTreeSet::new());
}

#[test]
fn navigate_bad_address_keeps_listing() {
    let mut s = state();
    let commands = act(&mut s, Action::Navigate(Side::Left, "nope://".into()));
    assert_eq!(commands, vec![]);
    assert!(s.panels[0].address_error.is_some());
    assert_eq!(s.panels[0].location, loc("file:///l/"));
}

#[test]
fn stale_page_is_dropped() {
    let mut s = state();
    act(&mut s, Action::Navigate(Side::Left, "file:///a/".into()));
    act(&mut s, Action::Navigate(Side::Left, "file:///b/".into()));
    update(&mut s, page(Side::Left, 2, true, vec![obj("old")], None));
    assert_eq!(s.panels[0].entries, vec![]);
    update(&mut s, page(Side::Left, 3, true, vec![obj("new")], None));
    assert_eq!(names(&s, Side::Left), ["new"]);
    assert_eq!(s.panels[0].address_text, "file:///b/");
}

#[test]
fn pages_sort_folders_first() {
    let mut s = state();
    let entries = vec![obj("b"), dir("z"), obj("a"), dir("c")];
    update(&mut s, page(Side::Left, 1, true, entries, None));
    assert_eq!(names(&s, Side::Left), ["c", "z", "a", "b"]);
}

#[test]
fn load_more_appends_and_keeps_selection() {
    let mut s = state();
    update(&mut s, page(Side::Left, 1, true, vec![obj("a")], Some("t")));
    act(
        &mut s,
        Action::Select(Side::Left, SelectOp::Only(obj("a").key())),
    );
    let commands = act(&mut s, Action::LoadMore(Side::Left));
    assert_eq!(
        commands,
        vec![Command::List {
            side: Side::Left,
            seq: 1,
            url: "file:///l/".to_owned(),
            token: Some("t".to_owned())
        }]
    );
    update(&mut s, page(Side::Left, 1, false, vec![obj("b")], None));
    assert_eq!(names(&s, Side::Left), ["a", "b"]);
    assert_eq!(s.panels[0].selection, BTreeSet::from([obj("a").key()]));
}

use std::collections::BTreeSet;

#[test]
fn rows_are_stale_only_until_the_first_page() {
    let mut s = state();
    assert!(s.panels[0].stale(), "initial listing");
    update(&mut s, page(Side::Left, 1, true, vec![dir("d")], Some("t")));
    assert!(!s.panels[0].stale());
    act(&mut s, Action::LoadMore(Side::Left));
    assert!(s.panels[0].loading);
    assert!(!s.panels[0].stale(), "Load more keeps the rows current");
    update(&mut s, page(Side::Left, 1, false, vec![obj("b")], None));
    act(&mut s, Action::Open(Side::Left, "d".to_owned()));
    assert!(s.panels[0].stale(), "the rows are the parent's");
    update(&mut s, page(Side::Left, 2, true, vec![], None));
    assert!(!s.panels[0].stale());
}

#[test]
fn cap_truncates() {
    let mut s = state();
    let first: Vec<Entry> = (0..MAX_ENTRIES).map(|i| obj(&format!("{i:06}"))).collect();
    update(&mut s, page(Side::Left, 1, true, first, Some("t")));
    assert!(s.panels[0].truncated);
    assert_eq!(s.panels[0].next, None);
    // One more than the cap, arriving as a later page.
    let mut s = state();
    let first: Vec<Entry> = (0..MAX_ENTRIES - 1)
        .map(|i| obj(&format!("{i:06}")))
        .collect();
    update(&mut s, page(Side::Left, 1, true, first, Some("t")));
    assert!(!s.panels[0].truncated);
    let more = vec![obj("zz1"), obj("zz2")];
    update(&mut s, page(Side::Left, 1, false, more, Some("u")));
    let p = &s.panels[0];
    assert_eq!(p.entries.len(), MAX_ENTRIES);
    assert!(p.truncated);
    assert_eq!(p.next, None);
    assert_eq!(act(&mut s, Action::LoadMore(Side::Left)), vec![]);
}

#[test]
fn load_more_error_keeps_rows() {
    let mut s = state();
    update(&mut s, page(Side::Left, 1, true, vec![obj("a")], Some("t")));
    act(&mut s, Action::LoadMore(Side::Left));
    update(
        &mut s,
        Msg::Event(Event::Page {
            side: Side::Left,
            seq: 1,
            first: false,
            result: Err("boom".into()),
        }),
    );
    let p = &s.panels[0];
    assert_eq!(p.entries.len(), 1);
    assert_eq!(p.next.as_deref(), Some("t"));
    assert_eq!(p.error.as_deref(), Some("boom"));
    assert!(!p.loading);
    // Retry resumes the paging.
    assert_eq!(act(&mut s, Action::Retry(Side::Left)).len(), 1);
}

#[test]
fn first_page_error_clears() {
    let mut s = state();
    update(&mut s, page(Side::Left, 1, true, vec![obj("a")], Some("t")));
    act(&mut s, Action::Refresh(Side::Left));
    update(
        &mut s,
        Msg::Event(Event::Page {
            side: Side::Left,
            seq: 2,
            first: true,
            result: Err("boom".into()),
        }),
    );
    assert_eq!(s.panels[0].entries, vec![]);
    assert_eq!(s.panels[0].next, None);
    assert_eq!(s.panels[0].error.as_deref(), Some("boom"));
}

#[test]
fn up_stops_at_root() {
    let mut s = state();
    act(&mut s, Action::Navigate(Side::Left, "s3://b/".into()));
    assert_eq!(act(&mut s, Action::Up(Side::Left)), vec![]);
    act(&mut s, Action::Navigate(Side::Left, "s3://b/p/q/".into()));
    let commands = act(&mut s, Action::Up(Side::Left));
    assert_eq!(commands.len(), 1);
    assert_eq!(s.panels[0].location, loc("s3://b/p/"));
}

#[test]
fn open_object_does_nothing() {
    let mut s = state();
    update(&mut s, page(Side::Left, 1, true, vec![obj("a")], None));
    assert_eq!(act(&mut s, Action::Open(Side::Left, "a".into())), vec![]);
    assert_eq!(s.panels[0].location, loc("file:///l/"));
}

#[test]
fn open_folder_navigates() {
    let mut s = state();
    update(&mut s, page(Side::Left, 1, true, vec![dir("d")], None));
    let commands = act(&mut s, Action::Open(Side::Left, "d".into()));
    assert_eq!(commands.len(), 1);
    assert_eq!(
        s.panels[0].location,
        loc("file:///l/").child_dir("d").expect("valid name")
    );
}

#[test]
fn range_select_uses_display_order() {
    let mut s = state();
    let entries = vec![obj("b"), dir("z"), obj("a"), dir("c"), obj("d")];
    update(&mut s, page(Side::Left, 1, true, entries, None));
    // Display order: c, z, a, b, d.
    act(
        &mut s,
        Action::Select(Side::Left, SelectOp::Only(dir("z").key())),
    );
    act(
        &mut s,
        Action::Select(Side::Left, SelectOp::Range(obj("b").key())),
    );
    let want = BTreeSet::from([dir("z").key(), obj("a").key(), obj("b").key()]);
    assert_eq!(s.panels[0].selection, want);
}

#[test]
fn range_without_anchor_acts_like_only() {
    let mut s = state();
    update(
        &mut s,
        page(Side::Left, 1, true, vec![obj("a"), obj("b")], None),
    );
    act(
        &mut s,
        Action::Select(Side::Left, SelectOp::Range(obj("b").key())),
    );
    assert_eq!(s.panels[0].selection, BTreeSet::from([obj("b").key()]));
}

#[test]
fn focus_and_selection_ops() {
    let mut s = state();
    act(&mut s, Action::SwitchPanel);
    assert_eq!(s.active, Side::Right);
    act(&mut s, Action::Activate(Side::Left));
    assert_eq!(s.active, Side::Left);
    update(
        &mut s,
        page(Side::Left, 1, true, vec![obj("a"), obj("b")], None),
    );
    act(&mut s, Action::Select(Side::Left, SelectOp::All));
    assert_eq!(s.panels[0].selection.len(), 2);
    act(
        &mut s,
        Action::Select(Side::Left, SelectOp::Toggle(obj("a").key())),
    );
    assert_eq!(s.panels[0].selection, BTreeSet::from([obj("b").key()]));
    act(&mut s, Action::Select(Side::Left, SelectOp::Clear));
    assert_eq!(s.panels[0].selection, BTreeSet::new());
}

#[test]
fn no_listing_while_closing() {
    let mut s = state();
    s.closing = Closing::Draining;
    assert_eq!(act(&mut s, Action::Refresh(Side::Left)), vec![]);
}

#[test]
fn exactly_max_entries_without_next_is_complete() {
    let mut s = state();
    let all: Vec<Entry> = (0..MAX_ENTRIES).map(|i| obj(&format!("{i:06}"))).collect();
    update(&mut s, page(Side::Left, 1, true, all, None));
    assert!(!s.panels[0].truncated);
    assert_eq!(s.panels[0].entries.len(), MAX_ENTRIES);
}

/// A first-page error after `go` leaves the address bar showing `location`.
fn assert_address_matches_after_error(s: &mut AppState, want: &Location) {
    let seq = s.panels[0].seq;
    update(
        s,
        Msg::Event(Event::Page {
            side: Side::Left,
            seq,
            first: true,
            result: Err("boom".into()),
        }),
    );
    assert_eq!(&s.panels[0].location, want);
    assert_eq!(s.panels[0].address_text, want.display());
}

#[test]
fn address_text_follows_up_open_and_refresh() {
    let mut s = state();
    act(&mut s, Action::Navigate(Side::Left, "s3://b/p/q".into()));
    update(&mut s, page(Side::Left, 2, true, vec![dir("d")], None));
    act(&mut s, Action::Up(Side::Left));
    assert_address_matches_after_error(&mut s, &loc("s3://b/p/"));

    let mut s = state();
    act(&mut s, Action::Navigate(Side::Left, "s3://b/p/".into()));
    update(&mut s, page(Side::Left, 2, true, vec![dir("d")], None));
    act(&mut s, Action::Open(Side::Left, "d".into()));
    assert_address_matches_after_error(&mut s, &loc("s3://b/p/d/"));

    let mut s = state();
    act(&mut s, Action::AddressEdited(Side::Left, "typed".into()));
    act(&mut s, Action::Refresh(Side::Left));
    assert_address_matches_after_error(&mut s, &loc("file:///l/"));
}
