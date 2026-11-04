//! Connection rules: choosing, scanning, scheme checks, roots, SSO login.

use super::{act, dir, loc, obj, page, plan, planned, s3, s3_state, state};
use crate::connection::sso::SsoLogin;
use crate::connection::{Connection, Discovery, Login, Provider, Source};
use crate::gui::action::{Action, Answer, Command, Event, Msg, SelectOp, update};
use crate::gui::model::{AppState, Dialog, Entry, Item, OpKind, Outcome, Side};
use std::sync::Arc;

fn provider(id: &str, provider: Provider) -> Arc<Connection> {
    Arc::new(Connection {
        provider,
        ..(*s3(id)).clone()
    })
}

fn sso(id: &str) -> Arc<Connection> {
    Arc::new(Connection {
        login: Some(Login::AwsSso(SsoLogin {
            identifier: "s".into(),
            start_url: "https://x.awsapps.com/start".into(),
            region: "eu-west-1".into(),
            scopes: Vec::new(),
        })),
        ..(*s3(id)).clone()
    })
}

fn event(s: &mut AppState, event: Event) -> Vec<Command> {
    update(s, Msg::Event(event))
}

/// `state()` that knows the connections `extra`.
fn knowing(extra: Vec<Arc<Connection>>) -> AppState {
    let mut s = state();
    s.connections.extend(extra);
    s
}

/// Left on `conn` at `url` with the listed `entries` all selected, right on Local.
fn selected_on(conn: Arc<Connection>, url: &str, entries: Vec<Entry>) -> AppState {
    let mut s = state();
    s.panels[0].connection = conn;
    s.panels[0].location = loc(url);
    update(&mut s, page(Side::Left, 1, true, entries, None));
    act(&mut s, Action::Select(Side::Left, SelectOp::All));
    s
}

#[test]
fn selecting_a_connection_shows_its_root() {
    let mut s = knowing(vec![
        s3("aws"),
        provider("gcp", Provider::Gcs),
        provider("az", Provider::Azure),
    ]);
    for (id, url) in [("aws", "s3:///"), ("gcp", "gs:///"), ("az", "az:///")] {
        let commands = act(
            &mut s,
            Action::SelectConnection {
                side: Side::Left,
                id: id.into(),
            },
        );
        let panel = &s.panels[0];
        assert_eq!(panel.connection.id, id);
        assert_eq!(panel.location.as_str(), url);
        assert_eq!(
            commands,
            vec![Command::List {
                side: Side::Left,
                seq: panel.seq,
                conn: Arc::clone(&panel.connection),
                url: url.to_owned(),
                token: None,
            }]
        );
        assert!(panel.loading);
    }
    assert_eq!(s.panels[0].seq, 4);
    let commands = act(
        &mut s,
        Action::SelectConnection {
            side: Side::Left,
            id: "local".into(),
        },
    );
    assert_eq!(s.panels[0].location, loc("."));
    assert_eq!(commands.len(), 1);
    assert_eq!(s.panels[1].seq, 1, "the other panel is untouched");
}

#[test]
fn selecting_an_unknown_connection_does_nothing() {
    let mut s = state();
    let action = Action::SelectConnection {
        side: Side::Left,
        id: "nope".into(),
    };
    assert_eq!(act(&mut s, action), vec![]);
    assert_eq!(s.panels[0].seq, 1);
}

#[test]
fn an_address_must_match_the_panels_provider() {
    let mut s = state();
    let commands = act(&mut s, Action::Navigate(Side::Left, "s3://b/".into()));
    assert_eq!(commands, vec![]);
    let error = s.panels[0].address_error.clone().expect("error");
    assert!(error.contains("choose a matching connection"), "{error}");
    assert_eq!(s.panels[0].location, loc("file:///l/"));
    // Local serves file and http(s).
    assert_eq!(
        act(&mut s, Action::Navigate(Side::Left, "https://h/x".into())).len(),
        1
    );
    for (conn, ok, bad) in [
        (s3("aws"), "s3://b/", "gs://b/"),
        (provider("g", Provider::Gcs), "gcs://b/", "s3://b/"),
        (provider("a", Provider::Azure), "abfss://c/", "file:///x/"),
    ] {
        let mut s = state();
        s.panels[0].connection = conn;
        assert_eq!(
            act(&mut s, Action::Navigate(Side::Left, ok.into())).len(),
            1
        );
        assert_eq!(
            act(&mut s, Action::Navigate(Side::Left, bad.into())),
            vec![]
        );
        assert!(s.panels[0].address_error.is_some(), "{bad}");
    }
}

#[test]
fn opening_a_bucket_from_the_root_and_going_up() {
    let mut s = s3_state();
    act(&mut s, Action::Navigate(Side::Left, "s3://".into()));
    update(&mut s, page(Side::Left, 2, true, vec![dir("media")], None));
    act(&mut s, Action::Open(Side::Left, "media".into()));
    assert_eq!(s.panels[0].location.as_str(), "s3://media/");
    let commands = act(&mut s, Action::Up(Side::Left));
    assert_eq!(commands.len(), 1);
    assert!(s.panels[0].location.is_provider_root());
}

#[test]
fn nothing_changes_at_a_provider_root() {
    let buckets = vec![dir("one"), dir("two")];
    let mut s = selected_on(s3("aws"), "s3://", buckets);
    for action in [
        Action::Copy,
        Action::Move,
        Action::Delete,
        Action::StartRename,
    ] {
        assert_eq!(act(&mut s, action), vec![]);
        assert_eq!(s.status, "Open a bucket or container first");
        assert_eq!((&s.job, &s.dialog), (&None, &None));
        s.status.clear();
    }
    // The destination panel at a root refuses a copy too.
    let mut s = state();
    update(&mut s, page(Side::Left, 1, true, vec![obj("a")], None));
    act(&mut s, Action::Select(Side::Left, SelectOp::All));
    s.panels[1].connection = s3("aws");
    s.panels[1].location = loc("s3://");
    assert_eq!(act(&mut s, Action::Copy), vec![]);
    assert_eq!(s.status, "Open a bucket or container first");
}

#[test]
fn a_bucket_root_can_still_be_deleted_by_typing_its_name() {
    let mut s = selected_on(s3("aws"), "s3://media/", vec![obj("a")]);
    act(&mut s, Action::Delete);
    planned(
        &mut s,
        1,
        plan(
            OpKind::Delete,
            vec![Item::DeleteObject {
                url: "s3://media/a".into(),
                rel: "a".into(),
            }],
            true,
        ),
    );
    assert!(matches!(
        s.dialog,
        Some(Dialog::ConfirmBucketDelete { ref bucket, .. }) if bucket == "media"
    ));
}

#[test]
fn a_cross_connection_batch_carries_both_connections() {
    let mut s = selected_on(s3("aws"), "s3://b/", vec![obj("a")]);
    let commands = act(&mut s, Action::Copy);
    let [
        Command::Plan {
            src_conn, dst_conn, ..
        },
    ] = commands.as_slice()
    else {
        panic!("{commands:?}");
    };
    assert_eq!(
        (src_conn.id.as_str(), dst_conn.id.as_str()),
        ("aws", "local")
    );
    let job = s.job.as_ref().expect("job");
    assert_eq!(
        (job.src_conn.id.as_str(), job.dst_conn.id.as_str()),
        ("aws", "local")
    );
    // The worker gets both again when the plan is executed.
    let commands = planned(&mut s, 1, plan(OpKind::Copy, vec![], true));
    let [
        Command::Execute {
            src_conn, dst_conn, ..
        },
    ] = commands.as_slice()
    else {
        panic!("{commands:?}");
    };
    assert_eq!(
        (src_conn.id.as_str(), dst_conn.id.as_str()),
        ("aws", "local")
    );
}

#[test]
fn the_same_url_on_two_connections_is_not_the_same_folder() {
    let mut s = selected_on(s3("one"), "s3://b/", vec![obj("a")]);
    s.panels[1].connection = s3("two");
    s.panels[1].location = loc("s3://b/");
    let commands = act(&mut s, Action::Copy);
    assert_eq!(commands.len(), 1, "{}", s.status);
    // The same connection on the same URL is refused.
    let mut s = selected_on(s3("one"), "s3://b/", vec![obj("a")]);
    s.panels[1].connection = s3("one");
    s.panels[1].location = loc("s3://b/");
    assert_eq!(act(&mut s, Action::Copy), vec![]);
    assert!(s.status.contains("same folder"));
}

#[test]
fn a_folder_may_be_copied_into_the_same_path_of_another_connection() {
    let mut s = selected_on(s3("one"), "s3://b/", vec![dir("f")]);
    s.panels[1].connection = s3("two");
    s.panels[1].location = loc("s3://b/f/");
    assert_eq!(act(&mut s, Action::Copy).len(), 1, "{}", s.status);
    let mut s = selected_on(s3("one"), "s3://b/", vec![dir("f")]);
    s.panels[1].connection = s3("one");
    s.panels[1].location = loc("s3://b/f/");
    assert_eq!(act(&mut s, Action::Copy), vec![]);
    assert!(s.status.contains("into itself"));
}

#[test]
fn a_finished_job_reloads_only_the_panels_on_its_connections() {
    // Same URL on two connections: only the destination's panel reloads.
    let mut s = selected_on(s3("one"), "s3://b/", vec![obj("a")]);
    s.panels[1].connection = s3("two");
    s.panels[1].location = loc("s3://b/");
    act(&mut s, Action::Copy);
    planned(&mut s, 1, plan(OpKind::Copy, vec![], true));
    let done = Outcome {
        done: 1,
        ..Outcome::default()
    };
    let commands = event(
        &mut s,
        Event::Finished {
            job: crate::gui::model::JobId(1),
            outcome: done,
        },
    );
    assert!(matches!(
        commands.as_slice(),
        [Command::List {
            side: Side::Right,
            ..
        }]
    ));
}

#[test]
fn a_scan_replaces_panel_connections_and_keeps_local_first() {
    let mut s = state();
    s.panels[0].connection = s3("aws");
    s.panels[1].connection = provider("az", Provider::Azure);
    let renamed = Arc::new(Connection {
        label: "renamed".into(),
        ..(*s3("aws")).clone()
    });
    let first = Discovery {
        connections: vec![Connection::local(), (*renamed).clone()],
        notes: vec!["note".into()],
    };
    assert_eq!(
        event(
            &mut s,
            Event::Connections {
                discovery: first,
                done: false
            }
        ),
        vec![]
    );
    assert_eq!(s.panels[0].connection.label, "renamed");
    // The Azure account is not found yet: the panel keeps it.
    assert_eq!(s.panels[1].connection.id, "az");
    assert!(s.discovering);
    assert_eq!(s.notes, ["note"]);
    let last = Discovery {
        connections: vec![Connection::local(), (*s3("other")).clone()],
        notes: Vec::new(),
    };
    event(
        &mut s,
        Event::Connections {
            discovery: last,
            done: true,
        },
    );
    let ids: Vec<_> = s.connections.iter().map(|c| c.id.as_str()).collect();
    assert_eq!(ids, ["local", "other"]);
    assert!(!s.discovering && s.notes.is_empty());
    assert_eq!(
        s.panels[0].connection.label, "renamed",
        "not in the list: kept"
    );
}

#[test]
fn rescan_asks_the_worker_to_discover() {
    let mut s = state();
    s.discovering = false;
    assert_eq!(act(&mut s, Action::Rescan), vec![Command::Discover]);
    assert!(s.discovering);
    s.worker_alive = false;
    assert_eq!(act(&mut s, Action::Rescan), vec![]);
}

fn login_dialog(s: &AppState) -> (&Option<(String, String)>, &Option<String>) {
    let Some(Dialog::Login { code, error, .. }) = &s.dialog else {
        panic!("no login dialog: {:?}", s.dialog);
    };
    (code, error)
}

#[test]
fn login_runs_through_code_and_success() {
    let mut s = state();
    s.panels[0].connection = sso("aws-profile:dev");
    s.panels[1].connection = sso("aws-profile:dev");
    let commands = act(&mut s, Action::LogIn { side: Side::Left });
    assert_eq!(
        commands,
        vec![Command::Login {
            conn: sso("aws-profile:dev")
        }]
    );
    assert_eq!(login_dialog(&s), (&None, &None));
    let code = Event::LoginCode {
        conn_id: "aws-profile:dev".into(),
        user_code: "ABCD".into(),
        url: "https://x/".into(),
    };
    event(&mut s, code);
    assert_eq!(
        login_dialog(&s).0,
        &Some(("ABCD".to_owned(), "https://x/".to_owned()))
    );
    let done = Event::LoginDone {
        conn_id: "aws-profile:dev".into(),
        result: Ok(()),
    };
    let commands = event(&mut s, done);
    assert_eq!(s.dialog, None);
    let sides: Vec<_> = commands
        .iter()
        .map(|c| match c {
            Command::List { side, .. } => *side,
            other => panic!("{other:?}"),
        })
        .collect();
    assert_eq!(sides, [Side::Left, Side::Right]);
}

#[test]
fn login_reloads_only_the_panels_on_that_connection() {
    let mut s = state();
    s.panels[0].connection = sso("aws-profile:dev");
    let done = Event::LoginDone {
        conn_id: "aws-profile:dev".into(),
        result: Ok(()),
    };
    let commands = event(&mut s, done);
    assert!(matches!(
        commands.as_slice(),
        [Command::List {
            side: Side::Left,
            ..
        }]
    ));
}

#[test]
fn login_error_stays_in_the_dialog_and_cancel_stops_it() {
    let mut s = state();
    s.panels[0].connection = sso("aws-profile:dev");
    act(&mut s, Action::LogIn { side: Side::Left });
    let done = Event::LoginDone {
        conn_id: "aws-profile:dev".into(),
        result: Err("denied".into()),
    };
    assert_eq!(event(&mut s, done), vec![]);
    assert_eq!(login_dialog(&s).1.as_deref(), Some("denied"));
    assert_eq!(
        act(&mut s, Action::DialogAnswer(Answer::Cancel)),
        vec![Command::CancelLogin]
    );
    assert_eq!(s.dialog, None);
    // Nothing to cancel any more.
    assert_eq!(act(&mut s, Action::DialogAnswer(Answer::Cancel)), vec![]);
}

#[test]
fn no_login_while_a_job_runs() {
    let mut s = state();
    update(&mut s, page(Side::Left, 1, true, vec![obj("a")], None));
    act(&mut s, Action::Select(Side::Left, SelectOp::All));
    assert_eq!(act(&mut s, Action::Copy).len(), 1);
    s.panels[1].connection = sso("aws-profile:dev");
    assert_eq!(act(&mut s, Action::LogIn { side: Side::Right }), vec![]);
    assert_eq!(s.dialog, None);
}

#[test]
fn the_login_dialog_closes_when_the_worker_dies() {
    let mut s = state();
    s.panels[0].connection = sso("aws-profile:dev");
    act(&mut s, Action::LogIn { side: Side::Left });
    event(&mut s, Event::WorkerGone);
    assert_eq!(s.dialog, None);
}

#[test]
fn delete_and_rename_ignore_the_other_panel_at_a_root() {
    let mut s = state();
    update(&mut s, page(Side::Left, 1, true, vec![obj("a")], None));
    act(&mut s, Action::Select(Side::Left, SelectOp::All));
    s.panels[1].connection = s3("aws");
    s.panels[1].location = loc("s3://");
    assert!(matches!(act(&mut s, Action::StartRename).as_slice(), []));
    assert!(matches!(s.dialog, Some(Dialog::Rename { .. })));
    s.dialog = None;
    assert!(matches!(
        act(&mut s, Action::Delete).as_slice(),
        [Command::Plan { .. }]
    ));
}

#[test]
fn a_late_login_event_for_another_connection_is_ignored() {
    let mut s = state();
    s.panels[0].connection = sso("aws-profile:dev");
    act(&mut s, Action::LogIn { side: Side::Left });
    let err = Event::LoginDone {
        conn_id: "aws-profile:other".into(),
        result: Err("x".into()),
    };
    event(&mut s, err);
    event(
        &mut s,
        Event::LoginCode {
            conn_id: "aws-profile:other".into(),
            user_code: "Z".into(),
            url: "u".into(),
        },
    );
    assert_eq!(login_dialog(&s), (&None, &None));
}

#[test]
fn login_needs_an_sso_connection_and_no_other_dialog() {
    let mut s = state();
    assert_eq!(act(&mut s, Action::LogIn { side: Side::Left }), vec![]);
    s.panels[0].connection = Arc::new(Connection {
        login: Some(Login::Command("az login")),
        source: Source::Env,
        ..(*s3("az")).clone()
    });
    assert_eq!(act(&mut s, Action::LogIn { side: Side::Left }), vec![]);
    assert_eq!(s.dialog, None);
    s.panels[0].connection = sso("aws-profile:dev");
    s.dialog = Some(Dialog::Quit);
    assert_eq!(act(&mut s, Action::LogIn { side: Side::Left }), vec![]);
    s.dialog = None;
    s.worker_alive = false;
    assert_eq!(act(&mut s, Action::LogIn { side: Side::Left }), vec![]);
}
