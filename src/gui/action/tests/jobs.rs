//! Job and dialog rules.

use super::{act, dir, loc, obj, page, state};
use crate::gui::action::{Action, Answer, Command, Event, Msg, SelectOp, update};
use crate::gui::model::{
    AppState, Batch, Dialog, Encryption, Entry, Failure, Item, JobId, JobPhase, OnExisting, OpKind,
    Outcome, Plan, Side,
};

/// A state whose left panel lists `entries` with `selected` chosen.
fn with_selection(left: &str, right: &str, entries: Vec<Entry>, selected: &[&str]) -> AppState {
    let mut s = AppState::new(loc(left), loc(right), Encryption::Off).0;
    update(&mut s, page(Side::Left, 1, true, entries, None));
    for name in selected {
        let key = s.panels[0]
            .entries
            .iter()
            .find(|entry| entry.name() == *name)
            .map(Entry::key)
            .expect("selected name is listed");
        act(&mut s, Action::Select(Side::Left, SelectOp::Toggle(key)));
    }
    s
}

fn transfer(src: &str, dst: &str, exists: Option<bool>) -> Item {
    Item::Transfer {
        src: src.to_owned(),
        dst: dst.to_owned(),
        rel: "a".to_owned(),
        exists,
    }
}

fn plan(kind: OpKind, items: Vec<Item>, checked: bool) -> Plan {
    Plan {
        kind,
        items,
        checked,
        failures: Vec::new(),
    }
}

fn planned(s: &mut AppState, job: u64, plan: Plan) -> Vec<Command> {
    update(
        s,
        Msg::Event(Event::Planned {
            job: JobId(job),
            plan,
        }),
    )
}

fn answer(s: &mut AppState, answer: Answer) -> Vec<Command> {
    act(s, Action::DialogAnswer(answer))
}

fn finished(s: &mut AppState, job: u64, outcome: Outcome) -> Vec<Command> {
    update(
        s,
        Msg::Event(Event::Finished {
            job: JobId(job),
            outcome,
        }),
    )
}

/// The default state with object `a` selected on the left.
fn one_selected() -> AppState {
    with_selection("file:///l/", "file:///r/", vec![obj("a")], &["a"])
}

fn copy_plan() -> Plan {
    plan(
        OpKind::Copy,
        vec![transfer("file:///l/a", "file:///r/a", Some(false))],
        true,
    )
}

fn phase(s: &AppState) -> Option<&JobPhase> {
    s.job.as_ref().map(|job| &job.phase)
}

#[test]
fn copy_sends_plan_for_selection() {
    let mut s = with_selection(
        "file:///l/",
        "file:///r/",
        vec![obj("c"), dir("a"), obj("b")],
        &["c", "a"],
    );
    let commands = act(&mut s, Action::Copy);
    assert_eq!(
        commands,
        vec![Command::Plan {
            job: JobId(1),
            batch: Batch {
                kind: OpKind::Copy,
                src_dir: loc("file:///l/"),
                entries: vec![dir("a"), obj("c")],
                dst_dir: loc("file:///r/"),
                rename_to: None,
            },
        }]
    );
    assert_eq!(phase(&s), Some(&JobPhase::Planning));
    assert_eq!(s.next_job, 2);
}

#[test]
fn copy_with_empty_selection_does_nothing() {
    let mut s = state();
    assert_eq!(act(&mut s, Action::Copy), vec![]);
    assert_eq!(act(&mut s, Action::Delete), vec![]);
    assert_eq!(s.job, None);
}

#[test]
fn copy_into_same_folder_is_refused() {
    let mut s = with_selection("file:///x/", "file:///x/", vec![obj("a")], &["a"]);
    assert_eq!(act(&mut s, Action::Copy), vec![]);
    assert!(s.status.contains("same folder"), "{}", s.status);
    assert_eq!(s.job, None);
    assert_eq!(act(&mut s, Action::Move), vec![]);
    assert_eq!(s.job, None);
}

#[test]
fn copy_folder_into_itself_is_refused() {
    let mut s = with_selection("file:///x/", "file:///x/a/b/", vec![dir("a")], &["a"]);
    assert_eq!(act(&mut s, Action::Copy), vec![]);
    assert_eq!(s.status, "Cannot copy a folder into itself");
    assert_eq!(act(&mut s, Action::Move), vec![]);
    assert!(s.status.contains("into itself"), "{}", s.status);
    assert_eq!(s.job, None);
    // A sibling whose name merely starts the same is fine.
    let mut s = with_selection("file:///x/", "file:///x/ab/", vec![dir("a")], &["a"]);
    assert_eq!(act(&mut s, Action::Copy).len(), 1);
}

#[test]
fn operations_ignored_while_job_runs() {
    let mut s = one_selected();
    assert_eq!(act(&mut s, Action::Copy).len(), 1);
    for action in [
        Action::Copy,
        Action::Move,
        Action::StartRename,
        Action::Delete,
    ] {
        assert_eq!(act(&mut s, action), vec![]);
    }
    assert_eq!(s.dialog, None);
    assert_eq!(s.next_job, 2);
}

#[test]
fn operations_ignored_while_dialog_open() {
    let mut s = one_selected();
    act(&mut s, Action::StartRename);
    assert!(matches!(s.dialog, Some(Dialog::Rename { .. })));
    assert_eq!(act(&mut s, Action::Copy), vec![]);
    assert_eq!(act(&mut s, Action::Delete), vec![]);
    assert_eq!(s.job, None);
}

#[test]
fn rename_needs_exactly_one_entry() {
    let mut s = with_selection(
        "file:///l/",
        "file:///r/",
        vec![obj("a"), obj("b")],
        &["a", "b"],
    );
    assert_eq!(act(&mut s, Action::StartRename), vec![]);
    assert_eq!(s.dialog, None);
}

#[test]
fn rename_validation() {
    let mut s = one_selected();
    act(&mut s, Action::StartRename);
    assert_eq!(
        s.dialog,
        Some(Dialog::Rename {
            side: Side::Left,
            folder: false,
            from: "a".to_owned(),
            to: "a".to_owned(),
            error: None,
        })
    );
    for bad in ["", ".", "..", "a/b", "a"] {
        act(&mut s, Action::DialogText(bad.to_owned()));
        assert_eq!(answer(&mut s, Answer::Proceed), vec![], "{bad:?}");
        let Some(Dialog::Rename { error, .. }) = &s.dialog else {
            panic!("rename dialog closed on {bad:?}");
        };
        assert!(error.is_some(), "{bad:?}");
        assert_eq!(s.job, None);
    }
    act(&mut s, Action::DialogText("new".to_owned()));
    let commands = answer(&mut s, Answer::Proceed);
    assert_eq!(
        commands,
        vec![Command::Plan {
            job: JobId(1),
            batch: Batch {
                kind: OpKind::Rename,
                src_dir: loc("file:///l/"),
                entries: vec![obj("a")],
                dst_dir: loc("file:///l/"),
                rename_to: Some("new".to_owned()),
            },
        }]
    );
    assert_eq!(s.dialog, None);
    assert_eq!(phase(&s), Some(&JobPhase::Planning));
}

#[test]
fn rename_cancel_closes_dialog() {
    let mut s = one_selected();
    act(&mut s, Action::StartRename);
    assert_eq!(answer(&mut s, Answer::Cancel), vec![]);
    assert_eq!(s.dialog, None);
    assert_eq!(s.job, None);
}

#[test]
fn rename_onto_existing_asks() {
    let mut s = one_selected();
    act(&mut s, Action::StartRename);
    act(&mut s, Action::DialogText("b".to_owned()));
    answer(&mut s, Answer::Proceed);
    let p = plan(
        OpKind::Rename,
        vec![transfer("file:///l/a", "file:///l/b", Some(true))],
        true,
    );
    assert_eq!(planned(&mut s, 1, p.clone()), vec![]);
    assert_eq!(
        s.dialog,
        Some(Dialog::Overwrite {
            kind: OpKind::Rename,
            existing: 1,
            total: 1,
            dst: "file:///l/".to_owned(),
        })
    );
    assert_eq!(phase(&s), Some(&JobPhase::Confirming(p.clone())));
    assert_eq!(
        answer(&mut s, Answer::Overwrite),
        vec![Command::Execute {
            job: JobId(1),
            plan: p,
            on_existing: OnExisting::Overwrite,
        }]
    );
    assert_eq!(phase(&s), Some(&JobPhase::Running));
    assert_eq!(s.dialog, None);
}

#[test]
fn unchecked_plan_warns() {
    let mut s = one_selected();
    act(&mut s, Action::Copy);
    let p = plan(
        OpKind::Copy,
        vec![transfer("file:///l/a", "file:///r/a", None)],
        false,
    );
    assert_eq!(planned(&mut s, 1, p.clone()), vec![]);
    assert_eq!(
        s.dialog,
        Some(Dialog::Unchecked {
            kind: OpKind::Copy,
            total: 1,
            dst: "file:///r/".to_owned(),
        })
    );
    assert_eq!(
        answer(&mut s, Answer::Proceed),
        vec![Command::Execute {
            job: JobId(1),
            plan: p,
            on_existing: OnExisting::Overwrite,
        }]
    );
}

#[test]
fn skip_answer_executes_skip() {
    let mut s = one_selected();
    act(&mut s, Action::Copy);
    let p = plan(
        OpKind::Copy,
        vec![
            transfer("file:///l/a", "file:///r/a", Some(true)),
            transfer("file:///l/b", "file:///r/b", Some(false)),
        ],
        true,
    );
    planned(&mut s, 1, p.clone());
    assert!(matches!(
        s.dialog,
        Some(Dialog::Overwrite {
            existing: 1,
            total: 2,
            ..
        })
    ));
    assert_eq!(
        answer(&mut s, Answer::Skip),
        vec![Command::Execute {
            job: JobId(1),
            plan: p,
            on_existing: OnExisting::Skip,
        }]
    );
}

#[test]
fn cancel_answer_drops_the_job() {
    let mut s = one_selected();
    act(&mut s, Action::Move);
    planned(&mut s, 1, plan(OpKind::Move, vec![], true));
    assert!(s.dialog.is_some());
    assert_eq!(answer(&mut s, Answer::Cancel), vec![]);
    assert_eq!(s.dialog, None);
    assert_eq!(s.job, None);
    // A new operation may start again.
    assert_eq!(act(&mut s, Action::Copy).len(), 1);
    assert_eq!(s.job.as_ref().map(|job| job.id), Some(JobId(2)));
}

#[test]
fn copy_without_existing_runs_at_once() {
    let mut s = one_selected();
    act(&mut s, Action::Copy);
    assert_eq!(
        planned(&mut s, 1, copy_plan()),
        vec![Command::Execute {
            job: JobId(1),
            plan: copy_plan(),
            on_existing: OnExisting::Overwrite,
        }]
    );
    assert_eq!(s.dialog, None);
    let job = s.job.as_ref().expect("job runs");
    assert_eq!(job.phase, JobPhase::Running);
    assert_eq!(job.total, 1);
}

#[test]
fn stale_events_are_dropped() {
    let mut s = one_selected();
    act(&mut s, Action::Copy);
    assert_eq!(planned(&mut s, 7, copy_plan()), vec![]);
    assert_eq!(phase(&s), Some(&JobPhase::Planning));
    assert_eq!(finished(&mut s, 7, Outcome::default()), vec![]);
    assert!(s.job.is_some());
}

#[test]
fn move_always_confirms() {
    let mut s = one_selected();
    act(&mut s, Action::Move);
    let p = plan(
        OpKind::Move,
        vec![transfer("file:///l/a", "file:///r/a", Some(false))],
        true,
    );
    assert_eq!(planned(&mut s, 1, p.clone()), vec![]);
    assert_eq!(
        s.dialog,
        Some(Dialog::ConfirmMove {
            total: 1,
            dst: "file:///r/".to_owned(),
        })
    );
    assert_eq!(
        answer(&mut s, Answer::Proceed),
        vec![Command::Execute {
            job: JobId(1),
            plan: p,
            on_existing: OnExisting::Overwrite,
        }]
    );
}

fn delete_plan() -> Plan {
    plan(
        OpKind::Delete,
        vec![
            Item::DeletePrefix {
                url: "s3://media/d/".to_owned(),
                rel: "d".to_owned(),
            },
            Item::DeleteObject {
                url: "s3://media/a".to_owned(),
                rel: "a".to_owned(),
            },
        ],
        true,
    )
}

#[test]
fn bucket_root_delete_needs_exact_name() {
    let mut s = with_selection(
        "s3://media/",
        "file:///r/",
        vec![dir("d"), obj("a")],
        &["d", "a"],
    );
    let commands = act(&mut s, Action::Delete);
    let [Command::Plan { batch, .. }] = commands.as_slice() else {
        panic!("expected one plan, got {commands:?}");
    };
    assert_eq!(batch.dst_dir, batch.src_dir);
    planned(&mut s, 1, delete_plan());
    assert_eq!(
        s.dialog,
        Some(Dialog::ConfirmBucketDelete {
            count: 2,
            folders: 1,
            bucket: "media".to_owned(),
            typed: String::new(),
        })
    );
    act(&mut s, Action::DialogText("medi".to_owned()));
    assert_eq!(answer(&mut s, Answer::Proceed), vec![]);
    assert!(s.dialog.is_some());
    act(&mut s, Action::DialogText("media".to_owned()));
    assert_eq!(
        answer(&mut s, Answer::Proceed),
        vec![Command::Execute {
            job: JobId(1),
            plan: delete_plan(),
            on_existing: OnExisting::Overwrite,
        }]
    );
    assert_eq!(s.dialog, None);
}

#[test]
fn delete_below_root_plain_confirm() {
    let mut s = with_selection("s3://media/p/", "file:///r/", vec![obj("a")], &["a"]);
    act(&mut s, Action::Delete);
    planned(&mut s, 1, delete_plan());
    assert_eq!(
        s.dialog,
        Some(Dialog::ConfirmDelete {
            count: 2,
            folders: 1,
        })
    );
    assert_eq!(answer(&mut s, Answer::Proceed).len(), 1);
    assert_eq!(phase(&s), Some(&JobPhase::Running));
}

#[test]
fn plan_with_only_failures_reports_them() {
    let mut s = one_selected();
    act(&mut s, Action::Copy);
    let failure = Failure {
        name: "a".to_owned(),
        error: "denied".to_owned(),
    };
    let mut p = plan(OpKind::Copy, vec![], true);
    p.failures.push(failure.clone());
    assert_eq!(planned(&mut s, 1, p), vec![]);
    assert_eq!(s.job, None);
    assert_eq!(
        s.dialog,
        Some(Dialog::Failures {
            kind: OpKind::Copy,
            outcome: Outcome {
                done: 0,
                cancelled: false,
                failures: vec![failure],
            },
        })
    );
    assert_eq!(answer(&mut s, Answer::Proceed), vec![]);
    assert_eq!(s.dialog, None);
}

#[test]
fn progress_updates_the_job() {
    let mut s = one_selected();
    act(&mut s, Action::Copy);
    planned(&mut s, 1, copy_plan());
    update(
        &mut s,
        Msg::Event(Event::Progress {
            job: JobId(1),
            done: 3,
            total: 9,
            current: "x".to_owned(),
        }),
    );
    let job = s.job.as_ref().expect("job runs");
    assert_eq!((job.done, job.total, job.current.as_str()), (3, 9, "x"));
}

#[test]
fn finished_refreshes_and_reports_failures() {
    let mut s = one_selected();
    act(&mut s, Action::Copy);
    planned(&mut s, 1, copy_plan());
    let outcome = Outcome {
        done: 1,
        cancelled: false,
        failures: vec![Failure {
            name: "a".to_owned(),
            error: "denied".to_owned(),
        }],
    };
    let commands = finished(&mut s, 1, outcome.clone());
    assert_eq!(
        commands,
        vec![
            Command::List {
                side: Side::Left,
                seq: 2,
                url: "file:///l/".to_owned(),
                token: None,
            },
            Command::List {
                side: Side::Right,
                seq: 2,
                url: "file:///r/".to_owned(),
                token: None,
            },
        ]
    );
    assert_eq!(s.job, None);
    assert_eq!(
        s.dialog,
        Some(Dialog::Failures {
            kind: OpKind::Copy,
            outcome,
        })
    );
    assert_eq!(s.status, "1 copied; 1 failed");
}

#[test]
fn finished_status_texts() {
    let cases = [
        (Action::Copy, OpKind::Copy, false, "4 copied"),
        (Action::Move, OpKind::Move, false, "4 moved"),
        (Action::Delete, OpKind::Delete, false, "4 deleted"),
        (Action::Copy, OpKind::Copy, true, "Cancelled after 4 of 9"),
    ];
    for (action, kind, cancelled, want) in cases {
        let mut s = one_selected();
        act(&mut s, action);
        planned(&mut s, 1, plan(kind, vec![], true));
        if s.dialog.is_some() {
            answer(&mut s, Answer::Proceed);
        }
        update(
            &mut s,
            Msg::Event(Event::Progress {
                job: JobId(1),
                done: 4,
                total: 9,
                current: String::new(),
            }),
        );
        let outcome = Outcome {
            done: 4,
            cancelled,
            failures: Vec::new(),
        };
        assert_eq!(finished(&mut s, 1, outcome).len(), 2);
        assert_eq!(s.status, want);
        assert_eq!(s.dialog, None);
    }
}

#[test]
fn cancel_job_sends_abort() {
    let mut s = one_selected();
    assert_eq!(act(&mut s, Action::CancelJob), vec![]);
    act(&mut s, Action::Copy);
    assert_eq!(
        act(&mut s, Action::CancelJob),
        vec![Command::Abort(JobId(1))]
    );
    planned(&mut s, 1, copy_plan());
    assert_eq!(phase(&s), Some(&JobPhase::Running));
    assert_eq!(
        act(&mut s, Action::CancelJob),
        vec![Command::Abort(JobId(1))]
    );
}

#[test]
fn worker_gone_blocks_operations() {
    let mut s = one_selected();
    update(&mut s, Msg::Event(Event::WorkerGone));
    assert!(!s.worker_alive);
    s.status.clear();
    for action in [
        Action::Copy,
        Action::Move,
        Action::StartRename,
        Action::Delete,
    ] {
        assert_eq!(act(&mut s, action.clone()), vec![], "{action:?}");
        assert!(!s.status.is_empty(), "{action:?}");
        s.status.clear();
    }
    assert_eq!(s.job, None);
    assert_eq!(s.dialog, None);
}

#[test]
fn selecting_an_object_leaves_a_same_named_folder_alone() {
    let mut s = state();
    update(
        &mut s,
        page(Side::Left, 1, true, vec![dir("x"), obj("x")], None),
    );
    act(
        &mut s,
        Action::Select(Side::Left, SelectOp::Only(obj("x").key())),
    );
    let commands = act(&mut s, Action::Delete);
    let [Command::Plan { batch, .. }] = commands.as_slice() else {
        panic!("expected one plan: {commands:?}");
    };
    assert_eq!(batch.entries, vec![obj("x")]);
}

#[test]
fn move_onto_existing_names_the_move() {
    let mut s = one_selected();
    act(&mut s, Action::Move);
    let p = plan(
        OpKind::Move,
        vec![transfer("file:///l/a", "file:///r/a", Some(true))],
        true,
    );
    assert_eq!(planned(&mut s, 1, p), vec![]);
    assert_eq!(
        s.dialog,
        Some(Dialog::Overwrite {
            kind: OpKind::Move,
            existing: 1,
            total: 1,
            dst: "file:///r/".to_owned(),
        })
    );
}

#[test]
fn rename_keeps_a_same_named_entry_of_the_other_kind() {
    let mut s = with_selection("file:///l/", "file:///r/", vec![dir("x"), obj("x")], &[]);
    act(
        &mut s,
        Action::Select(Side::Left, SelectOp::Only(obj("x").key())),
    );
    act(&mut s, Action::StartRename);
    act(&mut s, Action::DialogText("y".to_owned()));
    let commands = answer(&mut s, Answer::Proceed);
    let [Command::Plan { batch, .. }] = commands.as_slice() else {
        panic!("expected one plan: {commands:?}");
    };
    assert_eq!(batch.entries, vec![obj("x")]);
}
