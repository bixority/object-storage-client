//! Close and shutdown rules.

use super::{act, loc, obj, page, plan, planned};
use crate::gui::action::close::DRAINING;
use crate::gui::action::{Action, Answer, Command, Event, Msg, SelectOp, update};
use crate::gui::model::{
    AppState, Closing, Dialog, Encryption, Failure, Item, JobId, JobPhase, OpKind, Outcome, Plan,
    Side,
};

/// A state with object `a` selected on the left, `file:///l/` → `file:///r/`.
fn selected() -> AppState {
    let mut s = AppState::at(loc("file:///l/"), loc("file:///r/"), Encryption::Off);
    update(&mut s, page(Side::Left, 1, true, vec![obj("a")], None));
    act(
        &mut s,
        Action::Select(Side::Left, SelectOp::Only(obj("a").key())),
    );
    s
}

fn copy_plan(exists: Option<bool>) -> Plan {
    plan(
        OpKind::Copy,
        vec![Item::Transfer {
            src: "file:///l/a".into(),
            dst: "file:///r/a".into(),
            rel: "a".into(),
            size: 1,
            exists,
        }],
        true,
    )
}

fn planned_copy(s: &mut AppState, exists: Option<bool>) -> Vec<Command> {
    planned(s, 1, copy_plan(exists))
}

/// A copy of `a` that is running (planned with nothing in the way).
fn running() -> AppState {
    let mut s = selected();
    act(&mut s, Action::Copy);
    let commands = planned_copy(&mut s, Some(false));
    assert!(matches!(commands.as_slice(), [Command::Execute { .. }]));
    s
}

#[test]
fn close_without_job_drains() {
    let mut s = selected();
    assert_eq!(act(&mut s, Action::CloseRequested), vec![Command::Shutdown]);
    assert_eq!(s.closing, Closing::Draining);
    assert_eq!(s.status, DRAINING);
    assert_eq!(act(&mut s, Action::CloseRequested), vec![]);
    assert_eq!(update(&mut s, Msg::Event(Event::Interrupted)), vec![]);
}

#[test]
fn close_with_job_asks_first() {
    let mut s = running();
    assert_eq!(act(&mut s, Action::CloseRequested), vec![]);
    assert_eq!(s.dialog, Some(Dialog::Quit));
    assert_eq!(s.closing, Closing::Open);

    assert_eq!(act(&mut s, Action::DialogAnswer(Answer::Cancel)), vec![]);
    assert_eq!(s.dialog, None);
    assert!(matches!(
        s.job.as_ref().map(|j| &j.phase),
        Some(JobPhase::Running)
    ));

    act(&mut s, Action::CloseRequested);
    assert_eq!(
        act(&mut s, Action::DialogAnswer(Answer::Proceed)),
        vec![Command::Shutdown]
    );
    assert_eq!(s.closing, Closing::Draining);
    assert_eq!(s.dialog, None);
}

#[test]
fn close_while_planning_asks_and_keeps_the_question() {
    let mut s = selected();
    act(&mut s, Action::Copy);
    act(&mut s, Action::CloseRequested);
    assert!(matches!(s.dialog, Some(Dialog::Quit)));
    // The plan needs an answer, but the close question stays on top.
    assert_eq!(planned_copy(&mut s, Some(true)), vec![]);
    assert!(matches!(s.dialog, Some(Dialog::Quit)));
    // Keeping the window open brings the plan's own question back.
    act(&mut s, Action::DialogAnswer(Answer::Cancel));
    assert!(matches!(
        s.dialog,
        Some(Dialog::Overwrite { existing: 1, .. })
    ));
}

#[test]
fn plan_without_question_runs_after_quit_is_cancelled() {
    let mut s = selected();
    act(&mut s, Action::Copy);
    act(&mut s, Action::CloseRequested);
    assert_eq!(planned_copy(&mut s, Some(false)), vec![]);
    let commands = act(&mut s, Action::DialogAnswer(Answer::Cancel));
    assert!(matches!(commands.as_slice(), [Command::Execute { .. }]));
}

#[test]
fn close_while_confirming_drops_the_job() {
    let mut s = selected();
    act(&mut s, Action::Copy);
    planned_copy(&mut s, Some(true));
    assert!(matches!(s.dialog, Some(Dialog::Overwrite { .. })));
    assert_eq!(act(&mut s, Action::CloseRequested), vec![Command::Shutdown]);
    assert_eq!(s.job, None);
    assert_eq!(s.dialog, None);
}

#[test]
fn interrupt_skips_the_question() {
    let mut s = running();
    assert_eq!(
        update(&mut s, Msg::Event(Event::Interrupted)),
        vec![Command::Shutdown]
    );
    assert_eq!(s.closing, Closing::Draining);
}

#[test]
fn shutdown_done_closes() {
    let mut s = selected();
    update(&mut s, Msg::Event(Event::ShutdownDone));
    assert_eq!(s.closing, Closing::Open, "not draining: ignored");
    act(&mut s, Action::CloseRequested);
    update(&mut s, Msg::Event(Event::ShutdownDone));
    assert_eq!(s.closing, Closing::Done);
}

#[test]
fn no_listing_while_draining() {
    let mut s = selected();
    act(&mut s, Action::CloseRequested);
    assert_eq!(act(&mut s, Action::Refresh(Side::Left)), vec![]);
    assert_eq!(act(&mut s, Action::Copy), vec![]);
}

#[test]
fn worker_gone_clears_the_job_and_finishes_a_drain() {
    let mut s = running();
    act(&mut s, Action::CloseRequested);
    update(&mut s, Msg::Event(Event::WorkerGone));
    assert_eq!(s.job, None);
    assert_eq!(s.dialog, None);
    assert_eq!(s.closing, Closing::Open);
    // With no worker there is nothing to drain: close at once.
    assert_eq!(act(&mut s, Action::CloseRequested), vec![]);
    assert_eq!(s.closing, Closing::Done);

    let mut s = selected();
    act(&mut s, Action::CloseRequested);
    update(&mut s, Msg::Event(Event::WorkerGone));
    assert_eq!(s.closing, Closing::Done);
}

#[test]
fn failures_during_quit_question_survive_keep_running() {
    let mut s = running();
    act(&mut s, Action::CloseRequested);
    update(
        &mut s,
        Msg::Event(Event::Finished {
            job: JobId(1),
            outcome: Outcome {
                done: 0,
                cancelled: false,
                failures: vec![Failure {
                    name: "a".into(),
                    error: "denied".into(),
                }],
            },
        }),
    );
    assert!(matches!(s.dialog, Some(Dialog::Quit)));
    act(&mut s, Action::DialogAnswer(Answer::Cancel));
    assert!(
        matches!(s.dialog, Some(Dialog::Failures { .. })),
        "{:?}",
        s.dialog
    );
    assert!(s.status.contains("1 failed"), "{}", s.status);
}

#[test]
fn finished_while_draining_keeps_the_drain_status() {
    let mut s = running();
    update(&mut s, Msg::Event(Event::Interrupted));
    assert_eq!(s.closing, Closing::Draining);
    let commands = update(
        &mut s,
        Msg::Event(Event::Finished {
            job: JobId(1),
            outcome: Outcome {
                done: 0,
                cancelled: true,
                failures: vec![Failure {
                    name: "a".into(),
                    error: "aborted".into(),
                }],
            },
        }),
    );
    assert_eq!(commands, vec![]);
    assert_eq!(s.status, DRAINING);
    assert_eq!(s.dialog, None);
    assert_eq!(s.job, None);
}

#[test]
fn all_failure_plan_during_quit_is_deferred() {
    let mut s = selected();
    act(&mut s, Action::Copy);
    act(&mut s, Action::CloseRequested);
    let mut failed = copy_plan(None);
    failed.items.clear();
    failed.failures = vec![Failure {
        name: "a".into(),
        error: "denied".into(),
    }];
    update(
        &mut s,
        Msg::Event(Event::Planned {
            job: JobId(1),
            plan: failed,
        }),
    );
    assert_eq!(s.dialog, Some(Dialog::Quit));
    act(&mut s, Action::DialogAnswer(Answer::Cancel));
    assert!(matches!(s.dialog, Some(Dialog::Failures { .. })));
}

#[test]
fn plan_arriving_while_draining_is_dropped() {
    let mut s = selected();
    act(&mut s, Action::Copy);
    update(&mut s, Msg::Event(Event::Interrupted));
    assert_eq!(s.closing, Closing::Draining);
    assert_eq!(planned_copy(&mut s, Some(true)), vec![]);
    assert_eq!(s.job, None);
    assert_eq!(s.dialog, None);
    assert_eq!(s.status, DRAINING);
}
