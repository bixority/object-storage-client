use super::dialogs::{default_answer, overwrite_text};
use super::panel::{human_size, thousands};
use super::*;
use crate::gui::action::{Answer, Event, Msg, SelectOp, update};
use crate::gui::location::Location;
use crate::gui::model::{
    Closing, Dialog, Encryption, Entry, Failure, Job, JobId, JobPhase, Outcome, Page, Plan,
};
use chrono::DateTime;

fn loc(url: &str) -> Location {
    Location::parse(url).expect("test url parses")
}

fn state() -> AppState {
    AppState::new(loc("s3://bucket/"), loc("file:///r/"), Encryption::File).0
}

fn entries() -> Vec<Entry> {
    vec![
        Entry::Folder {
            name: "photos".to_owned(),
        },
        Entry::Object {
            // A control character: drawn through `display::text`.
            name: "a\u{1b}[31mb.txt".to_owned(),
            size: 1536,
            modified: DateTime::UNIX_EPOCH,
        },
    ]
}

/// Both panels loaded; the left one has a selection.
fn main_state() -> AppState {
    let mut s = state();
    for side in [Side::Left, Side::Right] {
        update(
            &mut s,
            Msg::Event(Event::Page {
                side,
                seq: 1,
                first: true,
                result: Ok(Page {
                    entries: entries(),
                    next: Some("token".to_owned()),
                }),
            }),
        );
    }
    update(
        &mut s,
        Msg::Action(Action::Select(
            Side::Left,
            SelectOp::Only((true, "photos".to_owned())),
        )),
    );
    s
}

fn render(state: &AppState) -> Vec<Action> {
    let mut actions = Vec::new();
    let ctx = egui::Context::default();
    // Two passes: some widgets settle their layout on the second.
    for _ in 0..2 {
        ctx.run_ui(egui::RawInput::default(), |ui| {
            actions = draw(ui, state);
        })
        .drop_without_applying_deltas();
    }
    actions
}

fn job(phase: JobPhase) -> Job {
    Job {
        id: JobId(1),
        kind: OpKind::Copy,
        phase,
        done: 3,
        total: 48,
        current: "photos/x.jpg".to_owned(),
        dst_dir: loc("file:///r/"),
    }
}

fn dialogs() -> Vec<Dialog> {
    let failures = (0..500)
        .map(|i| Failure {
            name: format!("f{i}"),
            error: "access denied".to_owned(),
        })
        .collect();
    vec![
        Dialog::Rename {
            side: Side::Left,
            folder: true,
            from: "photos".to_owned(),
            to: "pictures".to_owned(),
            error: Some("Enter a different name".to_owned()),
        },
        Dialog::Overwrite {
            kind: OpKind::Copy,
            existing: 2,
            total: 5,
            dst: "file:///r/".to_owned(),
        },
        Dialog::Overwrite {
            kind: OpKind::Move,
            existing: 1,
            total: 5,
            dst: "file:///r/".to_owned(),
        },
        Dialog::Unchecked {
            kind: OpKind::Move,
            total: 2_000,
            dst: "file:///r/".to_owned(),
        },
        Dialog::ConfirmMove {
            total: 1,
            dst: "file:///r/".to_owned(),
        },
        Dialog::ConfirmDelete {
            count: 3,
            folders: 1,
        },
        Dialog::ConfirmBucketDelete {
            count: 3,
            folders: 2,
            bucket: "bucket".to_owned(),
            typed: "buck".to_owned(),
        },
        Dialog::Failures {
            kind: OpKind::Delete,
            outcome: Outcome {
                done: 7,
                cancelled: true,
                failures,
            },
        },
        Dialog::Quit { done: 3, total: 48 },
    ]
}

#[test]
fn render_smoke_every_state() {
    let mut fixtures = vec![main_state()];

    let mut loading = main_state();
    update(&mut loading, Msg::Action(Action::Refresh(Side::Left)));
    assert!(loading.panels[0].stale());
    fixtures.push(loading);

    let mut error = main_state();
    error.panels[1].error = Some("access denied".to_owned());
    error.panels[1].entries.clear();
    error.panels[0].address_error = Some("unsupported URL scheme `nope`".to_owned());
    fixtures.push(error);

    let mut truncated = main_state();
    truncated.panels[0].truncated = true;
    truncated.panels[0].next = None;
    fixtures.push(truncated);

    for dialog in dialogs() {
        let mut s = main_state();
        if matches!(dialog, Dialog::Quit { .. }) {
            s.job = Some(job(JobPhase::Running));
        }
        s.dialog = Some(dialog);
        fixtures.push(s);
    }

    for phase in [
        JobPhase::Planning,
        JobPhase::Running,
        JobPhase::Confirming(Plan {
            kind: OpKind::Copy,
            items: Vec::new(),
            checked: true,
            failures: Vec::new(),
        }),
    ] {
        let mut s = main_state();
        s.job = Some(job(phase));
        fixtures.push(s);
    }

    let mut draining = main_state();
    update(&mut draining, Msg::Action(Action::CloseRequested));
    assert_eq!(draining.closing, Closing::Draining);
    fixtures.push(draining);

    let mut plain = main_state();
    plain.encryption = Encryption::Off;
    plain.worker_alive = false;
    fixtures.push(plain);

    for fixture in &fixtures {
        // Nothing was clicked or typed: no actions.
        assert_eq!(render(fixture), Vec::new());
    }
}

#[test]
fn render_startup_error() {
    let ctx = egui::Context::default();
    let mut quit = true;
    ctx.run_ui(egui::RawInput::default(), |ui| {
        quit = draw_startup_error(ui, "failed to read keyring");
    })
    .drop_without_applying_deltas();
    assert!(!quit);
}

fn key_input(key: egui::Key) -> egui::RawInput {
    egui::RawInput {
        events: vec![egui::Event::Key {
            key,
            physical_key: None,
            pressed: true,
            repeat: false,
            modifiers: egui::Modifiers::NONE,
        }],
        ..Default::default()
    }
}

/// The answer the open dialog gives to `key`.
fn dialog_key(dialog: Dialog, key: egui::Key) -> Vec<Action> {
    let mut s = main_state();
    s.dialog = Some(dialog);
    let ctx = egui::Context::default();
    ctx.run_ui(egui::RawInput::default(), |ui| {
        let _ = draw(ui, &s);
    })
    .drop_without_applying_deltas();
    let mut actions = Vec::new();
    ctx.run_ui(key_input(key), |ui| actions = draw(ui, &s))
        .drop_without_applying_deltas();
    actions
}

#[test]
fn enter_takes_the_safe_answer_and_esc_cancels() {
    for dialog in dialogs() {
        let expected = default_answer(&dialog);
        assert_eq!(
            dialog_key(dialog.clone(), egui::Key::Enter),
            vec![Action::DialogAnswer(expected)],
            "{dialog:?}"
        );
        assert_eq!(
            dialog_key(dialog.clone(), egui::Key::Escape),
            vec![Action::DialogAnswer(Answer::Cancel)],
            "{dialog:?}"
        );
    }
    let answers: Vec<Answer> = dialogs().iter().map(default_answer).collect();
    assert_eq!(
        answers,
        vec![
            Answer::Proceed,
            Answer::Skip,
            Answer::Cancel,
            Answer::Cancel,
            Answer::Cancel,
            Answer::Cancel,
            Answer::Cancel,
            Answer::Proceed,
            Answer::Cancel,
        ]
    );
}

#[test]
fn overwrite_text_names_the_operation() {
    assert_eq!(
        overwrite_text(OpKind::Move, 2, 5, "s3://b/p/"),
        "Move 5 items to s3://b/p/? 2 already exist there. The sources are deleted after copying."
    );
    assert_eq!(
        overwrite_text(OpKind::Copy, 1, 1, "file:///r/"),
        "Copy 1 item to file:///r/? 1 already exists there."
    );
}

#[test]
fn sizes_and_counts_read_well() {
    assert_eq!(human_size(0), "0 B");
    assert_eq!(human_size(1023), "1023 B");
    assert_eq!(human_size(1536), "1.5 KiB");
    assert_eq!(human_size(5 * 1024 * 1024), "5.0 MiB");
    assert_eq!(human_size(u64::MAX), "15.9 EiB");
    assert_eq!(thousands(0), "0");
    assert_eq!(thousands(999), "999");
    assert_eq!(thousands(50_000), "50,000");
    assert_eq!(thousands(1_234_567), "1,234,567");
    assert_eq!(count(1, "item"), "1 item");
    assert_eq!(count(2, "item"), "2 items");
}
