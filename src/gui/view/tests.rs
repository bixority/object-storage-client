use super::dialogs::{default_answer, overwrite_text};
use super::panel::{
    DEFAULT_PAGE_ROWS, desaturate, human_size, panel_stroke, seen_moves, seen_moves_id, thousands,
};
use super::*;
use crate::gui::action::{Answer, Event, Motion, Msg, SelectOp, update};
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

#[test]
fn panel_border_width_does_not_depend_on_focus() {
    for visuals in [egui::Visuals::dark(), egui::Visuals::light()] {
        let active = panel_stroke(&visuals, true);
        let inactive = panel_stroke(&visuals, false);
        assert!((active.width - inactive.width).abs() < f32::EPSILON);
        assert!((active.width - 2.0).abs() < f32::EPSILON);
        assert_eq!(active.color, visuals.selection.bg_fill);
        assert_eq!(
            inactive.color,
            visuals.widgets.noninteractive.bg_stroke.color
        );
    }
}

/// The left panel lists many rows and keyboard motion put the cursor on the
/// last one: the next frame has a scroll to do.
fn moved_state() -> AppState {
    let mut s = state();
    let many = (0..500)
        .map(|i| Entry::Object {
            name: format!("r{i:03}"),
            size: 1,
            modified: DateTime::UNIX_EPOCH,
        })
        .collect();
    update(
        &mut s,
        Msg::Event(Event::Page {
            side: Side::Left,
            seq: 1,
            first: true,
            result: Ok(Page {
                entries: many,
                next: None,
            }),
        }),
    );
    update(
        &mut s,
        Msg::Action(Action::Select(
            Side::Left,
            SelectOp::Move {
                motion: Motion::End,
                extend: false,
            },
        )),
    );
    s
}

#[test]
fn render_with_cursor_and_pending_scroll() {
    let mut s = moved_state();
    assert_eq!(s.panels[0].cursor_moves, 1);
    assert_eq!(render(&s), Vec::new());
    // A cursor that is not selected is drawn too.
    update(
        &mut s,
        Msg::Action(Action::Select(Side::Left, SelectOp::Clear)),
    );
    assert_eq!(render(&s), Vec::new());
}

#[test]
fn view_remembers_moves_and_page_rows() {
    let s = moved_state();
    let ctx = egui::Context::default();
    assert_eq!(page_rows(&ctx, Side::Left), DEFAULT_PAGE_ROWS);
    assert_eq!(seen_moves(&ctx, Side::Left), None);
    for _ in 0..2 {
        ctx.run_ui(egui::RawInput::default(), |ui| {
            let _ = draw(ui, &s);
        })
        .drop_without_applying_deltas();
    }
    assert_eq!(seen_moves(&ctx, Side::Left), Some(1));
    let rows = page_rows(&ctx, Side::Left);
    assert!((1..500).contains(&rows), "{rows}");
}

/// Whether a frame painted a row with this name: the table lays out only the
/// rows in view, so the last row is painted only after it scrolled there.
fn row_painted(output: &egui::FullOutput, name: &str) -> bool {
    output.shapes.iter().any(|clipped| {
        matches!(&clipped.shape, egui::Shape::Text(text) if text.galley.text().contains(name))
    })
}

#[test]
fn keyboard_motion_scrolls_the_cursor_into_view() {
    let s = moved_state();
    let ctx = egui::Context::default();
    let mut drawn = false;
    // The scroll may animate: let time pass.
    for frame in 0..30 {
        let input = egui::RawInput {
            time: Some(f64::from(frame) * 0.1),
            ..Default::default()
        };
        let output = ctx.run_ui(input, |ui| {
            let _ = draw(ui, &s);
        });
        drawn = row_painted(&output, "r499");
        output.drop_without_applying_deltas();
    }
    assert!(drawn, "the cursor row was never scrolled into view");

    // Without a move, the table stays at the top: the cursor row is not drawn.
    let ctx = egui::Context::default();
    let mut drawn = true;
    for frame in 0..30 {
        let input = egui::RawInput {
            time: Some(f64::from(frame) * 0.1),
            ..Default::default()
        };
        let output = ctx.run_ui(input, |ui| {
            if frame == 0 {
                // As if the move had been scrolled for already.
                ui.ctx()
                    .data_mut(|d| d.insert_temp(seen_moves_id(Side::Left), 1_u64));
            }
            let _ = draw(ui, &s);
        });
        drawn = row_painted(&output, "r499");
        output.drop_without_applying_deltas();
    }
    assert!(!drawn, "scrolled without a new move");
}

#[test]
fn desaturate_keeps_luminance_and_alpha() {
    use eframe::egui::Color32;
    let blue = Color32::from_rgba_unmultiplied(0, 92, 128, 200);
    let grey = desaturate(blue);
    assert_eq!(
        (grey.r(), grey.g(), grey.b()),
        (grey.r(), grey.r(), grey.r())
    );
    assert_eq!(grey.a(), 200);
    assert_eq!(desaturate(Color32::WHITE), Color32::WHITE);
    assert_eq!(desaturate(Color32::BLACK), Color32::BLACK);
    let red = desaturate(Color32::from_rgb(255, 0, 0));
    assert_eq!(red.r(), 76, "0.299 of 255");
}
