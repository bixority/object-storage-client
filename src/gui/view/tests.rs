use super::dialogs::{default_answer, overwrite_text};
use super::panel::{
    DEFAULT_PAGE_ROWS, desaturate, human_size, panel_stroke, seen_moves, seen_moves_id, thousands,
};
use super::*;
use crate::backend::ObjectState;
use crate::connection::sso::SsoLogin;
use crate::connection::{Connection, Login, Provider, Source};
use crate::gui::action::{Answer, Event, Motion, Msg, SelectOp, update};
use crate::gui::location::Location;
use crate::gui::model::{
    Closing, Dialog, Encryption, Entry, Failure, Job, JobId, JobPhase, Outcome, Page, Plan,
};
use chrono::DateTime;
use std::sync::Arc;

fn loc(url: &str) -> Location {
    Location::parse(url).expect("test url parses")
}

fn state() -> AppState {
    AppState::at(loc("s3://bucket/"), loc("file:///r/"), Encryption::File)
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
            state: ObjectState::Online,
            class: None,
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
        src_conn: Arc::new(Connection::local()),
        src_dir: loc("file:///l/"),
        dst_conn: Arc::new(Connection::local()),
        dst_dir: loc("file:///r/"),
        cancelled: false,
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
        Dialog::Failures {
            kind: OpKind::Delete,
            outcome: Outcome {
                done: 7,
                cancelled: true,
                failures,
            },
        },
        Dialog::Login {
            conn_id: "aws:dev".to_owned(),
            label: "AWS dev".to_owned(),
            code: None,
            error: None,
        },
        Dialog::Quit,
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
        if matches!(dialog, Dialog::Quit) {
            s.job = Some(job(JobPhase::Running));
        }
        s.dialog = Some(dialog);
        fixtures.push(s);
    }

    // The job ended while the question stayed open.
    let mut ended = main_state();
    ended.dialog = Some(Dialog::Quit);
    fixtures.push(ended);

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
            Answer::Proceed,
            Answer::Cancel,
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
            state: ObjectState::Online,
            class: None,
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
    // A cursor that is not selected renders without actions too.
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

fn sso() -> Connection {
    Connection {
        id: "aws:dev".to_owned(),
        label: "AWS dev".to_owned(),
        provider: Provider::S3,
        source: Source::Env,
        login: Some(Login::AwsSso(SsoLogin {
            identifier: "dev".to_owned(),
            start_url: "https://example.awsapps.com/start".to_owned(),
            region: "eu-west-1".to_owned(),
            scopes: Vec::new(),
        })),
    }
}

fn connections_state() -> AppState {
    let mut s = main_state();
    s.connections = vec![Arc::new(Connection::local()), Arc::new(sso())];
    s.notes = vec!["gcloud is not installed".to_owned()];
    s
}

/// Everything painted by one frame.
fn painted(shapes: &[egui::epaint::ClippedShape]) -> String {
    shapes
        .iter()
        .filter_map(|clipped| match &clipped.shape {
            egui::Shape::Text(text) => Some(text.galley.text().to_owned()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// One frame of `state` fed `events`: what it painted and the actions.
fn frame(
    ctx: &egui::Context,
    state: &AppState,
    events: Vec<egui::Event>,
) -> (Vec<egui::epaint::ClippedShape>, Vec<Action>) {
    let mut actions = Vec::new();
    let output = ctx.run_ui(
        egui::RawInput {
            events,
            ..Default::default()
        },
        |ui| actions = draw(ui, state),
    );
    let shapes = output.shapes.clone();
    output.drop_without_applying_deltas();
    (shapes, actions)
}

/// Where `text` was first painted, inside the glyphs.
fn find_text(shapes: &[egui::epaint::ClippedShape], text: &str) -> egui::Pos2 {
    shapes
        .iter()
        .find_map(|clipped| match &clipped.shape {
            egui::Shape::Text(t) if t.galley.text() == text => Some(t.pos + egui::vec2(4.0, 4.0)),
            _ => None,
        })
        .unwrap_or_else(|| panic!("{text} is not painted"))
}

/// A context after two frames of `state`, and what the second painted.
fn settled(state: &AppState) -> (egui::Context, Vec<egui::epaint::ClippedShape>) {
    let ctx = egui::Context::default();
    let _ = frame(&ctx, state, Vec::new());
    let (shapes, _) = frame(&ctx, state, Vec::new());
    (ctx, shapes)
}

/// Clicks at `pos` over a few frames: what the last frame painted and the
/// actions of all of them.
fn click(
    ctx: &egui::Context,
    state: &AppState,
    pos: egui::Pos2,
) -> (Vec<egui::epaint::ClippedShape>, Vec<Action>) {
    let button = |pressed| egui::Event::PointerButton {
        pos,
        button: egui::PointerButton::Primary,
        pressed,
        modifiers: egui::Modifiers::NONE,
    };
    let mut seen = Vec::new();
    let mut actions = Vec::new();
    for events in [
        vec![egui::Event::PointerMoved(pos)],
        vec![button(true)],
        vec![button(false)],
        Vec::new(),
        Vec::new(),
    ] {
        let (shapes, got) = frame(ctx, state, events);
        seen = shapes;
        actions.extend(got);
    }
    (seen, actions)
}

/// A context with the left panel's selector open.
fn selector_open(s: &AppState) -> (egui::Context, Vec<egui::epaint::ClippedShape>) {
    let (ctx, shapes) = settled(s);
    let (shapes, actions) = click(&ctx, s, find_text(&shapes, "Local"));
    assert!(actions.is_empty() && ctx.any_popup_open());
    (ctx, shapes)
}

#[test]
fn selector_shows_the_label_and_opens_to_the_items() {
    let mut s = connections_state();
    s.discovering = true;
    let (_, shapes) = settled(&s);
    assert!(painted(&shapes).contains("Local"));
    assert!(!painted(&shapes).contains("AWS dev"));
    let (_, shapes) = selector_open(&s);
    let seen = painted(&shapes);
    for item in ["AWS dev", "Searching…", "gcloud is not installed"] {
        assert!(seen.contains(item), "{item} missing in {seen}");
    }
}

#[test]
fn the_selector_items_emit_their_actions() {
    let mut s = connections_state();
    s.discovering = true;
    let (ctx, shapes) = selector_open(&s);
    let (_, actions) = click(&ctx, &s, find_text(&shapes, "AWS dev"));
    assert_eq!(
        actions,
        [Action::SelectConnection {
            side: Side::Left,
            id: "aws:dev".to_owned()
        }]
    );

    s.discovering = false;
    let (ctx, shapes) = selector_open(&s);
    let seen = painted(&shapes);
    assert!(
        seen.contains("Rescan") && !seen.contains("Searching…"),
        "{seen}"
    );
    let (_, actions) = click(&ctx, &s, find_text(&shapes, "Rescan"));
    assert_eq!(actions, [Action::Rescan]);
}

/// Double-clicks at `pos`, with explicit times so egui counts a double click.
fn double_click(ctx: &egui::Context, state: &AppState, pos: egui::Pos2) -> Vec<Action> {
    let button = |pressed| egui::Event::PointerButton {
        pos,
        button: egui::PointerButton::Primary,
        pressed,
        modifiers: egui::Modifiers::NONE,
    };
    let frames = [
        vec![egui::Event::PointerMoved(pos)],
        vec![button(true)],
        vec![button(false)],
        vec![button(true)],
        vec![button(false)],
        Vec::new(),
    ];
    let mut actions = Vec::new();
    let mut time = 100.0;
    for events in frames {
        let mut frame_actions = Vec::new();
        let output = ctx.run_ui(
            egui::RawInput {
                events,
                time: Some(time),
                ..Default::default()
            },
            |ui| frame_actions = draw(ui, state),
        );
        output.drop_without_applying_deltas();
        actions.extend(frame_actions);
        time += 0.05;
    }
    actions
}

#[test]
fn the_dotdot_row_goes_up_only_where_there_is_a_level_above() {
    let mut s = connections_state();
    s.panels[0].connection = Arc::new(sso());
    // The right panel must not show a ".." row of its own.
    s.panels[1].location = loc("file:///");
    for (url, expected) in [
        ("s3://media/", vec![Action::Up(Side::Left)]),
        ("s3:///", vec![]),
        ("file:///", vec![]),
    ] {
        s.panels[0].location = loc(url);
        let (ctx, shapes) = settled(&s);
        let shown = painted(&shapes).contains("🗀 ..");
        assert_eq!(shown, !expected.is_empty(), "{url}");
        if shown {
            let actions = double_click(&ctx, &s, find_text(&shapes, "🗀 .."));
            assert_eq!(actions, expected, "{url}");
        }
    }
}

#[test]
fn a_long_error_keeps_the_banner_buttons_in_the_panel() {
    let mut s = connections_state();
    s.panels[0].connection = Arc::new(sso());
    s.panels[0].error = Some(
        "Object store error: Generic S3 error: AWS profile `bixority` credentials \
         unavailable: the credential source or role assumption failed (for an SSO \
         profile, run `aws sso login --profile bixority`)"
            .to_owned(),
    );
    let ctx = egui::Context::default();
    let input = || egui::RawInput {
        screen_rect: Some(egui::Rect::from_min_size(
            egui::Pos2::ZERO,
            egui::vec2(1280.0, 800.0),
        )),
        ..Default::default()
    };
    let mut shapes = Vec::new();
    for _ in 0..2 {
        let output = ctx.run_ui(input(), |ui| {
            let _ = draw(ui, &s);
        });
        shapes.clone_from(&output.shapes);
        output.drop_without_applying_deltas();
    }
    // The left panel is the left half of the window.
    for button in ["Retry", "Log in"] {
        let right = shapes
            .iter()
            .find_map(|clipped| match &clipped.shape {
                egui::Shape::Text(t) if t.galley.text() == button => {
                    Some(t.pos.x + t.galley.rect.width())
                }
                _ => None,
            })
            .unwrap_or_else(|| panic!("{button} is not painted"));
        assert!(right < 640.0, "{button} ends at x = {right}");
    }
}

#[test]
fn error_banner_offers_the_login_of_the_connection() {
    let mut s = connections_state();
    s.panels[0].connection = Arc::new(sso());
    s.panels[0].error = Some("expired".to_owned());
    let (ctx, shapes) = settled(&s);
    let seen = painted(&shapes);
    assert!(seen.contains("Log in") && seen.contains("Retry"), "{seen}");
    let (_, actions) = click(&ctx, &s, find_text(&shapes, "Log in"));
    assert_eq!(actions, [Action::LogIn { side: Side::Left }]);

    let mut command = sso();
    command.login = Some(Login::Command("gcloud auth login"));
    s.panels[0].connection = Arc::new(command);
    let (shapes, _) = frame(&ctx, &s, Vec::new());
    let seen = painted(&shapes);
    assert!(
        seen.contains("Run `gcloud auth login` in a terminal, then Rescan"),
        "{seen}"
    );
    assert!(!seen.contains("Log in"), "{seen}");
}

#[test]
fn login_dialog_shows_the_code_and_the_buttons() {
    let login = |code: Option<(&str, &str)>, error: Option<&str>| Dialog::Login {
        conn_id: "aws:dev".to_owned(),
        label: "AWS dev".to_owned(),
        code: code.map(|(c, u)| (c.to_owned(), u.to_owned())),
        error: error.map(str::to_owned),
    };
    let seen = |dialog| {
        let mut s = main_state();
        s.dialog = Some(dialog);
        painted(&settled(&s).1)
    };
    let waiting = seen(login(None, None));
    assert!(waiting.contains("Waiting…") && waiting.contains("Cancel"));
    assert!(!waiting.contains("Open browser"));
    let code = seen(login(Some(("ABCD-EFGH", "https://x/verify")), None));
    for text in ["Log in to AWS dev", "ABCD-EFGH", "Open browser", "Cancel"] {
        assert!(code.contains(text), "{text} missing in {code}");
    }
    let failed = seen(login(None, Some("denied")));
    assert!(failed.contains("denied") && failed.contains("Close"));
    assert!(!failed.contains("Cancel"));

    let mut s = main_state();
    s.dialog = Some(login(None, None));
    let (ctx, shapes) = settled(&s);
    let (_, actions) = click(&ctx, &s, find_text(&shapes, "Cancel"));
    assert_eq!(actions, [Action::DialogAnswer(Answer::Cancel)]);
}

#[test]
fn each_object_state_has_its_own_icon_and_hover_text() {
    let object = |name: &str, state, class: Option<&str>| Entry::Object {
        name: name.to_owned(),
        size: 1,
        modified: DateTime::UNIX_EPOCH,
        state,
        class: class.map(str::to_owned),
    };
    let mut s = state();
    update(
        &mut s,
        Msg::Event(Event::Page {
            side: Side::Left,
            seq: 1,
            first: true,
            result: Ok(Page {
                entries: vec![
                    object("cold.bin", ObjectState::Archived, Some("GLACIER")),
                    object("blue.bin", ObjectState::Archived, Some("Archive")),
                    object("thaw.bin", ObjectState::Restoring, Some("GLACIER")),
                    object("warm.bin", ObjectState::Online, Some("STANDARD")),
                ],
                next: None,
            }),
        }),
    );
    let (ctx, shapes) = settled(&s);
    let text = painted(&shapes);
    assert!(text.contains("❄ cold.bin"), "{text}");
    assert!(text.contains("❄ blue.bin"), "{text}");
    assert!(text.contains("⌛ thaw.bin"), "{text}");
    assert!(text.contains("🗋 warm.bin"), "{text}");
    assert!(!text.contains("restore it before copying"));

    let mut time = 10.0;
    let mut hover = |name: &str| {
        let pos = find_text(&shapes, name);
        let mut seen = String::new();
        // The tooltip waits for the pointer to rest: move it once.
        for frame in 0..30 {
            let output = ctx.run_ui(
                egui::RawInput {
                    events: (frame == 0)
                        .then_some(egui::Event::PointerMoved(pos))
                        .into_iter()
                        .collect(),
                    time: Some(time),
                    ..Default::default()
                },
                |ui| drop(draw(ui, &s)),
            );
            seen = painted(&output.shapes);
            output.drop_without_applying_deltas();
            time += 0.1;
        }
        seen
    };
    assert!(hover("❄ cold.bin").contains("Archived (GLACIER): restore it before copying"));
    assert!(hover("❄ blue.bin").contains("Archived (Archive tier): rehydrate it before copying"));
    assert!(hover("⌛ thaw.bin").contains("Restore in progress (GLACIER)"));
    assert!(!hover("🗋 warm.bin").contains("before copying"));
}
