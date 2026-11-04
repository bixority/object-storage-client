//! The eframe applications: the browser window and the startup error window.

use super::action::{Action, Command, Event, Msg, SelectOp, update};
use super::keys::map_key;
use super::model::{AppState, Closing};
use super::view;
use super::worker::WorkerHandle;
use eframe::egui;
use std::collections::VecDeque;
use std::time::Duration;
use tokio::sync::mpsc::error::{TryRecvError, TrySendError};

/// How soon to retry commands the full worker channel did not take.
const RETRY_AFTER: Duration = Duration::from_millis(50);

/// The browser window: feeds input and worker events to the reducer and the
/// reducer's commands to the worker.
pub(super) struct App {
    state: AppState,
    worker: WorkerHandle,
    /// Commands not yet taken by the worker, in order.
    pending: VecDeque<Command>,
    /// Whether `Event::WorkerGone` was fed already.
    gone: bool,
    /// Shortcuts pressed this frame, mapped before egui saw the keys.
    keys: Vec<Action>,
}

impl App {
    /// `commands` are the ones `AppState::new` returned.
    pub(super) fn new(state: AppState, worker: WorkerHandle, commands: Vec<Command>) -> Self {
        Self {
            state,
            worker,
            pending: commands.into(),
            gone: false,
            keys: Vec::new(),
        }
    }

    fn apply(&mut self, msg: Msg) {
        let commands = update(&mut self.state, msg);
        self.pending.extend(commands);
    }

    /// Feeds `Event::WorkerGone`, once.
    fn worker_gone(&mut self) {
        if !self.gone {
            self.gone = true;
            self.pending.clear();
            self.apply(Msg::Event(Event::WorkerGone));
        }
    }

    /// Hands pending commands to the worker without blocking.
    fn flush(&mut self, ctx: &egui::Context) {
        while let Some(command) = self.pending.pop_front() {
            match self.worker.commands.try_send(command) {
                Ok(()) => {}
                Err(TrySendError::Full(command)) => {
                    self.pending.push_front(command);
                    ctx.request_repaint_after(RETRY_AFTER);
                    return;
                }
                Err(TrySendError::Closed(_)) => {
                    self.worker_gone();
                    return;
                }
            }
        }
    }
}

impl eframe::App for App {
    fn logic(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        loop {
            match self.worker.events.try_recv() {
                Ok(event) => self.apply(Msg::Event(event)),
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    self.worker_gone();
                    break;
                }
            }
        }
        if ctx.input(|i| i.viewport().close_requested()) && self.state.closing != Closing::Done {
            self.apply(Msg::Action(Action::CloseRequested));
            // With nothing to drain the window may close at once.
            if self.state.closing != Closing::Done {
                ctx.send_viewport_cmd(egui::ViewportCommand::CancelClose);
            }
        }
        if self.state.closing == Closing::Done {
            ctx.send_viewport_cmd(egui::ViewportCommand::Close);
        }
        self.flush(ctx);
    }

    fn raw_input_hook(&mut self, ctx: &egui::Context, raw_input: &mut egui::RawInput) {
        // The focus and popups as of the key press: the previous frame's. An
        // open popup (the connection list) keeps the arrows, Tab, Enter and Esc.
        let input_busy = ctx.text_edit_focused() || ctx.any_popup_open();
        let state = &self.state;
        // A page is what the active table showed fully last frame.
        let page_rows = view::page_rows(ctx, state.active);
        let keys = &mut self.keys;
        raw_input.events.retain(|event| {
            let egui::Event::Key {
                key,
                pressed: true,
                repeat,
                modifiers,
                ..
            } = event
            else {
                return true;
            };
            let Some(action) = map_key(*key, *modifiers, state, input_busy, page_rows) else {
                return true;
            };
            // A held arrow or page key keeps moving; other shortcuts fire once.
            let motion = matches!(action, Action::Select(_, SelectOp::Move { .. }));
            if *repeat && !motion {
                return true;
            }
            keys.push(action);
            // Tab switches panels and the motion keys move the cursor here;
            // egui would also move the focus or scroll.
            !(motion || *key == egui::Key::Tab)
        });
    }

    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        let ctx = ui.ctx().clone();
        let mut actions = view::draw(ui, &self.state);
        actions.append(&mut self.keys);
        for action in actions {
            if let Action::FocusAddress(side) = action {
                ctx.memory_mut(|m| m.request_focus(view::address_id(side)));
            }
            self.apply(Msg::Action(action));
        }
        self.flush(&ctx);
    }
}

/// The window shown when the client could not be opened.
pub(super) struct StartupErrorApp {
    /// Display text: sanitized by the caller.
    pub(super) message: String,
}

impl eframe::App for StartupErrorApp {
    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        if view::draw_startup_error(ui, &self.message) {
            ui.ctx().send_viewport_cmd(egui::ViewportCommand::Close);
        }
    }
}
