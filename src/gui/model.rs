//! State shared by the GUI.

use super::location::Location;
use chrono::{DateTime, Utc};
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use crate::backend::ObjectState;
use crate::connection::Connection;

/// How the opened client encrypts, and where its keys came from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Encryption {
    Off,
    File,
    Command,
}

/// The most bytes of any one string the GUI shows.
pub(crate) const DISPLAY_MAX: usize = 4_096;

/// Entries requested per listing page.
pub(crate) const PAGE_SIZE: usize = 1_000;
/// The most entries a panel keeps; more are cut and flagged as truncated.
pub(crate) const MAX_ENTRIES: usize = 50_000;
/// The most entries a pre-flight check examines.
pub(crate) const CHECK_LIMIT: usize = 1_000;
/// The most items a plan holds across all selected folders; a larger plan fails
/// as a whole before anything runs.
pub(crate) const MAX_PLAN_ITEMS: usize = 1_000_000;
/// Concurrent pre-flight checks.
pub(crate) const CHECK_CONCURRENCY: usize = 32;
/// Concurrent transfer operations.
pub(crate) const OP_CONCURRENCY: usize = 4;
/// How long closing waits for running work to drain.
pub(crate) const DRAIN_TIMEOUT: Duration = Duration::from_secs(10);

/// One of the two panels.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Side {
    Left,
    Right,
}

impl Side {
    /// The opposite panel.
    pub(crate) const fn other(self) -> Self {
        match self {
            Self::Left => Self::Right,
            Self::Right => Self::Left,
        }
    }

    /// The position in `AppState::panels`.
    pub(crate) const fn index(self) -> usize {
        match self {
            Self::Left => 0,
            Self::Right => 1,
        }
    }
}

/// What identifies a row of a listing: whether it is a folder, and its raw
/// name. A folder and an object may share a name.
pub(crate) type EntryKey = (bool, String);

/// A listed row. Names are raw: views format them with `display::text`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Entry {
    Folder {
        name: String,
    },
    Object {
        name: String,
        size: u64,
        modified: DateTime<Utc>,
        /// Whether it can be read now.
        state: ObjectState,
        /// The S3 storage class or Azure access tier, when listed.
        class: Option<String>,
    },
    /// A symlink of a `file://` folder, acted on as the link itself.
    Link {
        name: String,
    },
}

/// Why an object in `state` cannot be copied yet, or `None` when it is
/// online. `class` is the S3 storage class or Azure access tier; the Azure
/// tier names tell the two apart.
#[must_use]
pub fn unavailable_reason(state: ObjectState, class: Option<&str>) -> Option<String> {
    let azure = class.is_some_and(|c| {
        ["Hot", "Cool", "Cold", "Smart", "Archive"]
            .iter()
            .any(|tier| tier.eq_ignore_ascii_case(c))
    });
    let class = match class {
        Some(c) if azure => format!(" ({c} tier)"),
        Some(c) => format!(" ({c})"),
        None => String::new(),
    };
    match (state, azure) {
        (ObjectState::Online, _) => None,
        (ObjectState::Archived, false) => {
            Some(format!("Archived{class}: restore it before copying"))
        }
        (ObjectState::Archived, true) => {
            Some(format!("Archived{class}: rehydrate it before copying"))
        }
        (ObjectState::Restoring, false) => Some(format!(
            "Restore in progress{class}: it can be copied once it finishes"
        )),
        (ObjectState::Restoring, true) => Some(format!(
            "Rehydration in progress{class}: it can be copied once it finishes"
        )),
    }
}

impl Entry {
    /// The raw listed name.
    #[must_use]
    pub fn name(&self) -> &str {
        match self {
            Self::Folder { name } | Self::Object { name, .. } | Self::Link { name } => name,
        }
    }

    /// Whether this is a folder.
    #[must_use]
    pub const fn is_folder(&self) -> bool {
        matches!(self, Self::Folder { .. })
    }

    /// Listing order: folders first, then by name.
    pub(crate) fn listing_order(a: &Self, b: &Self) -> std::cmp::Ordering {
        (!a.is_folder(), a.name()).cmp(&(!b.is_folder(), b.name()))
    }

    /// The key that identifies this row in a selection.
    pub(crate) fn key(&self) -> EntryKey {
        (self.is_folder(), self.name().to_owned())
    }

    /// Whether this row is the one `key` identifies.
    pub(crate) fn is(&self, key: &EntryKey) -> bool {
        self.is_folder() == key.0 && self.name() == key.1
    }
}

/// One page of a listing.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Page {
    pub entries: Vec<Entry>,
    pub next: Option<String>,
}

/// Everything one panel shows.
pub(crate) struct PanelState {
    /// The connection whose client lists and changes `location`.
    pub(crate) connection: Arc<Connection>,
    pub(crate) location: Location,
    pub(crate) address_text: String,
    pub(crate) address_error: Option<String>,
    pub(crate) entries: Vec<Entry>,
    pub(crate) next: Option<String>,
    pub(crate) truncated: bool,
    pub(crate) loading: bool,
    pub(crate) error: Option<String>,
    pub(crate) selection: BTreeSet<EntryKey>,
    /// The fixed end of a range selection.
    pub(crate) anchor: Option<EntryKey>,
    /// The row keyboard motion moves from and Enter opens.
    pub(crate) cursor: Option<EntryKey>,
    /// How often keyboard motion moved the cursor; the view scrolls to the
    /// cursor when this changes.
    pub(crate) cursor_moves: u64,
    pub(crate) seq: u64,
}

impl PanelState {
    /// A panel at `location` of `connection`, waiting for its first page
    /// (listing `seq` 1).
    pub(crate) fn new(connection: Arc<Connection>, location: Location) -> Self {
        Self {
            connection,
            address_text: location.display(),
            location,
            address_error: None,
            entries: Vec::new(),
            next: None,
            truncated: false,
            loading: true,
            error: None,
            selection: BTreeSet::new(),
            anchor: None,
            cursor: None,
            cursor_moves: 0,
            seq: 1,
        }
    }

    /// Whether the rows belong to the previous folder: the first page of a
    /// navigation is still loading. Such rows are inert, since a name would
    /// resolve against the new location. (`Load more` keeps `next`, so a
    /// loading panel without one is waiting for its first page.)
    pub(crate) const fn stale(&self) -> bool {
        self.loading && self.next.is_none()
    }
}

/// Whether the window is open, draining work to close, or ready to close.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Closing {
    Open,
    Draining,
    Done,
}

/// Identifies one job, so late events from an older job are dropped.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct JobId(pub u64);

/// What a job does to its entries.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum OpKind {
    Copy,
    Move,
    Rename,
    Delete,
}

/// The entries one operation starts from, as the worker plans them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Batch {
    pub kind: OpKind,
    pub src_dir: Location,
    /// The selected entries of the active panel, in display order.
    pub entries: Vec<Entry>,
    /// The destination folder; for `Delete`, equal to `src_dir`.
    pub dst_dir: Location,
    /// The new name of the single entry of a `Rename`.
    pub rename_to: Option<String>,
}

/// One unit of planned work. URLs are raw; `rel` is the raw relative name.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Item {
    Transfer {
        src: String,
        dst: String,
        rel: String,
        /// The size the listing gave, 0 for a console folder marker.
        size: u64,
        /// Whether the destination exists; `None` when it was not checked.
        exists: Option<bool>,
    },
    /// Renames a symlink with `rename(2)`, without reading what it points to.
    RenameLink {
        src: String,
        dst: String,
        rel: String,
        /// Whether the destination exists; `None` when it was not checked.
        exists: Option<bool>,
    },
    DeleteObject {
        url: String,
        rel: String,
    },
    DeletePrefix {
        url: String,
        rel: String,
    },
}

/// One entry that failed. Both strings are already display text.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Failure {
    pub name: String,
    pub error: String,
}

/// The worker's plan for a batch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Plan {
    pub kind: OpKind,
    pub items: Vec<Item>,
    /// Whether every destination was checked for an existing object.
    pub checked: bool,
    pub failures: Vec<Failure>,
}

impl Plan {
    /// How many destinations are known to exist already.
    #[must_use]
    pub fn existing(&self) -> usize {
        self.items
            .iter()
            .filter(|item| {
                matches!(
                    item,
                    Item::Transfer {
                        exists: Some(true),
                        ..
                    } | Item::RenameLink {
                        exists: Some(true),
                        ..
                    }
                )
            })
            .count()
    }
}

/// What to do with a destination that exists already.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum OnExisting {
    Overwrite,
    Skip,
}

/// How a job ended.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Outcome {
    pub done: usize,
    pub cancelled: bool,
    pub failures: Vec<Failure>,
}

/// Where a job is in its life.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum JobPhase {
    Planning,
    Confirming(Plan),
    Running,
}

/// The one operation that may run at a time.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Job {
    pub(crate) id: JobId,
    pub(crate) kind: OpKind,
    pub(crate) phase: JobPhase,
    pub(crate) done: usize,
    pub(crate) total: usize,
    /// Display text of the item last reported.
    pub(crate) current: String,
    pub(crate) src_conn: Arc<Connection>,
    /// The folder the batch's entries are in.
    pub(crate) src_dir: Location,
    pub(crate) dst_conn: Arc<Connection>,
    /// The batch's destination folder, for the confirmation dialogs.
    pub(crate) dst_dir: Location,
    /// Cancel was pressed; a plan that arrives later is dropped.
    pub(crate) cancelled: bool,
}

/// A modal question or report. Every string is display text.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Dialog {
    /// `from` and `to` are raw names; the view escapes them. `folder` and
    /// `from` identify the entry renamed.
    Rename {
        side: Side,
        folder: bool,
        from: String,
        to: String,
        error: Option<String>,
    },
    Overwrite {
        kind: OpKind,
        existing: usize,
        total: usize,
        dst: String,
    },
    Unchecked {
        kind: OpKind,
        total: usize,
        dst: String,
    },
    ConfirmMove {
        total: usize,
        dst: String,
    },
    ConfirmDelete {
        count: usize,
        folders: usize,
    },
    ConfirmBucketDelete {
        count: usize,
        folders: usize,
        bucket: String,
        typed: String,
    },
    Failures {
        kind: OpKind,
        outcome: Outcome,
    },
    /// An SSO login in progress for the connection `conn_id` (shown as
    /// `label`). `code` is the user code and the page to approve it on, once
    /// the worker has them.
    Login {
        conn_id: String,
        label: String,
        code: Option<(String, String)>,
        error: Option<String>,
    },
    Quit,
}

/// The whole GUI state.
pub(crate) struct AppState {
    pub(crate) panels: [PanelState; 2],
    /// The connections to choose from; the first is always Local.
    pub(crate) connections: Vec<Arc<Connection>>,
    /// What the last scan could not use.
    pub(crate) notes: Vec<String>,
    /// Whether a scan is still running.
    pub(crate) discovering: bool,
    pub(crate) active: Side,
    pub(crate) status: String,
    pub(crate) encryption: Encryption,
    pub(crate) closing: Closing,
    pub(crate) next_job: u64,
    pub(crate) worker_alive: bool,
    pub(crate) job: Option<Job>,
    pub(crate) dialog: Option<Dialog>,
    /// A report that arrived while `Dialog::Quit` was open, shown if the user
    /// keeps the window open.
    pub(crate) deferred: Option<Dialog>,
}

#[cfg(test)]
mod reason_tests {
    use super::*;

    #[test]
    fn unavailable_reason_names_the_class_and_the_provider_verb() {
        let reason = |state, class| unavailable_reason(state, Some(class));
        assert_eq!(reason(ObjectState::Online, "STANDARD"), None);
        assert_eq!(
            reason(ObjectState::Archived, "GLACIER").as_deref(),
            Some("Archived (GLACIER): restore it before copying")
        );
        assert_eq!(
            reason(ObjectState::Archived, "Archive").as_deref(),
            Some("Archived (Archive tier): rehydrate it before copying")
        );
        assert_eq!(
            reason(ObjectState::Restoring, "DEEP_ARCHIVE").as_deref(),
            Some("Restore in progress (DEEP_ARCHIVE): it can be copied once it finishes")
        );
        assert_eq!(
            reason(ObjectState::Restoring, "Archive").as_deref(),
            Some("Rehydration in progress (Archive tier): it can be copied once it finishes")
        );
        assert_eq!(
            unavailable_reason(ObjectState::Archived, None).as_deref(),
            Some("Archived: restore it before copying")
        );
    }
}
