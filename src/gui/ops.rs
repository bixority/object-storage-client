//! The worker's operations over the client: list a page, plan a batch and
//! execute the plan. Generic over the codec, so plain and encrypting clients
//! both work.

use super::display;
use super::location::Location;
use super::model::{
    Batch, CHECK_CONCURRENCY, CHECK_LIMIT, Entry, Failure, Item, MAX_ENTRIES, MAX_PLAN_ITEMS,
    OP_CONCURRENCY, OnExisting, OpKind, Outcome, PAGE_SIZE, Page, Plan,
};
use crate::{Client, Codec};
use futures::{StreamExt as _, TryStreamExt as _};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, PoisonError};

#[cfg(test)]
mod tests;

/// One page of the folder `dir`, `PAGE_SIZE` (1,000) entries at most.
///
/// # Errors
/// Returns display text when the listing fails.
pub async fn list_page<C: Codec>(
    client: &Client<C>,
    dir: &Location,
    token: Option<String>,
) -> Result<Page, String> {
    list_page_with(client, dir, token, PAGE_SIZE).await
}

/// [`list_page`] with an explicit page size.
///
/// # Errors
/// Returns display text when the listing fails.
#[doc(hidden)]
pub async fn list_page_with<C: Codec>(
    client: &Client<C>,
    dir: &Location,
    token: Option<String>,
    max: usize,
) -> Result<Page, String> {
    let page = client
        .list_dir(dir.as_str(), token, max)
        .await
        .map_err(|err| display::text(&err.to_string()))?;
    let mut entries: Vec<Entry> = page
        .folders
        .into_iter()
        .map(|name| Entry::Folder { name })
        .chain(page.objects.into_iter().map(|object| Entry::Object {
            name: object.name,
            size: object.size,
            modified: object.modified,
        }))
        .chain(page.links.into_iter().map(|name| Entry::Link { name }))
        .collect();
    // A store without paged listing sends a whole directory as one page.
    // Sort and cap it here so the UI thread's reducer finds it ordered, and
    // keep one entry past the cap, which is how it sees the listing is cut.
    if page.next.is_none() && entries.len() > MAX_ENTRIES {
        entries.sort_by(Entry::listing_order);
        entries.truncate(MAX_ENTRIES + 1);
    }
    Ok(Page {
        entries,
        next: page.next,
    })
}

/// Expands a batch into items: folders become their objects (or one prefix
/// delete), and transfer destinations are checked for existing objects when
/// there are at most `CHECK_LIMIT` (1,000) items.
///
/// A plan of more than `MAX_PLAN_ITEMS` (1,000,000) items fails as a whole.
pub async fn plan<C: Codec>(client: &Client<C>, batch: Batch) -> Plan {
    plan_with(client, batch, MAX_PLAN_ITEMS).await
}

/// [`plan`] with an explicit item cap.
#[doc(hidden)]
pub async fn plan_with<C: Codec>(client: &Client<C>, batch: Batch, max_items: usize) -> Plan {
    let Batch {
        kind,
        src_dir,
        entries,
        dst_dir,
        rename_to,
    } = batch;
    let mut items = Vec::new();
    let mut failures = Vec::new();
    // Folders are listed several at a time; the results come back in order.
    // One budget bounds what all the listings in flight hold together.
    let dirs = (&src_dir, &dst_dir);
    let rename_to = rename_to.as_deref();
    let budget = AtomicUsize::new(max_items);
    let listings: Vec<_> = entries
        .iter()
        .map(|entry| entry_items(client, kind, dirs, entry, rename_to, &budget))
        .collect();
    let mut results = futures::stream::iter(listings).buffered(CHECK_CONCURRENCY);
    let mut selected = entries.iter();
    while let Some((nested, result)) = results.next().await {
        failures.extend(nested);
        let name = selected.next().map_or("", Entry::name);
        let result = result.and_then(|planned| {
            if items.len() + planned.len() > max_items {
                Err(PlanError::TooMany)
            } else {
                Ok(planned)
            }
        });
        match result {
            Ok(planned) => items.extend(planned),
            Err(PlanError::Entry(error)) => failures.push(Failure {
                name: display::text(name),
                error,
            }),
            // Running part of a move would leave the source half moved. The
            // folders are listed at once from one budget, so which of them ran
            // out is not meaningful: the failure names the source folder.
            Err(PlanError::TooMany) => {
                return Plan {
                    kind,
                    items: Vec::new(),
                    checked: true,
                    failures: vec![Failure {
                        name: display::text(&src_dir.display()),
                        error: format!("more than {max_items} objects; select a deeper folder"),
                    }],
                };
            }
        }
    }
    let checked = kind == OpKind::Delete || items.len() <= CHECK_LIMIT;
    if kind != OpKind::Delete && checked {
        check_existing(client, &mut items).await;
    }
    Plan {
        kind,
        items,
        checked,
        failures,
    }
}

/// The items of one selected `entry`, with the failures of objects below a
/// folder whose names are invalid. Each object listed below a folder takes
/// one from `budget`, shared by every listing of the plan.
async fn entry_items<C: Codec>(
    client: &Client<C>,
    kind: OpKind,
    (src_dir, dst_dir): (&Location, &Location),
    entry: &Entry,
    rename_to: Option<&str>,
    budget: &AtomicUsize,
) -> (Vec<Failure>, Result<Vec<Item>, PlanError>) {
    let name = entry.name();
    let target = rename_to.unwrap_or(name);
    let mut failures = Vec::new();
    let result = if kind == OpKind::Delete {
        delete_item(src_dir, entry)
            .map(|item| vec![item])
            .map_err(PlanError::Entry)
    } else if entry.is_folder() {
        let dirs = (src_dir, dst_dir);
        expand_folder(client, dirs, (name, target), budget, &mut failures).await
    } else if matches!(entry, Entry::Link { .. }) {
        link_items(kind, src_dir, dst_dir, (name, target)).map_err(PlanError::Entry)
    } else {
        src_dir
            .child_object(name)
            .zip(dst_dir.child_object(target))
            .map(|(src, dst)| {
                vec![Item::Transfer {
                    src,
                    dst,
                    rel: name.to_owned(),
                    size: entry_size(entry),
                    exists: None,
                }]
            })
            .ok_or_else(|| PlanError::Entry(INVALID_NAME.to_owned()))
    };
    (failures, result)
}

/// The error of a symlink that is not copied or moved.
const LINK_SKIPPED: &str = "symbolic link skipped";

/// The items for the selected symlink `name` of `src_dir`, acted on as the
/// link itself. A rename moves the link in its folder. A move to another
/// `file://` folder renames the link. A copy, or a move to any other store,
/// is a failure: the link is never read through.
fn link_items(
    kind: OpKind,
    src_dir: &Location,
    dst_dir: &Location,
    (name, target): (&str, &str),
) -> Result<Vec<Item>, String> {
    let (Some(src), Some(dst)) = (src_dir.child_object(name), dst_dir.child_object(target)) else {
        return Err(INVALID_NAME.to_owned());
    };
    let rel = name.to_owned();
    match kind {
        // `rename(2)` on the link itself: never through the object
        // operations, which read what the link points at.
        OpKind::Rename | OpKind::Move if is_file_dir(src_dir) && is_file_dir(dst_dir) => {
            Ok(vec![Item::RenameLink {
                src,
                dst,
                rel,
                exists: None,
            }])
        }
        OpKind::Copy | OpKind::Move | OpKind::Rename | OpKind::Delete => {
            Err(LINK_SKIPPED.to_owned())
        }
    }
}

fn is_file_dir(dir: &Location) -> bool {
    dir.as_str().starts_with("file:")
}

/// The error of an entry whose name is empty, `.` or `..`, or has such a segment.
const INVALID_NAME: &str = "invalid name";

/// The delete item for `entry` in `dir`, or the error for an invalid name.
fn delete_item(dir: &Location, entry: &Entry) -> Result<Item, String> {
    let name = entry.name();
    let item = if entry.is_folder() {
        dir.child_dir(name).map(|url| Item::DeletePrefix {
            url: url.as_str().to_owned(),
            rel: name.to_owned(),
        })
    } else {
        dir.child_object(name).map(|url| Item::DeleteObject {
            url,
            rel: name.to_owned(),
        })
    };
    item.ok_or_else(|| INVALID_NAME.to_owned())
}

/// Why part of a plan failed.
enum PlanError {
    /// This entry fails; the rest of the plan goes on.
    Entry(String),
    /// The plan is over the item cap: it fails as a whole.
    TooMany,
}

/// The listed size of `entry`, 0 for anything but an object.
const fn entry_size(entry: &Entry) -> u64 {
    match entry {
        Entry::Object { size, .. } => *size,
        _ => 0,
    }
}

/// The transfers of every object below the folder `name` of `src_dir`, into
/// the folder `target` of `dst_dir`, each taking one from `budget`. Fails as
/// a whole on a listing error or an invalid folder name; an object with an
/// invalid name is added to `failures`. The listing is read as a stream and
/// stops when the budget runs out.
async fn expand_folder<C: Codec>(
    client: &Client<C>,
    (src_dir, dst_dir): (&Location, &Location),
    (name, target): (&str, &str),
    budget: &AtomicUsize,
    failures: &mut Vec<Failure>,
) -> Result<Vec<Item>, PlanError> {
    let (Some(src), Some(dst)) = (src_dir.child_dir(name), dst_dir.child_dir(target)) else {
        return Err(PlanError::Entry(INVALID_NAME.to_owned()));
    };
    let text = |err: crate::Error| PlanError::Entry(display::text(&err.to_string()));
    let mut listing = client.list_meta_stream(src.as_str()).map_err(text)?;
    let mut items = Vec::new();
    while let Some((rel, size)) = listing.try_next().await.map_err(text)? {
        if budget
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |left| {
                left.checked_sub(1)
            })
            .is_err()
        {
            return Err(PlanError::TooMany);
        }
        let full = format!("{name}/{rel}");
        match src.child_object(&rel).zip(dst.child_object(&rel)) {
            Some((src, dst)) => items.push(Item::Transfer {
                src,
                dst,
                rel: full,
                size,
                exists: None,
            }),
            None => failures.push(Failure {
                name: display::text(&full),
                error: INVALID_NAME.to_owned(),
            }),
        }
    }
    Ok(items)
}

/// Sets `exists` on every transfer item. A failed check counts as existing,
/// so the overwrite question asks about it.
async fn check_existing<C: Codec>(client: &Client<C>, items: &mut [Item]) {
    let targets: Vec<(usize, String, bool)> = items
        .iter()
        .enumerate()
        .filter_map(|(index, item)| match item {
            Item::Transfer { dst, .. } => Some((index, dst.clone(), false)),
            Item::RenameLink { dst, .. } => Some((index, dst.clone(), true)),
            Item::DeleteObject { .. } | Item::DeletePrefix { .. } => None,
        })
        .collect();
    let found: Vec<(usize, bool)> = futures::stream::iter(targets)
        .map(|(index, dst, link)| async move {
            let present = if link {
                // `rename(2)` replaces whatever is there, a link included:
                // look at the name itself, not what a link there points at.
                link_target_exists(&dst).await
            } else {
                client.exists(&dst).await.unwrap_or(true)
            };
            (index, present)
        })
        .buffer_unordered(CHECK_CONCURRENCY)
        .collect()
        .await;
    for (index, present) in found {
        if let Some(Item::Transfer { exists, .. } | Item::RenameLink { exists, .. }) =
            items.get_mut(index)
        {
            *exists = Some(present);
        }
    }
}

/// A running job's counters, shared with the worker so it can report a
/// cancelled job. Start each job with a fresh one, seeded with the plan's
/// failures.
#[derive(Debug, Default)]
pub struct Progress {
    /// Items processed, failures included.
    pub done: AtomicUsize,
    /// Items that succeeded.
    pub succeeded: AtomicUsize,
    /// The plan's failures, then the items that failed so far.
    pub failures: Mutex<Vec<Failure>>,
}

impl Progress {
    /// Counters for a job whose plan already failed on `planned`.
    #[must_use]
    pub fn new(planned: Vec<Failure>) -> Self {
        Self {
            failures: Mutex::new(planned),
            ..Self::default()
        }
    }

    /// What the job got done so far; takes the failures.
    pub fn outcome(&self) -> Outcome {
        Outcome {
            done: self.succeeded.load(Ordering::SeqCst),
            cancelled: false,
            failures: std::mem::take(
                &mut *self.failures.lock().unwrap_or_else(PoisonError::into_inner),
            ),
        }
    }
}

/// Runs a plan, `OP_CONCURRENCY` (4) items at a time, continuing past failures.
///
/// Selected objects of a bucket store are deleted in batches instead: the
/// progress then advances in bursts, and a cancelled job can leave a batch
/// partly applied.
///
/// `notify(processed, total, current)` is called after each item, where
/// `current` is the item's display name. The returned `done` counts the
/// items that succeeded; the failures are those `progress` was seeded with
/// (the plan's), then the run's.
pub async fn execute<C: Codec>(
    client: &Client<C>,
    plan: Plan,
    on_existing: OnExisting,
    progress: &Progress,
    notify: &(dyn Fn(usize, usize, &str) + Send + Sync),
) -> Outcome {
    let Plan { kind, items, .. } = plan;
    let items: Vec<Item> = items
        .into_iter()
        .filter(|item| {
            on_existing == OnExisting::Overwrite
                || !matches!(
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
        .collect();
    let total = items.len();
    // Selected objects on a bucket store go to the store's batched delete.
    let (bulk, items): (Vec<Item>, Vec<Item>) = items
        .into_iter()
        .partition(|item| matches!(item, Item::DeleteObject { url, .. } if bulk_deletable(url)));
    let each =
        futures::stream::iter(items).for_each_concurrent(OP_CONCURRENCY, |item| async move {
            let (rel, result) = run(client, kind, item).await;
            record(progress, notify, total, &rel, result);
        });
    let (urls, rels): (Vec<String>, Vec<String>) = bulk
        .into_iter()
        .filter_map(|item| match item {
            Item::DeleteObject { url, rel } => Some((url, rel)),
            _ => None,
        })
        .unzip();
    let batched = async {
        if !urls.is_empty() {
            client
                .delete_many(&urls, |index, result| {
                    let result = result.map_err(RunError::Failed);
                    record(progress, notify, total, &rels[index], result);
                })
                .await;
        }
    };
    batched.await;
    each.await;
    progress.outcome()
}

/// Counts one finished item and tells `notify`.
fn record(
    progress: &Progress,
    notify: &(dyn Fn(usize, usize, &str) + Send + Sync),
    total: usize,
    rel: &str,
    result: Result<(), RunError>,
) {
    let shown = display::text(rel);
    match result {
        Ok(()) => {
            progress.succeeded.fetch_add(1, Ordering::SeqCst);
        }
        // A console folder marker: listed, but not a key.
        Err(RunError::Skipped) => {}
        Err(RunError::Failed(err)) => progress
            .failures
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(Failure {
                name: shown.clone(),
                error: display::text(&err.to_string()),
            }),
    }
    let processed = progress.done.fetch_add(1, Ordering::SeqCst) + 1;
    notify(processed, total, &shown);
}

/// Whether a selected object is deleted with the store's batched delete. A
/// local file and an `http(s)://` URL (which needs a `WebDAV` check first) go
/// one by one.
fn bulk_deletable(url: &str) -> bool {
    !["file:", "http:", "https:"].iter().any(|scheme| {
        url.get(..scheme.len())
            .is_some_and(|s| s.eq_ignore_ascii_case(scheme))
    })
}

/// How an item that did not succeed ended.
enum RunError {
    /// A zero-size listed entry that is not there: a console folder marker.
    Skipped,
    Failed(crate::Error),
}

/// Runs one item, returning its raw relative name and the result.
async fn run<C: Codec>(
    client: &Client<C>,
    kind: OpKind,
    item: Item,
) -> (String, Result<(), RunError>) {
    let (rel, result) = match item {
        Item::Transfer {
            src,
            dst,
            rel,
            size,
            ..
        } => {
            let result = if kind == OpKind::Copy {
                // A zero size may be a console folder marker: let the copy find out.
                client
                    .copy_sized(&src, &dst, (size > 0).then_some(size))
                    .await
            } else {
                client.move_object(&src, &dst).await
            };
            return (
                rel,
                result.map_err(|err| match err {
                    crate::Error::ObjectStore(object_store::Error::NotFound { .. })
                        if size == 0 =>
                    {
                        RunError::Skipped
                    }
                    err => RunError::Failed(err),
                }),
            );
        }
        Item::RenameLink { src, dst, rel, .. } => (rel, rename_link(&src, &dst).await),
        Item::DeleteObject { url, rel } => (rel, client.delete(&url).await),
        Item::DeletePrefix { url, rel } => (rel, client.delete_by_prefix(&url).await),
    };
    (rel, result.map_err(RunError::Failed))
}

/// Renames the symlink `src` to `dst` (both `file://` URLs) with `rename(2)`.
/// A different file system is a failure: the link is not copied.
async fn rename_link(src: &str, dst: &str) -> crate::Result<()> {
    let (src, dst) = (link_path(src)?, link_path(dst)?);
    tokio::fs::rename(&src, &dst).await.map_err(|err| {
        if err.kind() == std::io::ErrorKind::CrossesDevices {
            crate::Error::InvalidInput(format!("{LINK_SKIPPED} (other file system)"))
        } else {
            crate::Error::Io(err)
        }
    })
}

fn link_path(url: &str) -> crate::Result<std::path::PathBuf> {
    let url = url::Url::parse(url)?;
    crate::backend::fs_path(&crate::registry::object_path(&url)?)
}

/// Whether anything (a dangling link included) has the name `dst`. A
/// failed check counts as present, so the overwrite question is asked.
async fn link_target_exists(dst: &str) -> bool {
    let Ok(path) = link_path(dst) else {
        return true;
    };
    match tokio::fs::symlink_metadata(path).await {
        Ok(_) => true,
        Err(e) => e.kind() != std::io::ErrorKind::NotFound,
    }
}
