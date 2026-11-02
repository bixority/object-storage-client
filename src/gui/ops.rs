//! The worker's operations over the client: list a page, plan a batch and
//! execute the plan. Generic over the codec, so plain and encrypting clients
//! both work.

use super::display;
use super::location::Location;
use super::model::{
    Batch, CHECK_CONCURRENCY, CHECK_LIMIT, Entry, Failure, Item, OP_CONCURRENCY, OnExisting,
    OpKind, Outcome, PAGE_SIZE, Page, Plan,
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
    let entries = page
        .folders
        .into_iter()
        .map(|name| Entry::Folder { name })
        .chain(page.objects.into_iter().map(|object| Entry::Object {
            name: object.name,
            size: object.size,
            modified: object.modified,
        }))
        .collect();
    Ok(Page {
        entries,
        next: page.next,
    })
}

/// Expands a batch into items: folders become their objects (or one prefix
/// delete), and transfer destinations are checked for existing objects when
/// there are at most `CHECK_LIMIT` (1,000) items.
pub async fn plan<C: Codec>(client: &Client<C>, batch: Batch) -> Plan {
    let Batch {
        kind,
        src_dir,
        entries,
        dst_dir,
        rename_to,
    } = batch;
    let mut items = Vec::new();
    let mut failures = Vec::new();
    for entry in &entries {
        let name = entry.name();
        let target = rename_to.as_deref().unwrap_or(name);
        let result = if kind == OpKind::Delete {
            delete_item(&src_dir, entry).map(|item| vec![item])
        } else if entry.is_folder() {
            expand_folder(client, &src_dir, &dst_dir, (name, target), &mut failures).await
        } else {
            src_dir
                .child_object(name)
                .zip(dst_dir.child_object(target))
                .map(|(src, dst)| {
                    vec![Item::Transfer {
                        src,
                        dst,
                        rel: name.to_owned(),
                        exists: None,
                    }]
                })
                .ok_or_else(|| INVALID_NAME.to_owned())
        };
        match result {
            Ok(planned) => items.extend(planned),
            Err(error) => failures.push(Failure {
                name: display::text(name),
                error,
            }),
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

/// The transfers of every object below the folder `name` of `src_dir`, into
/// the folder `target` of `dst_dir`. Fails as a whole on a listing error or an
/// invalid folder name; an object with an invalid name is added to `failures`.
async fn expand_folder<C: Codec>(
    client: &Client<C>,
    src_dir: &Location,
    dst_dir: &Location,
    (name, target): (&str, &str),
    failures: &mut Vec<Failure>,
) -> Result<Vec<Item>, String> {
    let (Some(src), Some(dst)) = (src_dir.child_dir(name), dst_dir.child_dir(target)) else {
        return Err(INVALID_NAME.to_owned());
    };
    let mut items = Vec::new();
    for rel in list_all(client, &src).await? {
        let full = format!("{name}/{rel}");
        match src.child_object(&rel).zip(dst.child_object(&rel)) {
            Some((src, dst)) => items.push(Item::Transfer {
                src,
                dst,
                rel: full,
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

/// Every object name below `dir`, relative to it.
async fn list_all<C: Codec>(client: &Client<C>, dir: &Location) -> Result<Vec<String>, String> {
    let text = |err: crate::Error| display::text(&err.to_string());
    client
        .list_meta_stream(dir.as_str())
        .map_err(text)?
        .map_ok(|(name, _)| name)
        .try_collect()
        .await
        .map_err(text)
}

/// Sets `exists` on every transfer item. A failed check counts as existing,
/// so the overwrite question asks about it.
async fn check_existing<C: Codec>(client: &Client<C>, items: &mut [Item]) {
    let targets: Vec<(usize, String)> = items
        .iter()
        .enumerate()
        .filter_map(|(index, item)| match item {
            Item::Transfer { dst, .. } => Some((index, dst.clone())),
            Item::DeleteObject { .. } | Item::DeletePrefix { .. } => None,
        })
        .collect();
    let found: Vec<(usize, bool)> = futures::stream::iter(targets)
        .map(|(index, dst)| async move { (index, client.exists(&dst).await.unwrap_or(true)) })
        .buffer_unordered(CHECK_CONCURRENCY)
        .collect()
        .await;
    for (index, present) in found {
        if let Some(Item::Transfer { exists, .. }) = items.get_mut(index) {
            *exists = Some(present);
        }
    }
}

/// A running job's counters, shared with the worker so it can report a
/// cancelled job. Start each job with a fresh one.
#[derive(Debug, Default)]
pub struct Progress {
    /// Items processed, failures included.
    pub done: AtomicUsize,
    /// Items that succeeded.
    pub succeeded: AtomicUsize,
    /// Items that failed so far.
    pub failures: Mutex<Vec<Failure>>,
}

/// Runs a plan, `OP_CONCURRENCY` (4) items at a time, continuing past failures.
///
/// `notify(processed, total, current)` is called after each item, where
/// `current` is the item's display name. The returned `done` counts the
/// items that succeeded; the failures list the plan's first.
pub async fn execute<C: Codec>(
    client: &Client<C>,
    plan: Plan,
    on_existing: OnExisting,
    progress: &Progress,
    notify: &(dyn Fn(usize, usize, &str) + Send + Sync),
) -> Outcome {
    let Plan {
        kind,
        items,
        failures: planned,
        ..
    } = plan;
    let items: Vec<Item> = items
        .into_iter()
        .filter(|item| {
            on_existing == OnExisting::Overwrite
                || !matches!(
                    item,
                    Item::Transfer {
                        exists: Some(true),
                        ..
                    }
                )
        })
        .collect();
    let total = items.len();
    futures::stream::iter(items)
        .for_each_concurrent(OP_CONCURRENCY, |item| async move {
            let (rel, result) = run(client, kind, item).await;
            let shown = display::text(&rel);
            match result {
                Ok(()) => {
                    progress.succeeded.fetch_add(1, Ordering::SeqCst);
                }
                Err(err) => progress
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
        })
        .await;
    let mut failures = planned;
    failures.extend(
        progress
            .failures
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .cloned(),
    );
    Outcome {
        done: progress.succeeded.load(Ordering::SeqCst),
        cancelled: false,
        failures,
    }
}

/// Runs one item, returning its raw relative name and the result.
async fn run<C: Codec>(
    client: &Client<C>,
    kind: OpKind,
    item: Item,
) -> (String, crate::Result<()>) {
    match item {
        Item::Transfer { src, dst, rel, .. } => {
            let result = if kind == OpKind::Copy {
                client.copy(&src, &dst).await
            } else {
                client.move_object(&src, &dst).await
            };
            (rel, result)
        }
        Item::DeleteObject { url, rel } => (rel, client.delete(&url).await),
        Item::DeletePrefix { url, rel } => (rel, client.delete_by_prefix(&url).await),
    }
}
