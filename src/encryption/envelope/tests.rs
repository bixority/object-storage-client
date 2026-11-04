use std::fmt;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use object_store::memory::InMemory;
use object_store::path::Path as ObjectPath;
use object_store::{
    CopyOptions, GetOptions, GetResultPayload, MultipartUpload, ObjectStoreExt,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};

use super::*;
use crate::backend::TestBackend;
use crate::codec::sealed::CodecImpl as _;
use crate::encryption::keys::Keyring;
use crate::encryption::keys::{DataKey, EncryptedDataKey};

type StoredStream = BoxStream<'static, object_store::Result<Bytes>>;

fn plain_expecting(
    env: &Envelope,
    loc: &Location,
    stored: StoredStream,
    expect: Option<Header>,
) -> ByteStream {
    env.decode_stream(
        loc.clone(),
        stored,
        env.bound_key(loc),
        DecodeMode::Plaintext { expect },
    )
}

#[tokio::test]
async fn encryption_info_lists_only_own_namespace_keys() {
    let kms = EncryptedDataKey {
        provider_id: "aws-kms".to_owned(),
        provider_info:
            b"arn:aws:kms:eu-west-1:111122223333:key/1234abcd-12ab-34cd-56ef-1234567890ab".to_vec(),
        ciphertext: vec![0; 48],
    };
    let dek = DataKey::from_slice(&[7; 32]).expect("32 bytes");
    let ctx = EncryptionContext::new_reserved(None);
    let ring = Keyring::from_material("kek-1", &[("kek-1", &[1; 32])]);
    let own = ring.wrap(&dek, &ctx).await.expect("wrap").remove(0);
    let header = Header {
        message_id: [0; 32],
        context: EncryptionContext::new_reserved(None),
        edks: vec![kms, own],
        frame_length: 4096,
        commitment: [0; 32],
        auth_tag: [0; 16],
    };
    assert_eq!(encryption_info(&header, NAMESPACE).key_names, ["kek-1"]);
}

/// An `InMemory` wrapper with optional faults, recording the range of every
/// `get_opts` call.
#[derive(Debug, Default)]
struct TestStore {
    inner: InMemory,
    /// When set, unranged reads come from here: a two-faced source whose
    /// header probe (ranged) and full read disagree.
    full: Option<InMemory>,
    fail_ranged_reads: bool,
    /// A raw OS error code the copy fails with.
    copy_error: Option<std::io::ErrorKind>,
    ranges: Mutex<Vec<Option<GetRange>>>,
    /// Multipart uploads started.
    multiparts: AtomicUsize,
    /// `copy` calls received, failed or not.
    copies: AtomicUsize,
    /// Single and multipart writes fail.
    fail_puts: bool,
}

impl fmt::Display for TestStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TestStore")
    }
}

forward_object_store! {
    impl ObjectStore for TestStore => inner;
    forward [delete_stream, list, list_with_delimiter];
    {
        async fn put_opts(
            &self,
            location: &ObjectPath,
            payload: PutPayload,
            opts: PutOptions,
        ) -> object_store::Result<PutResult> {
            if self.fail_puts {
                return Err(Self::refused());
            }
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &ObjectPath,
            opts: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            if self.fail_puts {
                return Err(Self::refused());
            }
            self.multiparts.fetch_add(1, Ordering::SeqCst);
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(
            &self,
            location: &ObjectPath,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            self.ranges
                .lock()
                .expect("lock")
                .push(options.range.clone());
            if options.range.is_some() && self.fail_ranged_reads {
                return Err(object_store::Error::Generic {
                    store: "faulty",
                    source: "ranged read refused".into(),
                });
            }
            match &self.full {
                Some(full) if options.range.is_none() => full.get_opts(location, options).await,
                _ => self.inner.get_opts(location, options).await,
            }
        }

        async fn copy_opts(
            &self,
            from: &ObjectPath,
            to: &ObjectPath,
            options: CopyOptions,
        ) -> object_store::Result<()> {
            self.copies.fetch_add(1, Ordering::SeqCst);
            if let Some(code) = self.copy_error {
                return Err(object_store::Error::Generic {
                    store: "faulty",
                    source: std::io::Error::from(code).into(),
                });
            }
            self.inner.copy_opts(from, to, options).await
        }
    }
}

impl TestStore {
    fn refused() -> object_store::Error {
        object_store::Error::Generic {
            store: "faulty",
            source: "write refused".into(),
        }
    }

    fn requested(&self) -> Vec<Option<GetRange>> {
        self.ranges.lock().expect("lock").clone()
    }
}

fn fixture() -> (Envelope, Arc<TestStore>, TestBackend, Location) {
    let provider = Keyring::from_material("kek-1", &[("kek-1", &[7; 32])]);
    let envelope = Envelope::new(EncryptionConfig::new(Arc::new(provider)));
    let store = Arc::new(TestStore::default());
    let backend = TestBackend {
        store: store.clone(),
        multipart: false,
    };
    let loc = Location {
        url: Url::parse("test://bucket/obj").expect("url"),
        path: ObjectPath::from("obj"),
    };
    (envelope, store, backend, loc)
}

async fn put(env: &Envelope, store: &TestStore, loc: &Location, ctx: &CallerContext) {
    let stored = env
        .encode(loc, ctx, Bytes::from_static(b"payload"))
        .await
        .expect("encode");
    store
        .inner
        .put(&loc.path, stored.into())
        .await
        .expect("put");
}

#[tokio::test]
async fn header_probe_never_buffers_more_than_it_asked_for() {
    let store = InMemory::new();
    let path = ObjectPath::from("big");
    store
        .put(&path, vec![7u8; 1 << 20].into())
        .await
        .expect("put");
    // An unranged GET stands in for a server that ignores `Range`.
    let whole = store.get(&path).await.expect("get");
    assert_eq!(read_prefix(whole, 4096).await.expect("prefix").len(), 4096);
    let short = store.get(&path).await.expect("get");
    assert_eq!(
        read_prefix(short, 1 << 30).await.expect("prefix").len(),
        1 << 20
    );
}

#[tokio::test]
async fn small_header_takes_one_small_ranged_read() {
    let (env, store, backend, loc) = fixture();
    put(&env, &store, &loc, &CallerContext::new()).await;
    let meta = env.describe(&backend, &loc).await.expect("describe");
    assert_eq!(meta.size_bytes, 7);
    assert_eq!(store.requested(), [Some(GetRange::Bounded(0..1024))]);
}

#[tokio::test]
async fn large_header_takes_two_ranged_reads() {
    let (env, store, backend, loc) = fixture();
    let ctx = CallerContext::from([("big".to_owned(), "x".repeat(6000))]);
    put(&env, &store, &loc, &ctx).await;
    let meta = env.describe(&backend, &loc).await.expect("describe");
    assert_eq!(meta.size_bytes, 7);
    let max = env.cfg.limits.max_header_len as u64;
    assert_eq!(
        store.requested(),
        [
            Some(GetRange::Bounded(0..1024)),
            Some(GetRange::Bounded(0..max))
        ]
    );
    let info = meta.encryption.expect("info");
    assert_eq!(info.context["big"].len(), 6000);
}

#[tokio::test]
async fn plain_and_truncated_objects_are_rejected() {
    let (env, store, backend, loc) = fixture();
    for body in [&b""[..], b"x", b"plain text object"] {
        store
            .inner
            .put(&loc.path, body.to_vec().into())
            .await
            .expect("put");
        assert!(
            matches!(
                env.describe(&backend, &loc).await,
                Err(Error::NotEncrypted { .. })
            ),
            "{body:?}"
        );
    }
    put(&env, &store, &loc, &CallerContext::new()).await;
    let full = store
        .inner
        .get(&loc.path)
        .await
        .expect("get")
        .bytes()
        .await
        .expect("bytes");
    let header_len = env
        .read_header(&backend, &loc)
        .await
        .expect("header")
        .header_len;
    for cut in [3, 40, header_len - 1] {
        store
            .inner
            .put(&loc.path, full.slice(..cut).into())
            .await
            .expect("put");
        assert!(env.describe(&backend, &loc).await.is_err(), "cut {cut}");
    }
    // A header with no room for a final frame has no valid plaintext size.
    store
        .inner
        .put(&loc.path, full.slice(..header_len).into())
        .await
        .expect("put");
    assert!(matches!(
        env.describe(&backend, &loc).await,
        Err(Error::Decryption(DecryptError::Format(
            FormatError::Malformed(_)
        )))
    ));
}

/// A fake backend over `store`; `host`/`name` make the location URL.
fn resolved(host: &str, name: &str, store: Arc<dyn ObjectStore>, multipart: bool) -> Resolved {
    Resolved {
        backend: Arc::new(TestBackend { store, multipart }),
        loc: Location {
            url: Url::parse(&format!("test://{host}/{name}")).expect("url"),
            path: ObjectPath::from(name),
        },
    }
}

async fn stored(store: &InMemory) -> Bytes {
    stored_at(store, "obj").await
}

#[tokio::test]
async fn same_name_copy_to_another_store_is_reencrypted_and_a_move_deletes_the_source() {
    let (env, _, _, _) = fixture();
    let (a, b) = (Arc::new(InMemory::new()), Arc::new(InMemory::new()));
    let (src, dst) = (
        resolved("a", "obj", a.clone(), false),
        resolved("b", "obj", b.clone(), false),
    );
    let ctx = CallerContext::from([("tenant".to_owned(), "acme".to_owned())]);
    let bytes = env
        .encode(&src.loc, &ctx, Bytes::from_static(b"payload"))
        .await
        .expect("encode");
    a.put(&src.loc.path, bytes.clone().into())
        .await
        .expect("put");
    let before = env
        .inspect(src.backend.as_ref(), &src.loc)
        .await
        .expect("src");

    env.copy(&src, &dst, true).await.expect("move");
    assert_ne!(stored(&b).await, bytes);
    let after = env
        .inspect(dst.backend.as_ref(), &dst.loc)
        .await
        .expect("dst");
    assert_ne!(after.message_id, before.message_id, "fresh message");
    assert_eq!(after.context["tenant"], "acme");
    let got = dst.backend.store().get(&dst.loc.path).await.expect("get");
    let plain = env.get_bytes(&dst.loc, got).await.expect("decrypt");
    assert_eq!(plain.as_ref(), b"payload");
    assert!(a.head(&src.loc.path).await.is_err());
}

#[tokio::test]
async fn a_failed_destination_write_keeps_the_source_of_a_move() {
    let (env, _, _, _) = fixture();
    let a = Arc::new(TestStore::default());
    let b = Arc::new(TestStore {
        fail_puts: true,
        ..TestStore::default()
    });
    let (x, y) = (
        resolved("h", "x", a.clone(), false),
        resolved("h", "y", b.clone(), false),
    );
    put(&env, &a, &x.loc, &CallerContext::new()).await;

    assert!(env.copy(&x, &y, true).await.is_err());
    assert!(a.inner.head(&x.loc.path).await.is_ok());
    assert!(b.inner.head(&y.loc.path).await.is_err());
}

#[tokio::test]
async fn an_unbound_clients_same_bucket_copy_is_reencrypted_not_server_side() {
    let (env, _, _, _) = fixture();
    let env = Envelope::new(env.cfg.as_ref().clone().bind_object_key(false));
    let store = Arc::new(TestStore::default());
    let (x, y) = (
        resolved("h", "x", store.clone(), false),
        resolved("h", "y", store.clone(), false),
    );
    put(&env, &store, &x.loc, &CallerContext::new()).await;
    let before = env.inspect(x.backend.as_ref(), &x.loc).await.expect("src");

    for delete in [false, true] {
        env.copy(&x, &y, delete).await.expect("copy");
        assert_eq!(
            store.copies.load(Ordering::SeqCst),
            0,
            "no server-side copy"
        );
        let after = env.inspect(y.backend.as_ref(), &y.loc).await.expect("dst");
        assert_ne!(after.message_id, before.message_id, "fresh message");
        let got = y.backend.store().get(&y.loc.path).await.expect("get");
        assert_eq!(
            env.get_bytes(&y.loc, got).await.expect("decrypt").as_ref(),
            b"payload"
        );
        assert_eq!(store.inner.head(&x.loc.path).await.is_err(), delete);
        if !delete {
            store.inner.delete(&y.loc.path).await.expect("delete");
        }
    }
}

#[tokio::test]
async fn same_key_copy_from_an_old_key_reencrypts_with_a_fresh_message() {
    let (old_env, _, _, _) = fixture();
    let provider = Keyring::from_material("kek-2", &[("kek-2", &[9; 32]), ("kek-1", &[7; 32])]);
    let env = Envelope::new(EncryptionConfig::new(Arc::new(provider)));

    let (a, b) = (Arc::new(InMemory::new()), Arc::new(InMemory::new()));
    let (src, dst) = (
        resolved("a", "obj", a.clone(), false),
        resolved("b", "obj", b.clone(), false),
    );
    let ctx = CallerContext::from([("tenant".to_owned(), "acme".to_owned())]);
    let bytes = old_env
        .encode(&src.loc, &ctx, Bytes::from_static(b"payload"))
        .await
        .expect("encode");
    a.put(&src.loc.path, bytes.into()).await.expect("put");

    env.copy(&src, &dst, false).await.expect("copy");
    let before = env
        .inspect(src.backend.as_ref(), &src.loc)
        .await
        .expect("src");
    let after = env
        .inspect(dst.backend.as_ref(), &dst.loc)
        .await
        .expect("dst");
    assert_eq!(before.key_names, ["kek-1"]);
    assert_eq!(after.key_names, ["kek-2"]);
    assert_ne!(before.message_id, after.message_id);
    assert_eq!(after.context["tenant"], "acme");
    assert_eq!(after.context["osc:key"], "obj");

    let got = env
        .get_bytes(
            &dst.loc,
            dst.backend.store().get(&dst.loc.path).await.expect("get"),
        )
        .await
        .expect("decrypt");
    assert_eq!(got.as_ref(), b"payload");
    // The source is untouched by a copy.
    assert!(a.head(&src.loc.path).await.is_ok());
}

async fn put_at(env: &Envelope, at: &Resolved, bound_to: &Resolved, plain: &'static [u8]) {
    let bytes = env
        .encode(
            &bound_to.loc,
            &CallerContext::new(),
            Bytes::from_static(plain),
        )
        .await
        .expect("encode");
    at.backend
        .store()
        .put(&at.loc.path, bytes.into())
        .await
        .expect("put");
}

#[tokio::test]
async fn source_bound_to_another_name_is_not_copied_or_moved() {
    let (env, _, _, _) = fixture();
    let store = Arc::new(InMemory::new());
    let (x, y) = (
        resolved("a", "x", store.clone(), false),
        resolved("a", "y", store.clone(), false),
    );
    // The provider placed y's ciphertext at x.
    put_at(&env, &x, &y, b"content of y").await;
    for delete in [false, true] {
        let err = env.copy(&x, &y, delete).await.expect_err("binding");
        assert!(matches!(
            err,
            Error::Decryption(DecryptError::BindingMismatch { .. })
        ));
        assert!(store.head(&y.loc.path).await.is_err());
        assert!(store.head(&x.loc.path).await.is_ok());
    }
}

#[tokio::test]
async fn move_of_a_source_with_a_flipped_binding_keeps_the_source() {
    let (env, _, _, _) = fixture();
    let store = Arc::new(InMemory::new());
    let (x, y) = (
        resolved("a", "x", store.clone(), false),
        resolved("a", "y", store.clone(), false),
    );
    put_at(&env, &x, &x, b"data").await;
    let mut raw = stored_at(&store, "x").await.to_vec();
    let at = raw
        .windows(7)
        .position(|w| w == b"osc:key")
        .expect("key name");
    let value = &mut raw[at + 7..];
    let pos = value.iter().position(|&b| b == b'x').expect("value");
    value[pos] = b'y';
    store.put(&x.loc.path, raw.into()).await.expect("put");

    assert!(env.copy(&x, &y, true).await.is_err());
    assert!(store.head(&x.loc.path).await.is_ok());
    assert!(store.head(&y.loc.path).await.is_err());
}

#[tokio::test]
async fn copy_to_another_name_skips_the_probe_and_keeps_the_caller_context() {
    let (env, _, _, _) = fixture();
    let (a, b) = (
        Arc::new(TestStore::default()),
        Arc::new(TestStore::default()),
    );
    let (x, y) = (
        resolved("h", "x", a.clone(), false),
        resolved("h", "y", b.clone(), false),
    );
    let ctx = CallerContext::from([("tenant".to_owned(), "acme".to_owned())]);
    let bytes = env
        .encode(&x.loc, &ctx, Bytes::from_static(b"payload"))
        .await
        .expect("encode");
    a.inner
        .put(&x.loc.path, bytes.clone().into())
        .await
        .expect("put");
    let before = env
        .inspect(x.backend.as_ref(), &x.loc)
        .await
        .expect("inspect");
    a.ranges.lock().expect("lock").clear();

    env.copy(&x, &y, false).await.expect("copy");
    // One whole read of the source, no header probe.
    assert_eq!(a.requested(), [None]);
    let after = env
        .inspect(y.backend.as_ref(), &y.loc)
        .await
        .expect("inspect");
    assert_eq!(after.context["tenant"], "acme");
    assert_eq!(after.context[KEY_OBJECT], "y");
    assert_ne!(after.message_id, before.message_id, "fresh message");
    let got = y.backend.store().get(&y.loc.path).await.expect("get");
    let plain = env.get_bytes(&y.loc, got).await.expect("decrypt");
    assert_eq!(plain.as_ref(), b"payload");

    // An empty object has no frames to wait for, and still copies.
    let empty = env
        .encode(&x.loc, &ctx, Bytes::new())
        .await
        .expect("encode");
    a.inner.put(&x.loc.path, empty.into()).await.expect("put");
    env.copy(&x, &y, true).await.expect("copy empty");
    let got = y.backend.store().get(&y.loc.path).await.expect("get");
    assert!(
        env.get_bytes(&y.loc, got)
            .await
            .expect("decrypt")
            .is_empty()
    );
    assert!(a.inner.head(&x.loc.path).await.is_err(), "moved");
}

#[tokio::test]
async fn copy_to_another_name_of_a_bad_source_commits_nothing() {
    let (env, _, _, _) = fixture();
    let unbound = Envelope::new(env.cfg.as_ref().clone().bind_object_key(false));
    let x_loc = resolved("h", "x", Arc::new(TestStore::default()), false).loc;
    let y_loc = resolved("h", "y", Arc::new(TestStore::default()), false).loc;
    let header_len = |b: &Bytes| header_of(&env, b).1;
    let flip = |b: &Bytes, at: usize| {
        let mut v = b.to_vec();
        v[at] ^= 1;
        Bytes::from(v)
    };
    let small = 100_000;
    let big = 12 << 20;
    let cases: Vec<(&str, usize, Bytes)> = {
        let mut cases = Vec::new();
        for len in [small, big] {
            let good = env
                .encode(&x_loc, &CallerContext::new(), Bytes::from(vec![3u8; len]))
                .await
                .expect("encode");
            cases.push(("last tag byte", len, flip(&good, good.len() - 1)));
            cases.push(("truncated", len, good.slice(..good.len() - 100)));
            cases.push(("dropped final frame", len, good.slice(..good.len() - 60)));
            cases.push(("trailing byte", len, [&good[..], &[0]].concat().into()));
            let unbound_msg = unbound
                .encode(&x_loc, &CallerContext::new(), Bytes::from(vec![3u8; len]))
                .await
                .expect("encode");
            cases.push(("unbound", len, unbound_msg));
            let other = env
                .encode(&y_loc, &CallerContext::new(), Bytes::from(vec![3u8; len]))
                .await
                .expect("encode");
            cases.push(("bound to another name", len, other));
            if len == small {
                let hl = header_len(&good);
                cases.push(("header tag", len, flip(&good, hl - 1)));
                cases.push(("first frame", len, flip(&good, hl + 20)));
                cases.push(("not OSC", len, Bytes::from(vec![0u8; len])));
            }
        }
        cases
    };
    for (what, len, stored) in cases {
        for multipart in [false, true] {
            for delete in [false, true] {
                let (a, b) = (
                    Arc::new(TestStore::default()),
                    Arc::new(TestStore::default()),
                );
                let (x, y) = (
                    resolved("h", "x", a.clone(), multipart),
                    resolved("h", "y", b.clone(), multipart),
                );
                a.inner
                    .put(&x.loc.path, stored.clone().into())
                    .await
                    .expect("put");
                assert!(
                    env.copy(&x, &y, delete).await.is_err(),
                    "{what} {len} multipart {multipart} delete {delete}"
                );
                assert!(
                    b.inner.head(&y.loc.path).await.is_err(),
                    "{what}: committed"
                );
                assert!(
                    a.inner.head(&x.loc.path).await.is_ok(),
                    "{what}: source gone"
                );
                if multipart && len == big && what == "last tag byte" {
                    // The failure came after the upload had started.
                    assert_eq!(b.multiparts.load(Ordering::SeqCst), 1);
                }
            }
        }
    }
}

/// The parsed header of a complete stored object, and its length.
fn header_of(env: &Envelope, stored: &[u8]) -> (Header, usize) {
    match HeaderCodec::parse(stored, &env.cfg.limits).expect("parse") {
        HeaderParse::Done {
            header, header_len, ..
        } => (header, header_len),
        HeaderParse::NeedMore => panic!("complete header"),
    }
}

async fn stored_at(store: &InMemory, name: &str) -> Bytes {
    store
        .get(&ObjectPath::from(name))
        .await
        .expect("get")
        .bytes()
        .await
        .expect("bytes")
}

#[tokio::test]
async fn decode_fails_when_the_header_differs_from_the_one_probed() {
    let (env, _, _, _) = fixture();
    let store = Arc::new(InMemory::new());
    let x = resolved("a", "x", store.clone(), false);
    put_at(&env, &x, &x, b"data").await;
    let other = CallerContext::from([("tenant".to_owned(), "evil".to_owned())]);
    let forged = env
        .encode(&x.loc, &other, Bytes::from_static(b"data"))
        .await
        .expect("encode");
    let probed = env
        .read_header(x.backend.as_ref(), &x.loc)
        .await
        .expect("header")
        .header;

    let raw = x.backend.store().get(&x.loc.path).await.expect("get");
    let mut ok = plain_expecting(&env, &x.loc, raw.into_stream(), Some(probed.clone()));
    while let Some(frame) = ok.next().await {
        frame.expect("same header");
    }
    let forged_stream = stream::once(async move { Ok(forged) }).boxed();
    let mut bad = plain_expecting(&env, &x.loc, forged_stream, Some(probed));
    let err = bad.next().await.expect("item").expect_err("changed");
    assert!(matches!(err, Error::Changed), "{err:?}");
}

#[tokio::test]
async fn expected_header_is_compared_once_whatever_the_chunking() {
    let (env, _, _, _) = fixture();
    let store = Arc::new(InMemory::new());
    let x = resolved("a", "x", store.clone(), false);
    let ctx = CallerContext::from([("big".to_owned(), "x".repeat(300))]);
    let bytes = env
        .encode(&x.loc, &ctx, Bytes::from_static(b"data"))
        .await
        .expect("encode");
    let (right, _) = header_of(&env, &bytes);
    let other = env
        .encode(&x.loc, &ctx, Bytes::from_static(b"data"))
        .await
        .expect("encode");

    let one_byte = |b: &Bytes| {
        let chunks: Vec<_> = b.iter().map(|&c| Ok(Bytes::from(vec![c]))).collect();
        stream::iter(chunks).boxed()
    };
    let mut ok = plain_expecting(&env, &x.loc, one_byte(&bytes), Some(right.clone()));
    let mut out = Vec::new();
    while let Some(frame) = ok.next().await {
        out.extend_from_slice(&frame.expect("same header"));
    }
    assert_eq!(out, b"data");

    let mut bad = plain_expecting(&env, &x.loc, one_byte(&other), Some(right));
    let mut failed = false;
    while let Some(frame) = bad.next().await {
        failed |= frame.is_err();
    }
    assert!(failed);
}

/// A streamed `GetResult` of `head` followed by `garbage_chunks` chunks of
/// `GARBAGE_CHUNK` bytes, produced lazily and counted in `pulled`.
fn tailed_result(head: Bytes, garbage_chunks: usize, pulled: &Arc<AtomicUsize>) -> GetResult {
    const GARBAGE_CHUNK: usize = 64 * 1024;
    let counter = pulled.clone();
    let tail = stream::iter(0..garbage_chunks).map(move |_| {
        counter.fetch_add(GARBAGE_CHUNK, Ordering::SeqCst);
        Ok(Bytes::from(vec![0xA5; GARBAGE_CHUNK]))
    });
    let counter = pulled.clone();
    let head_len = head.len();
    let first = stream::once(async move {
        counter.fetch_add(head_len, Ordering::SeqCst);
        Ok(head)
    });
    let size = (head_len + garbage_chunks * GARBAGE_CHUNK) as u64;
    GetResult {
        payload: GetResultPayload::Stream(first.chain(tail).boxed()),
        meta: ObjectMeta {
            location: ObjectPath::from("obj"),
            last_modified: chrono::DateTime::UNIX_EPOCH,
            size,
            e_tag: None,
            version: None,
        },
        range: 0..size,
        attributes: object_store::Attributes::new(),
        extensions: http::Extensions::new(),
    }
}

#[tokio::test]
async fn get_bytes_stops_reading_a_tampered_object_early() {
    const GARBAGE_CHUNK: usize = 64 * 1024;
    let (env, _, _, loc) = fixture();
    let frame = env.cfg.frame_length as usize;
    let stride = frame + 32;
    // Two data frames and a final one, so the first frame is genuine and
    // non-final.
    let stored = env
        .encode(
            &loc,
            &CallerContext::new(),
            Bytes::from(vec![7; 2 * frame + 5]),
        )
        .await
        .expect("encode");
    let header_len = header_of(&env, &stored).1;
    // Garbage in place of frame 2: rejected once its sequence number is
    // seen, long before 64 MiB were pulled.
    let pulled = Arc::new(AtomicUsize::new(0));
    let result = tailed_result(stored.slice(..header_len + stride), 1024, &pulled);
    let e = env.get_bytes(&loc, result).await.expect_err("tampered");
    assert!(
        matches!(
            e,
            Error::Decryption(DecryptError::FrameAuth { .. } | DecryptError::BadSequence { .. })
        ),
        "{e:?}"
    );
    assert!(pulled.load(Ordering::SeqCst) <= header_len + stride + GARBAGE_CHUNK);

    // Garbage after a complete genuine message.
    let pulled = Arc::new(AtomicUsize::new(0));
    let result = tailed_result(stored.clone(), 1024, &pulled);
    let e = env.get_bytes(&loc, result).await.expect_err("trailing");
    assert!(
        matches!(e, Error::Decryption(DecryptError::TrailingBytes)),
        "{e:?}"
    );
    assert!(pulled.load(Ordering::SeqCst) <= stored.len() + GARBAGE_CHUNK);
}

#[tokio::test]
async fn get_bytes_decrypts_a_single_oversized_chunk() {
    let (env, _, _, loc) = fixture();
    let plain = Bytes::from(vec![0x5a; 5 * SLICE / 2]);
    let stored = env
        .encode(&loc, &CallerContext::new(), plain.clone())
        .await
        .expect("encode");
    let size = stored.len() as u64;
    let result = GetResult {
        payload: GetResultPayload::Stream(stream::iter(vec![Ok(stored)]).boxed()),
        meta: ObjectMeta {
            location: ObjectPath::from("obj"),
            last_modified: chrono::DateTime::UNIX_EPOCH,
            size,
            e_tag: None,
            version: None,
        },
        range: 0..size,
        attributes: object_store::Attributes::new(),
        extensions: http::Extensions::new(),
    };
    assert_eq!(env.get_bytes(&loc, result).await.expect("decrypt"), plain);
}

#[tokio::test]
async fn an_oversized_stored_chunk_is_fed_in_slices() {
    let (env, _, _, loc) = fixture();
    let plain = Bytes::from(vec![0x5a; 5 * SLICE / 2]);
    let stored = env
        .encode(&loc, &CallerContext::new(), plain.clone())
        .await
        .expect("encode");
    let one_chunk = |b: &Bytes| stream::iter(vec![Ok(b.clone())]).boxed();

    let mut got = BytesMut::new();
    let mut dec = plain_expecting(&env, &loc, one_chunk(&stored), None);
    while let Some(frame) = dec.next().await {
        got.extend_from_slice(&frame.expect("decrypt"));
    }
    assert_eq!(got, plain);
}

#[tokio::test]
async fn out_of_range_frame_length_fails_before_any_wrap() {
    let (_, _, _, loc) = fixture();
    let provider = Keyring::from_material("kek-1", &[("kek-1", &[7; 32])]);
    // Limits set after the frame length leave an inconsistent pair.
    let cfg = EncryptionConfig::new(Arc::new(provider))
        .frame_length(65536)
        .expect("in range for the default limits")
        .limits(DecodeLimits {
            max_frame_length: 4096,
            ..DecodeLimits::default()
        });
    let env = Envelope::new(cfg);
    let r = env
        .encode(&loc, &CallerContext::new(), Bytes::from_static(b"x"))
        .await;
    assert!(matches!(r, Err(Error::InvalidInput(_))));
    let mut s = env.encode_stream(loc, CallerContext::new(), stream::empty().boxed());
    assert!(matches!(s.next().await, Some(Err(Error::InvalidInput(_)))));
}

#[test]
fn not_encrypted_errors_carry_no_credentials() {
    let url = Url::parse("s3://user:secret@bucket/obj?sig=tok#frag").expect("url");
    let e = read_err(&url, DecryptError::Format(FormatError::NotOsc));
    let Error::NotEncrypted { url } = e else {
        panic!("expected NotEncrypted");
    };
    assert_eq!(url, "s3://bucket/obj");
}

#[tokio::test]
async fn cross_store_copy_of_a_source_that_fails_verification_commits_nothing() {
    let (env, _, _, _) = fixture();
    let dst_store = Arc::new(InMemory::new());
    let dst = resolved("dst", "obj", dst_store.clone(), false);
    let genuine = env
        .encode(
            &resolved("src", "obj", Arc::new(InMemory::new()), false).loc,
            &CallerContext::new(),
            Bytes::from(vec![5u8; 200_000]),
        )
        .await
        .expect("encode");
    let other = env
        .encode(
            &resolved("src", "other", Arc::new(InMemory::new()), false).loc,
            &CallerContext::new(),
            Bytes::from(vec![5u8; 200_000]),
        )
        .await
        .expect("encode");
    let fakes = [
        ("bound to another name", other),
        ("not an OSC message", Bytes::from(vec![0u8; 1000])),
        ("truncated", genuine.slice(..genuine.len() - 100)),
    ];
    for (what, fake) in fakes {
        for delete in [false, true] {
            let src_store = Arc::new(TestStore {
                full: Some(InMemory::new()),
                ..TestStore::default()
            });
            let path = ObjectPath::from("obj");
            src_store
                .inner
                .put(&path, genuine.clone().into())
                .await
                .expect("put");
            src_store
                .full
                .as_ref()
                .expect("full")
                .put(&path, fake.clone().into())
                .await
                .expect("put");
            let src = resolved("src", "obj", src_store.clone(), false);

            let result = env.copy(&src, &dst, delete).await;
            assert!(result.is_err(), "{what} (delete {delete})");
            assert!(dst_store.head(&path).await.is_err(), "{what}");
            assert!(src_store.inner.head(&path).await.is_ok(), "{what}");
        }
    }

    // The same layout with a genuine full read still copies.
    let src_store = Arc::new(TestStore {
        full: Some(InMemory::new()),
        ..TestStore::default()
    });
    let path = ObjectPath::from("obj");
    for half in [&src_store.inner, src_store.full.as_ref().expect("full")] {
        half.put(&path, genuine.clone().into()).await.expect("put");
    }
    let src = resolved("src", "obj", src_store.clone(), false);
    env.copy(&src, &dst, true).await.expect("genuine copy");
    let got = dst.backend.store().get(&path).await.expect("get");
    let plain = env.get_bytes(&dst.loc, got).await.expect("decrypt");
    assert_eq!(plain.as_ref(), vec![5u8; 200_000].as_slice());
}

#[tokio::test]
async fn copy_or_move_onto_itself_does_nothing_whatever_the_key() {
    let (old, active) = rotated();
    let store = Arc::new(TestStore::default());
    let at = |scheme: &str, name: &str| Resolved {
        backend: Arc::new(TestBackend {
            store: store.clone(),
            multipart: false,
        }),
        loc: Location {
            url: Url::parse(&format!("{scheme}://bucket/{name}")).expect("url"),
            path: ObjectPath::from(name),
        },
    };
    // One object under the old key, one under the active key.
    for (env, name) in [(&old, "old-key"), (&active, "active-key")] {
        let loc = at("gs", name).loc;
        let bytes = env
            .encode(&loc, &CallerContext::new(), Bytes::from_static(b"data"))
            .await
            .expect("encode");
        store.inner.put(&loc.path, bytes.into()).await.expect("put");
    }
    let stored = |name: &str| {
        let store = store.clone();
        let path = ObjectPath::from(name);
        async move {
            store
                .inner
                .get(&path)
                .await
                .expect("get")
                .bytes()
                .await
                .expect("bytes")
        }
    };
    for name in ["old-key", "active-key"] {
        let before = stored(name).await;
        for delete_source in [false, true] {
            // The same URL, and a `gcs://` alias of it.
            for alias in ["gs", "gcs"] {
                active
                    .copy(&at("gs", name), &at(alias, name), delete_source)
                    .await
                    .expect("onto itself");
                assert_eq!(stored(name).await, before, "{name} {alias} {delete_source}");
            }
        }
    }
    assert!(store.requested().is_empty(), "{:?}", store.requested());
    let info = active
        .inspect(
            at("gs", "old-key").backend.as_ref(),
            &at("gs", "old-key").loc,
        )
        .await
        .expect("inspect");
    assert_eq!(info.key_names, ["kek-1"]);
}

#[tokio::test]
async fn describing_a_missing_object_is_not_found() {
    let (env, _, backend, loc) = fixture();
    assert!(matches!(
        env.describe(&backend, &loc).await,
        Err(Error::ObjectStore(object_store::Error::NotFound { .. }))
    ));
}

#[tokio::test]
async fn a_failed_header_read_is_the_stores_error_not_a_plain_object() {
    let (env, _, _, _) = fixture();
    let store = Arc::new(TestStore {
        fail_ranged_reads: true,
        ..TestStore::default()
    });
    let x = resolved("same", "x", store.clone(), false);
    store
        .put(&x.loc.path, Bytes::from_static(b"not empty").into())
        .await
        .expect("put");
    let err = env.describe(x.backend.as_ref(), &x.loc).await;
    assert!(
        matches!(
            &err,
            Err(Error::ObjectStore(object_store::Error::Generic { .. }))
        ),
        "{err:?}"
    );
}

#[tokio::test]
async fn move_onto_itself_keeps_an_active_key_object() {
    let (env, _, _, _) = fixture();
    let store = Arc::new(InMemory::new());
    let x = resolved("a", "x", store.clone(), false);
    put_at(&env, &x, &x, b"data").await;
    let before = stored_at(&store, "x").await;

    env.copy(&x, &x, true).await.expect("move onto itself");
    assert_eq!(stored_at(&store, "x").await, before);
}

/// The encrypted client never asks the store to copy or rename: a store
/// whose `copy` fails, with whatever error, does not matter.
#[tokio::test]
async fn copy_never_uses_the_stores_copy() {
    let provider = Keyring::from_material("kek-1", &[("kek-1", &[7; 32])]);
    let env = Envelope::new(EncryptionConfig::new(Arc::new(provider)).bind_object_key(false));
    // `same_object` stats the paths of two file URLs, so they live in a
    // private tempdir; the objects themselves are in memory.
    let tmp = tempfile::tempdir().expect("tempdir");
    let root = tmp.path().display().to_string();
    for (scheme, error) in [
        ("file", std::io::ErrorKind::CrossesDevices),
        ("file", std::io::ErrorKind::PermissionDenied),
        ("test", std::io::ErrorKind::CrossesDevices),
    ] {
        let store = Arc::new(TestStore {
            copy_error: Some(error),
            ..TestStore::default()
        });
        let backend: Arc<dyn Backend> = Arc::new(TestBackend {
            store: store.clone(),
            multipart: false,
        });
        let at = |name: &str| {
            let url = Url::parse(&format!("{scheme}://{root}/{name}")).expect("url");
            let path = ObjectPath::from_url_path(url.path()).expect("path");
            Resolved {
                backend: Arc::clone(&backend),
                loc: Location { url, path },
            }
        };
        let (x, y) = (at("x"), at("y"));
        assert!(
            !same_object(&x.loc, &y.loc).await,
            "{scheme}: x and y differ"
        );
        put(&env, &store, &x.loc, &CallerContext::new()).await;

        env.copy(&x, &y, true).await.expect("copy");
        assert_eq!(store.copies.load(Ordering::SeqCst), 0, "{scheme} {error}");
        let got = store.get(&y.loc.path).await.expect("copied");
        let plain = env.get_bytes(&y.loc, got).await.expect("decrypt");
        assert_eq!(plain.as_ref(), b"payload");
        assert!(store.head(&x.loc.path).await.is_err());
    }
}

/// Copying an active-key object onto itself writes nothing.
#[tokio::test]
async fn copy_onto_itself_leaves_an_active_key_object_untouched() {
    let (env, _, _, _) = fixture();
    let store = Arc::new(InMemory::new());
    let x = resolved("a", "x", store.clone(), false);
    put_at(&env, &x, &x, b"data").await;
    let before = store.head(&x.loc.path).await.expect("head");

    env.copy(&x, &x, false).await.expect("copy onto itself");
    assert_eq!(store.head(&x.loc.path).await.expect("head"), before);
}

#[derive(Debug)]
struct BrokenProvider;

#[async_trait]
impl KeyProvider for BrokenProvider {
    #[expect(clippy::unnecessary_literal_bound, reason = "trait signature")]
    fn namespace(&self) -> &str {
        "osc-file"
    }

    fn active_key_name(&self) -> String {
        "broken".to_owned()
    }

    async fn wrap(
        &self,
        _dek: &DataKey,
        _ctx: &EncryptionContext,
    ) -> std::result::Result<Vec<EncryptedDataKey>, KeyError> {
        Err(KeyError::Rng)
    }

    async fn unwrap(
        &self,
        _edks: &[EncryptedDataKey],
        _ctx: &EncryptionContext,
    ) -> std::result::Result<DataKey, KeyError> {
        Err(KeyError::Rng)
    }
}

#[tokio::test]
async fn a_large_encrypted_put_streams_to_a_multipart_upload_and_round_trips() {
    let provider = Keyring::from_material("kek-1", &[("kek-1", &[7; 32])]);
    let client = crate::ObjectStorageClient::with_vars(std::collections::HashMap::new())
        .with_encryption(EncryptionConfig::new(Arc::new(provider)));
    let store = Arc::new(TestStore::default());
    client.preload_store("s3://bucket/", store.clone());

    let data: Bytes = (0..9 * 1024 * 1024_usize)
        .map(|i| u8::try_from(i % 251).expect("byte"))
        .collect::<Vec<u8>>()
        .into();
    client
        .put("s3://bucket/big", data.clone())
        .await
        .expect("put");
    assert_eq!(store.multiparts.load(Ordering::SeqCst), 1);
    assert_eq!(client.get("s3://bucket/big").await.expect("get"), data);

    let failing = Arc::new(TestStore {
        fail_puts: true,
        ..TestStore::default()
    });
    client.preload_store("s3://broken/", failing.clone());
    client
        .put("s3://broken/big", data)
        .await
        .expect_err("the refused upload fails the put");
    assert!(failing.inner.list(None).next().await.is_none());
}

#[tokio::test]
async fn a_provider_that_cannot_wrap_fails_the_encrypting_stream() {
    let env = Envelope::new(EncryptionConfig::with_provider(Arc::new(BrokenProvider)));
    let (_, _, _, loc) = fixture();
    let plain = stream::iter([Ok(Bytes::from_static(b"data"))]).boxed();
    let mut s = env.encode_stream(loc, CallerContext::new(), plain);
    assert!(matches!(
        s.next().await,
        Some(Err(Error::Key(KeyError::Rng)))
    ));
    assert!(s.next().await.is_none(), "nothing follows the error");
}

#[tokio::test]
async fn a_failing_source_stream_fails_the_decrypting_stream() {
    let (env, _, _, loc) = fixture();
    let full = env
        .encode(&loc, &CallerContext::new(), Bytes::from_static(b"payload"))
        .await
        .expect("encode");
    let stored = stream::iter([
        Ok(full.slice(..10)),
        Err(object_store::Error::Generic {
            store: "faulty",
            source: "connection reset".into(),
        }),
    ])
    .boxed();
    let mut s = env.decode(loc, stored);
    assert!(matches!(
        s.next().await,
        Some(Err(Error::ObjectStore(object_store::Error::Generic { .. })))
    ));
    assert!(s.next().await.is_none());
}

// --- reencrypt ---

/// What a [`Meddle`] store does, once, when its trigger is reached.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum When {
    /// Before the first unranged, non-HEAD read (the pinned body read).
    Get,
    /// Before the first HEAD (the commit-time re-check).
    Head,
    /// Before the first conditional put.
    Put,
}

/// An `InMemory` wrapper that, once, replaces (or with `newer: None`
/// deletes) the object it holds at the moment of `when`, as a concurrent
/// writer would, and counts multipart aborts.
#[derive(Debug)]
struct Meddle {
    inner: Arc<dyn ObjectStore>,
    when: When,
    newer: Option<Bytes>,
    fired: std::sync::atomic::AtomicBool,
    aborts: Arc<AtomicUsize>,
}

impl fmt::Display for Meddle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Meddle")
    }
}

#[derive(Debug)]
struct CountAborts(Box<dyn MultipartUpload>, Arc<AtomicUsize>);

#[async_trait]
impl MultipartUpload for CountAborts {
    fn put_part(&mut self, data: PutPayload) -> object_store::UploadPart {
        self.0.put_part(data)
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        self.0.complete().await
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        self.1.fetch_add(1, Ordering::SeqCst);
        self.0.abort().await
    }
}

impl Meddle {
    async fn at(&self, now: When, location: &ObjectPath) {
        if now == self.when && !self.fired.swap(true, Ordering::SeqCst) {
            match &self.newer {
                Some(bytes) => {
                    self.inner
                        .put(location, bytes.clone().into())
                        .await
                        .expect("meddle");
                }
                None => self.inner.delete(location).await.expect("meddle"),
            }
        }
    }
}

forward_object_store! {
    impl ObjectStore for Meddle => inner;
    forward [delete_stream, list, list_with_delimiter, copy_opts];
    {
        async fn put_opts(
            &self,
            location: &ObjectPath,
            payload: PutPayload,
            opts: PutOptions,
        ) -> object_store::Result<PutResult> {
            if matches!(opts.mode, object_store::PutMode::Update(_)) {
                self.at(When::Put, location).await;
            }
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &ObjectPath,
            opts: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            let upload = self.inner.put_multipart_opts(location, opts).await?;
            Ok(Box::new(CountAborts(upload, Arc::clone(&self.aborts))))
        }

        async fn get_opts(
            &self,
            location: &ObjectPath,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            if options.head {
                self.at(When::Head, location).await;
            } else if options.range.is_none() {
                self.at(When::Get, location).await;
            }
            self.inner.get_opts(location, options).await
        }
    }
}

/// `kek-1` is the old key, `kek-2` the active one.
fn rotated() -> (Envelope, Envelope) {
    rotated_binding(true, true)
}

fn from_kek1(before: Option<chrono::DateTime<chrono::Utc>>, dry_run: bool) -> ReencryptOptions {
    ReencryptOptions {
        from: "kek-1".to_owned(),
        before,
        dry_run,
    }
}

async fn seal_with(env: &Envelope, at: &Resolved, plain: &[u8]) -> Bytes {
    let ctx = CallerContext::from([("tenant".to_owned(), "acme".to_owned())]);
    let bytes = env
        .encode(&at.loc, &ctx, Bytes::copy_from_slice(plain))
        .await
        .expect("encode");
    at.backend
        .store()
        .put(&at.loc.path, bytes.clone().into())
        .await
        .expect("put");
    bytes
}

async fn read_back(env: &Envelope, at: &Resolved) -> Bytes {
    let got = at.backend.store().get(&at.loc.path).await.expect("get");
    env.get_bytes(&at.loc, got).await.expect("decrypt")
}

async fn raw(at: &Resolved) -> Bytes {
    at.backend
        .store()
        .get(&at.loc.path)
        .await
        .expect("get")
        .bytes()
        .await
        .expect("bytes")
}

#[tokio::test]
async fn reencrypt_moves_an_object_to_the_active_key_and_is_then_skipped() {
    let (old, env) = rotated();
    let store = Arc::new(InMemory::new());
    let at = resolved("bucket", "dir/obj", store, false);
    let before = seal_with(&old, &at, b"payload").await;

    let opts = from_kek1(None, false);
    assert_eq!(
        env.reencrypt(&at, &opts).await.expect("reencrypt"),
        Reencrypted::Rewritten
    );
    let after = raw(&at).await;
    assert_ne!(before, after);
    let info = env
        .inspect(at.backend.as_ref(), &at.loc)
        .await
        .expect("info");
    assert_eq!(info.key_names, ["kek-2"]);
    assert_eq!(info.context["tenant"], "acme");
    assert_eq!(info.context["osc:key"], "dir/obj");
    assert_eq!(read_back(&env, &at).await.as_ref(), b"payload");

    assert_eq!(
        env.reencrypt(&at, &opts).await.expect("again"),
        Reencrypted::Skipped
    );
    assert_eq!(raw(&at).await, after);
}

/// [`rotated`] with the binding chosen per side.
fn rotated_binding(old_bound: bool, new_bound: bool) -> (Envelope, Envelope) {
    let old = Keyring::from_material("kek-1", &[("kek-1", &[7; 32])]);
    let new = Keyring::from_material("kek-2", &[("kek-2", &[9; 32]), ("kek-1", &[7; 32])]);
    (
        Envelope::new(EncryptionConfig::new(Arc::new(old)).bind_object_key(old_bound)),
        Envelope::new(EncryptionConfig::new(Arc::new(new)).bind_object_key(new_bound)),
    )
}

#[tokio::test]
async fn reencrypt_on_an_unbound_client_keeps_a_bound_objects_binding() {
    let (old, new) = rotated_binding(true, false);
    let at = resolved("bucket", "dir/obj", Arc::new(InMemory::new()), false);
    seal_with(&old, &at, b"payload").await;

    let r = new.reencrypt(&at, &from_kek1(None, false)).await;
    assert_eq!(r.expect("reencrypt"), Reencrypted::Rewritten);
    let info = new
        .inspect(at.backend.as_ref(), &at.loc)
        .await
        .expect("info");
    assert_eq!(info.key_names, ["kek-2"]);
    assert_eq!(info.context["osc:key"], "dir/obj");
    // A bound reader still opens it.
    let (_, bound) = rotated_binding(true, true);
    assert_eq!(read_back(&bound, &at).await.as_ref(), b"payload");
}

#[tokio::test]
async fn reencrypt_on_a_binding_client_keeps_an_unbound_object_unbound() {
    let (old, new) = rotated_binding(false, true);
    let at = resolved("bucket", "dir/obj", Arc::new(InMemory::new()), false);
    seal_with(&old, &at, b"payload").await;

    let dry = new.reencrypt(&at, &from_kek1(None, true)).await;
    assert_eq!(dry.expect("dry run"), Reencrypted::WouldRewrite);
    let real = new.reencrypt(&at, &from_kek1(None, false)).await;
    assert_eq!(real.expect("reencrypt"), Reencrypted::Rewritten);
    let info = new
        .inspect(at.backend.as_ref(), &at.loc)
        .await
        .expect("info");
    assert_eq!(info.key_names, ["kek-2"]);
    assert!(!info.context.contains_key("osc:key"), "{:?}", info.context);
    assert_eq!(info.context["tenant"], "acme");
    let (_, unbound) = rotated_binding(false, false);
    assert_eq!(read_back(&unbound, &at).await.as_ref(), b"payload");
}

#[tokio::test]
async fn reencrypt_of_an_object_bound_to_another_name() {
    let (old, new_bound) = rotated_binding(true, true);
    let (_, new_unbound) = rotated_binding(true, false);
    let at = resolved("bucket", "dir/obj", Arc::new(InMemory::new()), false);
    let other = Location {
        url: Url::parse("test://bucket/other").expect("url"),
        path: ObjectPath::from("other"),
    };
    let bytes = old
        .encode(
            &other,
            &CallerContext::new(),
            Bytes::from_static(b"payload"),
        )
        .await
        .expect("encode");
    at.backend
        .store()
        .put(&at.loc.path, bytes.clone().into())
        .await
        .expect("put");

    for dry_run in [true, false] {
        let r = new_bound.reencrypt(&at, &from_kek1(None, dry_run)).await;
        assert!(
            matches!(
                r,
                Err(Error::Decryption(DecryptError::BindingMismatch { .. }))
            ),
            "dry_run {dry_run}: {r:?}"
        );
        assert_eq!(raw(&at).await, bytes);
    }

    let r = new_unbound.reencrypt(&at, &from_kek1(None, false)).await;
    assert_eq!(r.expect("reencrypt"), Reencrypted::Rewritten);
    let info = new_unbound
        .inspect(at.backend.as_ref(), &at.loc)
        .await
        .expect("info");
    assert_eq!(info.key_names, ["kek-2"]);
    assert_eq!(info.context["osc:key"], "other");
}

#[tokio::test]
async fn reencrypt_skips_other_keys_and_refuses_the_active_one() {
    let (_, env) = rotated();
    let other = Keyring::from_material("kek-3", &[("kek-3", &[3; 32])]);
    let other = Envelope::new(EncryptionConfig::new(Arc::new(other)));
    let store = Arc::new(TestStore::default());
    let at = resolved("bucket", "dir/obj", store.clone(), false);
    let bytes = seal_with(&other, &at, b"payload").await;
    let calls = store.requested().len();

    let skipped = env.reencrypt(&at, &from_kek1(None, false)).await;
    assert_eq!(skipped.expect("skip"), Reencrypted::Skipped);
    assert_eq!(raw(&at).await, bytes);

    let active = ReencryptOptions {
        from: "kek-2".to_owned(),
        ..from_kek1(None, false)
    };
    let before = store.requested().len();
    let err = env.reencrypt(&at, &active).await;
    assert!(matches!(err, Err(Error::InvalidInput(_))), "{err:?}");
    // Refused before any I/O.
    assert_eq!(store.requested().len(), before);
    assert!(calls < before);
}

#[tokio::test]
async fn reencrypt_refuses_a_key_the_keyring_does_not_hold_before_any_io() {
    let (_, env) = rotated();
    let store = Arc::new(TestStore::default());
    let at = resolved("bucket", "dir/obj", store.clone(), false);
    let opts = ReencryptOptions {
        from: "kek-typo".to_owned(),
        ..from_kek1(None, false)
    };
    let err = env.reencrypt(&at, &opts).await;
    assert!(
        matches!(&err, Err(Error::Key(KeyError::UnknownId { id })) if id == "kek-typo"),
        "{err:?}"
    );
    assert!(store.requested().is_empty(), "{:?}", store.requested());
}

#[tokio::test]
async fn reencrypt_leaves_plain_and_empty_objects_alone() {
    let (_, env) = rotated();
    let store = Arc::new(InMemory::new());
    let at = resolved("bucket", "dir/obj", store.clone(), false);
    for body in [&b"just some plain text"[..], b""] {
        store
            .put(&at.loc.path, Bytes::copy_from_slice(body).into())
            .await
            .expect("put");
        let got = env.reencrypt(&at, &from_kek1(None, false)).await;
        assert_eq!(got.expect("outcome"), Reencrypted::NotEncrypted);
        assert_eq!(raw(&at).await.as_ref(), body);
    }
}

#[tokio::test]
async fn reencrypt_honours_the_cutoff_and_a_dry_run_writes_nothing() {
    let (old, env) = rotated();
    let store = Arc::new(InMemory::new());
    let at = resolved("bucket", "dir/obj", store, false);
    let bytes = seal_with(&old, &at, b"payload").await;
    let modified = at.backend.store().head(&at.loc.path).await.expect("head");
    let modified = modified.last_modified;
    let hour = chrono::Duration::hours(1);

    // At the cutoff is after it.
    let got = env.reencrypt(&at, &from_kek1(Some(modified), false)).await;
    assert_eq!(got.expect("outcome"), Reencrypted::AfterCutoff);
    let got = env
        .reencrypt(&at, &from_kek1(Some(modified - hour), true))
        .await;
    assert_eq!(got.expect("outcome"), Reencrypted::AfterCutoff);

    let got = env
        .reencrypt(&at, &from_kek1(Some(modified + hour), true))
        .await;
    assert_eq!(got.expect("outcome"), Reencrypted::WouldRewrite);
    assert_eq!(raw(&at).await, bytes);

    let got = env
        .reencrypt(&at, &from_kek1(Some(modified + hour), false))
        .await;
    assert_eq!(got.expect("outcome"), Reencrypted::Rewritten);
}

#[tokio::test]
async fn reencrypt_of_a_tampered_object_fails_and_changes_nothing() {
    let (old, env) = rotated();
    for multipart in [false, true] {
        let store = Arc::new(InMemory::new());
        let at = resolved("bucket", "dir/obj", store, multipart);
        let bytes = seal_with(&old, &at, b"payload").await;
        let mut bad = bytes.to_vec();
        let last = bad.len() - 1;
        bad[last] ^= 1;
        at.backend
            .store()
            .put(&at.loc.path, Bytes::from(bad.clone()).into())
            .await
            .expect("put");

        let got = env.reencrypt(&at, &from_kek1(None, false)).await;
        assert!(matches!(got, Err(Error::Decryption(_))), "{got:?}");
        assert_eq!(raw(&at).await.as_ref(), bad.as_slice());
    }
}

#[tokio::test]
async fn reencrypt_of_a_corrupt_header_is_an_error_not_a_plain_object() {
    let (old, env) = rotated();
    let store = Arc::new(InMemory::new());
    let at = resolved("bucket", "dir/obj", store, false);
    let bytes = seal_with(&old, &at, b"payload").await;
    at.backend
        .store()
        .put(&at.loc.path, bytes.slice(..20).into())
        .await
        .expect("put");
    let got = env.reencrypt(&at, &from_kek1(None, false)).await;
    assert!(
        got.is_err() && !matches!(got, Err(Error::NotEncrypted { .. })),
        "{got:?}"
    );
}

/// An `InMemory` wrapper whose reads leave out the e-tag and version
/// (`strip_version`) and report `last_modified` when it is set.
#[derive(Debug, Default)]
struct Doctored {
    inner: InMemory,
    strip_version: bool,
    last_modified: Option<chrono::DateTime<chrono::Utc>>,
}

impl fmt::Display for Doctored {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Doctored")
    }
}

forward_object_store! {
    impl ObjectStore for Doctored => inner;
    forward [put_opts, put_multipart_opts, delete_stream, list, list_with_delimiter, copy_opts];
    {
        async fn get_opts(
            &self,
            location: &ObjectPath,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            let mut got = self.inner.get_opts(location, options).await?;
            if self.strip_version {
                got.meta.e_tag = None;
                got.meta.version = None;
            }
            if let Some(t) = self.last_modified {
                got.meta.last_modified = t;
            }
            Ok(got)
        }
    }
}

#[tokio::test]
async fn reencrypt_treats_a_missing_modification_time_as_after_the_cutoff() {
    let (old, env) = rotated();
    let at = resolved(
        "bucket",
        "dir/obj",
        Arc::new(Doctored {
            last_modified: Some(chrono::DateTime::UNIX_EPOCH),
            ..Doctored::default()
        }),
        false,
    );
    let bytes = seal_with(&old, &at, b"payload").await;

    let cutoff = Some(chrono::Utc::now());
    let r = env.reencrypt(&at, &from_kek1(cutoff, false)).await;
    assert_eq!(r.expect("reencrypt"), Reencrypted::AfterCutoff);
    assert_eq!(raw(&at).await, bytes);

    let r = env.reencrypt(&at, &from_kek1(None, false)).await;
    assert_eq!(r.expect("reencrypt"), Reencrypted::Rewritten);
}

#[tokio::test]
async fn reencrypt_refuses_an_object_without_an_e_tag_or_version() {
    let (old, env) = rotated();
    let at = resolved(
        "bucket",
        "dir/obj",
        Arc::new(Doctored {
            strip_version: true,
            ..Doctored::default()
        }),
        false,
    );
    let bytes = seal_with(&old, &at, b"payload").await;

    // A dry run decides without writing, so it still answers.
    let dry = env.reencrypt(&at, &from_kek1(None, true)).await;
    assert_eq!(dry.expect("dry run"), Reencrypted::WouldRewrite);
    let got = env.reencrypt(&at, &from_kek1(None, false)).await;
    assert!(
        matches!(&got, Err(Error::Generic(m)) if m.contains("neither an e-tag nor a version")),
        "{got:?}"
    );
    assert_eq!(raw(&at).await, bytes);
}

#[cfg(unix)]
#[tokio::test]
async fn an_alias_rebind_is_abandoned_when_the_file_changed() {
    let (env, _) = rotated();
    let tmp = tempfile::tempdir().expect("dir");
    let real = tmp.path().join("real");
    std::fs::create_dir(&real).expect("dir");
    std::os::unix::fs::symlink(&real, tmp.path().join("link")).expect("symlink");
    let at = |dir: &str| {
        let file = tmp.path().join(dir).join("x");
        Location {
            url: Url::from_file_path(&file).expect("url"),
            path: ObjectPath::from_absolute_path(&file).expect("path"),
        }
    };
    let (via_link, direct) = (at("link"), at("real"));
    let store = Arc::new(Meddle {
        inner: Arc::new(object_store::local::LocalFileSystem::new()),
        when: When::Head,
        newer: Some(Bytes::from_static(b"newer")),
        fired: std::sync::atomic::AtomicBool::new(false),
        aborts: Arc::default(),
    });
    let resolved = |loc: Location| Resolved {
        backend: Arc::new(TestBackend {
            store: store.clone(),
            multipart: false,
        }),
        loc,
    };
    // Encrypted for (bound to) the link name, stored under the real one.
    let bytes = env
        .encode(
            &via_link,
            &CallerContext::new(),
            Bytes::from_static(b"data"),
        )
        .await
        .expect("encode");
    std::fs::write(real.join("x"), &bytes).expect("write");

    let r = env
        .copy(&resolved(via_link), &resolved(direct), false)
        .await;
    assert!(matches!(r, Err(Error::Changed)), "{r:?}");
    assert_eq!(std::fs::read(real.join("x")).expect("read"), b"newer");
}

/// Runs a reencrypt of an object that `when` replaces with `newer` (or
/// deletes), and returns the outcome and the meddling store.
async fn meddled(
    when: When,
    newer: Option<&'static [u8]>,
    len: usize,
    multipart: bool,
) -> (Reencrypted, Resolved, Arc<Meddle>, Bytes) {
    let (old, env) = rotated();
    let store = Arc::new(Meddle {
        inner: Arc::new(InMemory::new()),
        when,
        newer: newer.map(Bytes::from_static),
        fired: std::sync::atomic::AtomicBool::new(false),
        aborts: Arc::default(),
    });
    let at = resolved("bucket", "dir/obj", store.clone(), multipart);
    let plain = vec![5u8; len];
    let bytes = seal_with(&old, &at, &plain).await;
    let got = env
        .reencrypt(&at, &from_kek1(None, false))
        .await
        .expect("reencrypt");
    (got, at, store, bytes)
}

#[tokio::test]
async fn an_object_replaced_at_each_step_of_a_single_put_is_changed() {
    for when in [When::Get, When::Head, When::Put] {
        let (got, at, _, _) = meddled(when, Some(b"newer"), 100, false).await;
        assert_eq!(got, Reencrypted::Changed, "{when:?}");
        assert_eq!(raw(&at).await.as_ref(), b"newer", "{when:?}");
    }
}

#[tokio::test]
async fn an_object_deleted_before_the_commit_is_changed_and_stays_deleted() {
    for when in [When::Get, When::Head] {
        let (got, at, _, _) = meddled(when, None, 100, false).await;
        assert_eq!(got, Reencrypted::Changed, "{when:?}");
        let head = at.backend.store().head(&at.loc.path).await;
        assert!(
            matches!(head, Err(object_store::Error::NotFound { .. })),
            "{when:?}"
        );
    }
}

#[tokio::test]
async fn a_multipart_object_replaced_before_completion_is_changed_and_aborted() {
    let (got, at, store, _) = meddled(When::Head, Some(b"newer"), 9 << 20, true).await;
    assert_eq!(got, Reencrypted::Changed);
    assert_eq!(raw(&at).await.as_ref(), b"newer");
    assert_eq!(store.aborts.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn an_unmolested_multipart_object_is_rewritten() {
    // The trigger never fires: the first HEAD happens at the commit, and
    // `Put` is only reached by a single put.
    let (got, at, store, _) = meddled(When::Put, Some(b"newer"), 9 << 20, true).await;
    assert_eq!(got, Reencrypted::Rewritten);
    assert_eq!(store.aborts.load(Ordering::SeqCst), 0);
    let (_, env) = rotated();
    assert_eq!(read_back(&env, &at).await.len(), 9 << 20);
}
