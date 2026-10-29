//! A loopback HTTP stub shared by the backend unit tests.
//!
//! [`serve`] binds `127.0.0.1:0` and answers the next connections, one canned
//! [`Reply`] each, in order. Every request is read completely (headers and the
//! `Content-Length` body) before the reply is written, so the client never sees
//! a connection reset, and is recorded for the test to inspect.

use std::fmt::Write as _;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// A canned response.
pub struct Reply {
    status: u16,
    headers: Vec<(&'static str, &'static str)>,
    body: String,
}

impl Reply {
    pub fn new(status: u16, body: impl Into<String>) -> Self {
        Self {
            status,
            headers: Vec::new(),
            body: body.into(),
        }
    }

    pub fn header(mut self, name: &'static str, value: &'static str) -> Self {
        self.headers.push((name, value));
        self
    }
}

/// A request as the stub received it.
#[derive(Clone)]
pub struct Request {
    /// The request line, for example `PUT /bkt HTTP/1.1`.
    pub line: String,
    /// The header block after the request line, one `name: value` per line.
    pub headers: String,
    pub body: Vec<u8>,
}

/// A running stub.
pub struct Stub {
    /// `http://127.0.0.1:<port>`, without a trailing slash.
    pub base: String,
    requests: Arc<Mutex<Vec<Request>>>,
}

impl Stub {
    /// The requests received so far.
    pub fn requests(&self) -> Vec<Request> {
        self.requests.lock().expect("lock").clone()
    }
}

/// Starts a stub that answers one connection per entry of `replies`.
pub async fn serve(replies: Vec<Reply>) -> Stub {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let base = format!("http://{}", listener.local_addr().expect("addr"));
    let requests = Arc::new(Mutex::new(Vec::new()));
    let seen = Arc::clone(&requests);
    tokio::spawn(async move {
        for reply in replies {
            let (mut conn, _) = listener.accept().await.expect("accept");
            let request = read_request(&mut conn).await;
            let head_only = request.line.starts_with("HEAD ");
            seen.lock().expect("lock").push(request);

            let mut head = format!(
                "HTTP/1.1 {} Stub\r\nConnection: close\r\nContent-Length: {}\r\n",
                reply.status,
                reply.body.len()
            );
            for (name, value) in reply.headers {
                let _ = write!(head, "{name}: {value}\r\n");
            }
            head.push_str("\r\n");
            let _ = conn.write_all(head.as_bytes()).await;
            if !head_only {
                let _ = conn.write_all(reply.body.as_bytes()).await;
            }
            let _ = conn.shutdown().await;
        }
    });
    Stub { base, requests }
}

async fn read_request(conn: &mut tokio::net::TcpStream) -> Request {
    let mut buf = Vec::new();
    let mut chunk = [0_u8; 1024];
    let split = loop {
        if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break i + 4;
        }
        let n = conn.read(&mut chunk).await.expect("read request head");
        assert!(n > 0, "connection closed before the request head ended");
        buf.extend_from_slice(&chunk[..n]);
    };
    let head = String::from_utf8_lossy(&buf[..split]).into_owned();
    let length = head
        .lines()
        .find_map(|l| {
            let (name, value) = l.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().ok())?
        })
        .unwrap_or(0);
    while buf.len() < split + length {
        let n = conn.read(&mut chunk).await.expect("read request body");
        assert!(n > 0, "connection closed before the request body ended");
        buf.extend_from_slice(&chunk[..n]);
    }
    let (line, headers) = head.split_once("\r\n").unwrap_or((&head, ""));
    Request {
        line: line.to_owned(),
        headers: headers.trim_end().to_owned(),
        body: buf[split..split + length].to_vec(),
    }
}

/// The bytes [`serve_slow`] sends per chunk.
pub const CHUNK: usize = 1024;

/// Starts a stub that answers its first connection with `200`, a
/// `Last-Modified` header (the stores require one) and no `ETag`,
/// declaring `declared` chunks of [`CHUNK`] bytes. It sends `sent` of them,
/// waiting `gap` before each, then goes silent with the connection open.
/// Returns `http://127.0.0.1:<port>`.
pub async fn serve_slow(declared: usize, sent: usize, gap: Duration) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let base = format!("http://{}", listener.local_addr().expect("addr"));
    tokio::spawn(async move {
        let (mut conn, _) = listener.accept().await.expect("accept");
        read_request(&mut conn).await;
        let head = format!(
            "HTTP/1.1 200 Stub\r\nLast-Modified: Wed, 21 Oct 2015 07:28:00 GMT\r\n\
             Content-Length: {}\r\n\r\n",
            declared * CHUNK
        );
        conn.write_all(head.as_bytes()).await.expect("head");
        for _ in 0..sent {
            tokio::time::sleep(gap).await;
            conn.write_all(&[b'x'; CHUNK]).await.expect("chunk");
        }
        std::future::pending::<()>().await;
    });
    base
}
