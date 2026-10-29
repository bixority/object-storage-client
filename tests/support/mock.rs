//! A recording loopback HTTP server for tests that run the `osc` binary
//! against stand-ins for AWS services (SSO portal, STS, S3, an IMDS canary).
//!
//! It binds `127.0.0.1:0`, reads each request's head and `Content-Length`
//! body, records it, and answers with whatever the handler returns, closing
//! the connection afterwards. The request is recorded before the response is
//! written, so once a client has seen the response the record is complete.

use std::fmt::Write as _;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

/// One request as the server saw it.
#[derive(Clone, Debug)]
pub struct Request {
    /// `GET /path?query HTTP/1.1`.
    pub line: String,
    /// Header names in lower case.
    pub headers: Vec<(String, String)>,
    pub body: String,
}

impl Request {
    pub fn method(&self) -> &str {
        self.line.split(' ').next().unwrap_or_default()
    }

    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
    }

    /// The `Credential=` value of the `Authorization` header:
    /// `<key id>/<date>/<region>/<service>/aws4_request`.
    pub fn credential_scope(&self) -> Option<&str> {
        let auth = self.header("authorization")?;
        let rest = auth.split("Credential=").nth(1)?;
        rest.split(',').next()
    }
}

/// A canned response.
pub struct Response {
    pub status: u16,
    pub headers: Vec<(&'static str, String)>,
    pub body: String,
}

impl Response {
    pub fn json(body: &str) -> Self {
        Self {
            status: 200,
            headers: vec![("Content-Type", "application/json".into())],
            body: body.into(),
        }
    }

    pub fn xml(body: &str) -> Self {
        Self {
            status: 200,
            headers: vec![("Content-Type", "text/xml".into())],
            body: body.into(),
        }
    }

    pub const fn status(status: u16) -> Self {
        Self {
            status,
            headers: Vec::new(),
            body: String::new(),
        }
    }
}

type Handler = Box<dyn Fn(&Request) -> Response + Send>;

pub struct Mock {
    port: u16,
    stop: Arc<AtomicBool>,
    connections: Arc<AtomicUsize>,
    requests: Arc<Mutex<Vec<Request>>>,
    thread: Option<JoinHandle<()>>,
}

impl Mock {
    pub fn start(handler: impl Fn(&Request) -> Response + Send + 'static) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let port = listener.local_addr().expect("addr").port();
        let stop = Arc::new(AtomicBool::new(false));
        let connections = Arc::new(AtomicUsize::new(0));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let (stop_flag, count, log) = (stop.clone(), connections.clone(), requests.clone());
        let handler: Handler = Box::new(handler);
        let thread = std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(stream) = stream else { continue };
                if stop_flag.load(Ordering::SeqCst) {
                    break;
                }
                count.fetch_add(1, Ordering::SeqCst);
                serve(stream, &handler, &log);
            }
        });
        Self {
            port,
            stop,
            connections,
            requests,
            thread: Some(thread),
        }
    }

    /// A server that answers every request with an empty 500, to show that
    /// nothing contacted it.
    pub fn canary() -> Self {
        Self::start(|_| Response::status(500))
    }

    pub fn url(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }

    /// The requests recorded so far.
    pub fn requests(&self) -> Vec<Request> {
        self.requests.lock().expect("log").clone()
    }

    /// How many connections were accepted, including ones that sent nothing.
    pub fn connections(&self) -> usize {
        self.connections.load(Ordering::SeqCst)
    }
}

impl Drop for Mock {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        // Wakes the blocked accept so the thread sees the flag.
        let _ = TcpStream::connect(("127.0.0.1", self.port));
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

fn serve(mut stream: TcpStream, handler: &Handler, log: &Mutex<Vec<Request>>) {
    let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
    let mut data = Vec::new();
    let mut buf = [0u8; 4096];
    let head_end = loop {
        if let Some(at) = data.windows(4).position(|w| w == b"\r\n\r\n") {
            break at + 4;
        }
        match stream.read(&mut buf) {
            Ok(0) | Err(_) => return,
            Ok(n) => data.extend_from_slice(&buf[..n]),
        }
    };
    let head = String::from_utf8_lossy(&data[..head_end]).into_owned();
    let mut lines = head.split("\r\n");
    let line = lines.next().unwrap_or_default().to_owned();
    let headers: Vec<(String, String)> = lines
        .filter_map(|l| l.split_once(':'))
        .map(|(k, v)| (k.trim().to_ascii_lowercase(), v.trim().to_owned()))
        .collect();
    let length: usize = headers
        .iter()
        .find(|(k, _)| k == "content-length")
        .and_then(|(_, v)| v.parse().ok())
        .unwrap_or(0);
    while data.len() < head_end + length {
        match stream.read(&mut buf) {
            Ok(0) | Err(_) => break,
            Ok(n) => data.extend_from_slice(&buf[..n]),
        }
    }
    let body = String::from_utf8_lossy(&data[head_end..]).into_owned();
    let request = Request {
        line,
        headers,
        body,
    };
    let response = handler(&request);
    log.lock().expect("log").push(request.clone());

    let mut out = format!("HTTP/1.1 {} X\r\nConnection: close\r\n", response.status);
    for (k, v) in &response.headers {
        let _ = write!(out, "{k}: {v}\r\n");
    }
    let has_length = response
        .headers
        .iter()
        .any(|(k, _)| k.eq_ignore_ascii_case("content-length"));
    if !has_length {
        let _ = write!(out, "Content-Length: {}\r\n", response.body.len());
    }
    out.push_str("\r\n");
    if request.method() != "HEAD" {
        out.push_str(&response.body);
    }
    let _ = stream.write_all(out.as_bytes());
}
