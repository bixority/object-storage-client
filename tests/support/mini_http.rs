//! A tiny in-memory HTTP object server (GET, HEAD, PUT, DELETE) for tests of
//! aliased origins: `127.0.0.1` and `localhost` reach the same server.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;

type Objects = Arc<Mutex<HashMap<String, (Vec<u8>, u64)>>>;

pub struct Server {
    pub port: u16,
    objects: Objects,
}

impl Server {
    pub async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let port = listener.local_addr().expect("addr").port();
        let objects = Objects::default();
        let shared = Arc::clone(&objects);
        tokio::spawn(async move {
            let version = Arc::new(AtomicU64::new(0));
            loop {
                let Ok((sock, _)) = listener.accept().await else {
                    return;
                };
                tokio::spawn(serve(sock, Arc::clone(&shared), Arc::clone(&version)));
            }
        });
        Self { port, objects }
    }

    pub fn contents(&self, path: &str) -> Option<Vec<u8>> {
        let objects = self.objects.lock().expect("lock");
        objects.get(path).map(|(body, _)| body.clone())
    }
}

async fn serve(sock: tokio::net::TcpStream, objects: Objects, version: Arc<AtomicU64>) {
    let (read, mut write) = sock.into_split();
    let mut read = BufReader::new(read);
    loop {
        let mut request = String::new();
        if read.read_line(&mut request).await.unwrap_or(0) == 0 {
            return;
        }
        let mut parts = request.split_whitespace();
        let (method, path) = (
            parts.next().unwrap_or_default().to_owned(),
            parts.next().unwrap_or_default().to_owned(),
        );
        let mut length = 0usize;
        let mut range: Option<(usize, usize)> = None;
        loop {
            let mut line = String::new();
            if read.read_line(&mut line).await.unwrap_or(0) == 0 {
                return;
            }
            if line.trim().is_empty() {
                break;
            }
            if let Some((name, value)) = line.split_once(':') {
                if name.eq_ignore_ascii_case("content-length") {
                    length = value.trim().parse().unwrap_or(0);
                } else if name.eq_ignore_ascii_case("range") {
                    // Only `bytes=a-b` is understood.
                    range = value
                        .trim()
                        .strip_prefix("bytes=")
                        .and_then(|r| r.split_once('-'))
                        .and_then(|(a, b)| Some((a.parse().ok()?, b.parse().ok()?)));
                }
            }
        }
        let mut body = vec![0u8; length];
        if read.read_exact(&mut body).await.is_err() {
            return;
        }
        let reply = {
            let mut objects = objects.lock().expect("lock");
            match method.as_str() {
                "PUT" => {
                    let v = version.fetch_add(1, Ordering::SeqCst) + 1;
                    objects.insert(path, (body, v));
                    b"HTTP/1.1 201 Created\r\nContent-Length: 0\r\n\r\n".to_vec()
                }
                "DELETE" => {
                    objects.remove(&path);
                    b"HTTP/1.1 204 No Content\r\n\r\n".to_vec()
                }
                "GET" | "HEAD" => match objects.get(&path) {
                    Some((data, v)) if method == "GET" && range.is_some() => {
                        let (from, to) = range.unwrap_or_default();
                        let from = from.min(data.len());
                        let to = to.min(data.len().saturating_sub(1)) + 1;
                        let part = &data[from..to.max(from)];
                        let mut head = format!(
                            "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\n\
                             Content-Range: bytes {from}-{}/{}\r\nETag: \"v{v}\"\r\n\
                             Last-Modified: Tue, 01 Jan 2030 00:00:00 GMT\r\n\r\n",
                            part.len(),
                            to.max(from + 1) - 1,
                            data.len()
                        )
                        .into_bytes();
                        head.extend_from_slice(part);
                        head
                    }
                    Some((data, v)) => {
                        let mut head = format!(
                            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nETag: \"v{v}\"\r\n\
                             Last-Modified: Tue, 01 Jan 2030 00:00:00 GMT\r\n\r\n",
                            data.len()
                        )
                        .into_bytes();
                        if method == "GET" {
                            head.extend_from_slice(data);
                        }
                        head
                    }
                    None => b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n".to_vec(),
                },
                _ => b"HTTP/1.1 501 Not Implemented\r\nContent-Length: 0\r\n\r\n".to_vec(),
            }
        };
        if write.write_all(&reply).await.is_err() {
            return;
        }
    }
}
