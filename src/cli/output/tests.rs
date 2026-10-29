use super::*;

#[test]
fn redact_url_drops_secrets() {
    assert_eq!(
        redact_url("https://user:secret@host.example/a/b?X-Amz-Signature=abc#frag"),
        "https://host.example/a/b?…"
    );
    assert_eq!(redact_url("s3://bucket/key"), "s3://bucket/key");
    assert_eq!(redact_url("https://h/x?sig=1"), "https://h/x?…");
    assert_eq!(redact_url("https://:pw@h/x"), "https://h/x");
    assert_eq!(redact_url("https://tok@h/x?q=1"), "https://h/x?…");
    // Not parseable as a URL: still cut.
    let odd = redact_url("http://u:pw@[bad/x?token=1");
    assert!(!odd.contains("pw") && !odd.contains("token"), "{odd}");
}
