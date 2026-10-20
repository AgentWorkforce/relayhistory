use super::*;
#[test]
fn default_roots_ignore_operator_environment() {
    let r = roots::resolve(Path::new("/fixture/home"), false);
    assert_eq!(
        r.paths,
        vec![PathBuf::from("/fixture/home/.kimi-code/sessions")]
    );
}
#[test]
fn unsupported_legacy_and_malformed_protocols_fail() {
    assert!(format::parse(b"{\"role\":\"user\"}\n").is_err());
    assert!(format::parse(
        b"{\"type\":\"metadata\",\"protocol_version\":\"9\",\"created_at\":1}\n"
    )
    .is_err());
}
