use super::*;
#[test]
fn default_roots_ignore_operator_environment() {
    let home = std::env::current_dir().unwrap().join("fixture-home");
    let r = roots::resolve(&home, false);
    assert_eq!(r.paths, vec![home.join(".kimi-code/sessions")]);
}
#[test]
fn unsupported_legacy_and_malformed_protocols_fail() {
    assert!(format::parse(b"{\"role\":\"user\"}\n").is_err());
    assert!(format::parse(
        b"{\"type\":\"metadata\",\"protocol_version\":\"9\",\"created_at\":1}\n"
    )
    .is_err());
}
