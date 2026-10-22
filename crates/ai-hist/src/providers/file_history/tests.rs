use super::*;

fn isolated() -> (tempfile::TempDir, ProviderRoots, PathBuf) {
    let home = tempfile::tempdir().unwrap();
    let roots = ProviderRoots::from_home(home.path().into(), home.path().join("opencode.db"));
    let root = roots.for_provider("qoder")[0].clone();
    std::fs::create_dir_all(&root).unwrap();
    (home, roots, root)
}

#[test]
fn retirement_extension_is_explicit_and_existing_files_survive() {
    let (_home, roots, root) = isolated();
    let metadata = root.join("session.json");
    assert!(!confirmed_missing_with_extension("qoder", &roots, &metadata, "jsonl").unwrap());
    assert!(confirmed_missing_with_extension("qoder", &roots, &metadata, "json").unwrap());
    let journal = root.join("session.jsonl");
    assert!(confirmed_missing_with_extension("qoder", &roots, &journal, "jsonl").unwrap());
    assert!(!confirmed_missing_with_extension("qoder", &roots, &journal, "json").unwrap());
    std::fs::write(&metadata, b"{}").unwrap();
    assert!(!confirmed_missing_with_extension("qoder", &roots, &metadata, "json").unwrap());
}

#[test]
fn missing_root_does_not_prove_metadata_deletion() {
    let (_home, roots, root) = isolated();
    std::fs::remove_dir(&root).unwrap();
    assert!(
        !confirmed_missing_with_extension("qoder", &roots, &root.join("session.json"), "json")
            .unwrap()
    );
}

#[cfg(unix)]
#[test]
fn escaped_parent_cannot_prove_metadata_deletion() {
    let (_home, roots, root) = isolated();
    let outside = tempfile::tempdir().unwrap();
    std::os::unix::fs::symlink(outside.path(), root.join("escaped")).unwrap();
    assert!(confirmed_missing_with_extension(
        "qoder",
        &roots,
        &root.join("escaped/session.json"),
        "json"
    )
    .is_err());
}
