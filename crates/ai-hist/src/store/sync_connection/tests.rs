use super::*;

fn fixture() -> (tempfile::TempDir, PathBuf, Cache) {
    let temp = tempfile::tempdir().unwrap();
    let db = temp.path().join("history.db");
    open_db(&db).unwrap();
    (temp, db, Cache::default())
}

fn use_connection(cache: &mut Cache, db: &Path) {
    with_cache(cache, || with_connection(db, |_| Ok(()))).unwrap();
}

#[test]
fn unchanged_connection_validates_once_but_external_ddl_and_data_revalidate() {
    let (_temp, db, mut cache) = fixture();
    let before = validations();
    for _ in 0..125 {
        use_connection(&mut cache, &db);
    }
    assert_eq!(validations() - before, 1);
    let external = open_db(&db).unwrap();
    external
        .execute_batch("DROP INDEX idx_session_continuity_origin")
        .unwrap();
    use_connection(&mut cache, &db);
    assert!(!needs_migration(&external).unwrap());
    let after_ddl = validations();
    external
        .execute(
            "DELETE FROM schema_migrations WHERE name = 'claude_standalone_records_v1'",
            [],
        )
        .unwrap();
    assert!(needs_migration(&external).unwrap());
    use_connection(&mut cache, &db);
    assert!(
        !needs_migration(&external).unwrap(),
        "a data-only readiness change must migrate"
    );
    assert!(validations() > after_ddl);
}

#[test]
fn a_write_during_acquisition_is_not_adopted_as_already_validated() {
    let (_temp, db, mut cache) = fixture();
    use_connection(&mut cache, &db);
    let external = open_db(&db).unwrap();
    with_cache(&mut cache, || {
        with_connection(&db, |_| {
            external.execute(
                "DELETE FROM schema_migrations WHERE name='claude_standalone_records_v1'",
                [],
            )?;
            Ok(())
        })
    })
    .unwrap();
    let before = validations();
    use_connection(&mut cache, &db);
    assert!(validations() > before);
    assert!(!needs_migration(&external).unwrap());
}

#[test]
fn a_writer_during_validation_allows_the_call_without_caching_or_extra_retries() {
    let (_temp, db, mut cache) = fixture();
    let external = open_db(&db).unwrap();
    AFTER_VALIDATION.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(move || {
            external
                .execute(
                    "DELETE FROM schema_migrations WHERE name='claude_standalone_records_v1'",
                    [],
                )
                .unwrap();
        }));
    });
    let before = validations();
    with_cache(&mut cache, || {
        with_connection(&db, |conn| {
            assert!(needs_migration(conn)?);
            Ok(())
        })
    })
    .unwrap();
    assert_eq!(
        validations() - before,
        1,
        "use the existing single initialization pass"
    );
    assert!(
        cache.0.is_none(),
        "an unstable bracket cannot authorize future reuse"
    );
    with_cache(&mut cache, || {
        with_connection(&db, |conn| {
            assert!(!needs_migration(conn)?);
            Ok(())
        })
    })
    .unwrap();
    assert_eq!(validations() - before, 2);
    // Repair can change the schema cookie itself. Reuse starts after a
    // subsequent quiet validation, without retrying inside either call.
    use_connection(&mut cache, &db);
    assert_eq!(validations() - before, 3);
    assert!(cache.0.is_some());
}

#[test]
fn nested_auxiliary_paths_never_borrow_the_outer_connection() {
    let (temp, db, mut cache) = fixture();
    let other = temp.path().join("other.db");
    let outer_path = db.clone();
    with_cache(&mut cache, || {
        with_connection(&db, |outer| {
            with_connection(&other, |inner| {
                assert_ne!(outer.path(), inner.path());
                assert_eq!(
                    file_identity(Path::new(outer.path().unwrap())),
                    file_identity(&outer_path)
                );
                Ok(())
            })
        })
    })
    .unwrap();
    use_connection(&mut cache, &db);
    assert_eq!(cache.0.as_ref().unwrap().path, db);
}

#[test]
fn a_failed_transaction_is_closed_and_panics_restore_the_scoped_cache() {
    let (_temp, db, mut cache) = fixture();
    let failed = with_cache(&mut cache, || {
        with_connection(&db, |conn| -> Result<()> {
            conn.execute_batch(
                "BEGIN; INSERT INTO schema_migrations(name) VALUES ('must_rollback')",
            )?;
            anyhow::bail!("acquisition failed")
        })
    });
    assert!(failed.is_err() && cache.0.is_none());
    with_cache(&mut cache, || {
        with_connection(&db, |conn| {
            assert_eq!(
                conn.query_row(
                    "SELECT COUNT(*) FROM schema_migrations WHERE name='must_rollback'",
                    [],
                    |r| r.get::<_, i64>(0)
                )?,
                0
            );
            Ok(())
        })
    })
    .unwrap();
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        with_cache(&mut cache, || {
            with_connection(&db, |_| -> Result<()> { panic!("provider panic") })
        })
    }));
    assert!(panic.is_err());
    assert!(CURRENT.with(|slot| slot.borrow().is_none()));
    use_connection(&mut cache, &db);
}

#[cfg(unix)]
#[test]
fn replacing_the_database_discards_the_old_connection() {
    let (temp, db, mut cache) = fixture();
    use_connection(&mut cache, &db);
    let old_identity = cache.0.as_ref().unwrap().identity.clone();
    let other = temp.path().join("replacement.db");
    let conn = open_db(&other).unwrap();
    conn.execute(
        "INSERT INTO schema_migrations(name) VALUES ('replacement_only')",
        [],
    )
    .unwrap();
    drop(conn);
    fs::rename(other, &db).unwrap();
    with_cache(&mut cache, || {
        with_connection(&db, |conn| {
            assert_eq!(
                conn.query_row(
                    "SELECT COUNT(*) FROM schema_migrations WHERE name='replacement_only'",
                    [],
                    |r| r.get::<_, i64>(0)
                )?,
                1
            );
            Ok(())
        })
    })
    .unwrap();
    assert_ne!(cache.0.as_ref().unwrap().identity, old_identity);
}
