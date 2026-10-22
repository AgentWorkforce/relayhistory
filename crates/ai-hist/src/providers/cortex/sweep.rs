use super::*;
fn cache_hit(
    conn: &Connection,
    candidate: &Candidate,
    previous: &Value,
    repairs: &crate::ingest::SweepRepairs,
) -> Result<bool> {
    let Some(sid) = previous["sid"].as_str() else {
        return Ok(false);
    };
    if previous["stamp"].as_str() != Some(&candidate.stamp)
        || previous["generation"].as_str() != Some(GENERATION)
        || repairs.contains(ID, sid)
    {
        return Ok(false);
    }
    Ok(conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sessions WHERE source='cortex' AND session_id=?1)",
        [sid],
        |row| row.get(0),
    )?)
}
fn retire_missing_native(conn: &Connection, roots: &ProviderRoots) -> Result<()> {
    let mut statement = conn.prepare(
        "SELECT session_id, raw_path, 1 AS catalog FROM sessions WHERE source='cortex' AND raw_path IS NOT NULL \
         UNION SELECT session_id, raw_locator, 0 FROM session_observations WHERE source='cortex' AND location='local' AND raw_locator IS NOT NULL \
         UNION SELECT session_id, raw_locator, 0 FROM session_presences WHERE source='cortex' AND location='local' AND raw_locator IS NOT NULL",
    )?;
    let paths = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)? != 0,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut by_session: std::collections::BTreeMap<String, Vec<String>> =
        std::collections::BTreeMap::new();
    for (sid, raw, catalog) in paths {
        // Remote intake may replace the catalog's raw_path. Only a catalog
        // locator naming a configured native metadata file is local evidence;
        // explicit local observations remain blockers even if their root was
        // subsequently removed from configuration.
        if catalog
            && !roots
                .for_provider(ID)
                .iter()
                .any(|root| Path::new(&raw).parent() == Some(root.as_path()))
        {
            continue;
        }
        by_session.entry(sid).or_default().push(raw);
    }
    for (sid, paths) in by_session {
        crate::ingest::check_capture_cancelled()?;
        let mut all_missing = true;
        for raw in paths {
            let path = PathBuf::from(raw);
            let side = sidecar(&path)?;
            if !crate::providers::file_history::confirmed_missing_with_extension(
                ID, roots, &path, "json",
            )? || !crate::providers::file_history::confirmed_missing_with_extension(
                ID, roots, &side, "jsonl",
            )? {
                all_missing = false;
                break;
            }
        }
        if all_missing {
            let tx = conn.unchecked_transaction()?;
            crate::providers::file_history::retire_local_session(&tx, ID, &sid)?;
            tx.commit()?;
        }
    }
    Ok(())
}
pub(super) fn run(ctx: SweepContext<'_>) -> Result<SweepResult> {
    let env = DiscoveryEnv::with_provider_roots(ctx.conn, ctx.roots.clone())
        .with_path_scope(ctx.paths.cloned());
    let mut result = SweepResult::default();
    let cache_key = "cortex_file_stamps";
    let mut cache = ctx
        .state
        .get(cache_key)
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    let candidates = Provider.enumerate(&env, None)?;
    if ctx.paths.is_none() {
        let visited: std::collections::BTreeSet<_> =
            candidates.iter().map(|c| c.locator.as_str()).collect();
        cache.retain(|path, _| visited.contains(path.as_str()));
    }
    for candidate in candidates {
        crate::ingest::check_capture_cancelled()?;
        let path = Path::new(&candidate.locator);
        let generation = GENERATION;
        if let Some(previous) = cache.get(&candidate.locator) {
            if cache_hit(ctx.conn, &candidate, previous, ctx.repairs)? {
                continue;
            }
        }
        let ingest = || -> Result<(usize, bool, String)> {
            let snapshot = snapshot(path, &AcquisitionContext::new(ID, ctx.roots))?;
            let tx = ctx.conn.unchecked_transaction()?;
            let inserted = normalize::write(&tx, &snapshot.payload, path)?;
            tx.commit()?;
            Ok((
                inserted,
                snapshot.payload["tail"]
                    .as_array()
                    .is_some_and(|a| !a.is_empty()),
                format::string(&snapshot.payload["metadata"], "session_id")?.to_owned(),
            ))
        };
        match ingest() {
            Ok((n, partial, sid)) => {
                if partial {
                    cache.remove(&candidate.locator);
                } else {
                    cache.insert(candidate.locator.clone(), serde_json::json!({"stamp": candidate.stamp, "generation": generation, "sid": sid}));
                }
                result.inserted += n;
                result.unread += u64::from(partial);
            }
            Err(error) => {
                crate::ingest::check_capture_cancelled()?;
                eprintln!("Cortex {}: {error:#}", path.display());
                cache.remove(&candidate.locator);
                result.unread += 1;
            }
        }
    }
    if ctx.paths.is_none() && result.unread == 0 {
        retire_missing_native(ctx.conn, ctx.roots)?;
    }
    ctx.state.insert(cache_key.into(), Value::Object(cache));
    Ok(result)
}
