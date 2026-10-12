use super::*;

// Called only after successful full enumeration and normalization of survivors.
pub(super) fn retire_missing(conn: &Connection, roots: &ProviderRoots) -> Result<()> {
    let local = local_footprint();
    let mut stmt = conn.prepare(&format!("SELECT DISTINCT raw_path FROM sessions s WHERE source='pi' AND raw_path IS NOT NULL AND ({local})"))?;
    let paths = stmt
        .query_map([], |r| r.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    for path in paths {
        crate::ingest::check_capture_cancelled()?;
        let path = PathBuf::from(path);
        if confirmed_missing(roots, &path)? {
            let tx = conn.unchecked_transaction()?;
            normalize::retire_missing_locator(&tx, &path)?;
            tx.commit()?;
        }
    }
    Ok(())
}
fn confirmed_missing(roots: &ProviderRoots, path: &Path) -> Result<bool> {
    if path.extension().is_none_or(|e| e != "jsonl") {
        return Ok(false);
    }
    let context = AcquisitionContext::new(ID, roots);
    for root in roots.for_provider(ID) {
        let Ok(relative) = path.strip_prefix(root) else {
            continue;
        };
        if !relative
            .components()
            .all(|p| matches!(p, std::path::Component::Normal(_)))
            || !root.is_dir()
        {
            continue;
        }
        match std::fs::symlink_metadata(path) {
            Ok(_) => {
                context.validate(path)?;
                return Ok(false);
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
        context.validate(root)?;
        for entry in std::fs::read_dir(root)? {
            entry?;
        }
        let mut parent = path.parent();
        while let Some(dir) = parent {
            if dir == root {
                break;
            }
            validate_directory(&context, dir)?;
            parent = dir.parent();
        }
        // Revalidate the required root before accepting the locator's absence.
        context.validate(root)?;
        std::fs::read_dir(root)?;
        return Ok(!path.try_exists()?);
    }
    Ok(false)
}
fn validate_directory(context: &AcquisitionContext<'_>, dir: &Path) -> Result<()> {
    match std::fs::symlink_metadata(dir) {
        Ok(_) => {
            context.validate(dir)?;
            for entry in std::fs::read_dir(dir)? {
                entry?;
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e.into()),
    }
    Ok(())
}

pub(super) fn local_footprint() -> String {
    let evidence = crate::store::EVIDENCE_LOCATION_TABLES.iter().map(|table|
        format!("EXISTS(SELECT 1 FROM {table} e WHERE e.source=s.source AND e.session_id=s.session_id AND e.location IN ('local','both'))"))
        .collect::<Vec<_>>().join(" OR ");
    format!("EXISTS(SELECT 1 FROM session_presences p WHERE p.source=s.source AND p.session_id=s.session_id AND p.location='local') OR EXISTS(SELECT 1 FROM session_observations o WHERE o.source=s.source AND o.session_id=s.session_id AND o.location='local') OR {evidence}")
}
