//! Dry run of hostname canonicalization over hostnames exported elsewhere.
//!
//! Reads one hostname per line on stdin, optionally followed by a tab and a
//! count (`hostname\tcount`, as a `SELECT hostname, count(*) ... GROUP BY
//! hostname` exports it), and prints the mapping every spelling would get and
//! which spellings would merge into one canonical name. It reads no database
//! and writes nothing; applying a mapping is a separate, reviewed migration.
//!
//! ```sh
//! printf 'Khaliqs-MacBook-Pro.local\t12\nKHALIQS_MACBOOK_PRO\t3\n' \
//!   | cargo run -q -p ai-hist --example canonical_hostnames
//! ```
use ai_hist::{canonical_hostname, CANONICAL_HOSTNAME_RULE_VERSION, UNKNOWN_HOSTNAME};
use serde_json::json;
use std::collections::BTreeMap;
use std::io::{self, BufRead};

fn main() -> io::Result<()> {
    let mut groups: BTreeMap<String, BTreeMap<String, u64>> = BTreeMap::new();
    for line in io::stdin().lock().lines() {
        let line = line?;
        let (hostname, count) = match line.rsplit_once('\t') {
            Some((hostname, count)) => match count.trim().parse::<u64>() {
                Ok(count) => (hostname.to_string(), count),
                Err(_) => (line.clone(), 1),
            },
            None => (line.clone(), 1),
        };
        *groups
            .entry(canonical_hostname(&hostname))
            .or_default()
            .entry(hostname)
            .or_default() += count;
    }
    let mapping: Vec<_> = groups
        .iter()
        .flat_map(|(canonical, variants)| {
            variants.iter().map(move |(hostname, count)| {
                json!({ "hostname": hostname, "canonical": canonical, "rows": count,
                        "changes": hostname != canonical })
            })
        })
        .collect();
    let merges: Vec<_> = groups
        .iter()
        .filter(|(_, variants)| variants.len() > 1)
        .map(|(canonical, variants)| {
            json!({ "canonical": canonical, "variants": variants,
                    "rows": variants.values().sum::<u64>() })
        })
        .collect();
    let unknown = groups.get(UNKNOWN_HOSTNAME).map(BTreeMap::len).unwrap_or(0);
    let report = json!({
        "rule_version": CANONICAL_HOSTNAME_RULE_VERSION,
        "dry_run": true,
        "distinct_hostnames": mapping.len(),
        "distinct_canonical": groups.len(),
        "unchanged": mapping.iter().filter(|m| m["changes"] == false).count(),
        "merging_groups": merges.len(),
        "unknown_hostnames": unknown,
        "merges": merges,
        "mapping": mapping,
    });
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}
