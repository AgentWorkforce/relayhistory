# Proposal: canonical hostnames for machines already in the cloud

- **Status:** Proposal only. Nothing here has been run against any database.
  It needs review and sign-off from Khaliq and the cloud schema owner.
- **Date:** 2026-10-25
- **Related:** [machine identity ADR](../decisions/2026-10-25-machine-identity-and-session-trace-ids.md)

## Problem

The same machine shows up in cloud data under several hostname spellings
(`Khaliqs-MacBook-Pro.local`, `KHALIQS_MACBOOK_PRO`, `khaliqs-macbook-pro`).
Grouping by the raw hostname splits one machine into several.

## Rule

`ai_hist::canonical_hostname` (rule version
`CANONICAL_HOSTNAME_RULE_VERSION = 1`) is pure and deterministic:

1. Lowercase.
2. Strip trailing dots and `.local`.
3. Keep only the first DNS label, dropping the domain.
4. Turn every run of anything but `[a-z0-9]` (punctuation, spaces,
   underscores, apostrophes, non-ASCII) into one `-`.
5. Trim leading and trailing hyphens and cap the result at 63 characters.

An empty result becomes `unknown-host`. New installs store the canonical form
locally as `machine_name`, and every observed spelling goes in
`hostname_aliases`.

## Guarantees

- **Ids never change.** An existing `machine_id` and every row that
  references it stay byte-for-byte as they are. Canonicalization adds a name;
  it never re-keys a machine.
- **Additive only.** The original hostname column is kept unchanged. The
  canonical name is a new nullable column, so the migration can be rolled
  back by ignoring or dropping that column.
- **Dry run first.** No write happens until the dry-run report has been
  reviewed.

## Steps

### 1. Export distinct hostnames (read-only, by someone with access)

The table and column names below are **placeholders**. The cloud schema is not
in this repository and has not been verified.

```sql
SELECT hostname, count(*) FROM <machines_table> GROUP BY hostname;
```

### 2. Dry run (offline, no database)

```sh
psql ... -At -F $'\t' -c "<query above>" > hostnames.tsv
cargo run -q -p ai-hist --example canonical_hostnames < hostnames.tsv > report.json
```

`report.json` contains:

- `mapping`: every distinct spelling with its canonical form and row count.
- `merges`: every canonical name that more than one spelling folds into.
  These are the groups to eyeball.
- `unknown_hostnames`: spellings with nothing usable left.

Review the `merges` groups for false merges before going further. Two
genuinely different machines that canonicalize to the same name (two stock
`MacBook-Pro`s) must **not** be merged by name. They keep distinct
`machine_id`s, and only display grouping uses the canonical name.

### 3. Additive migration (proposal; apply only after sign-off)

```sql
-- placeholders: <machines_table>, hostname
ALTER TABLE <machines_table>
  ADD COLUMN canonical_hostname text NULL,
  ADD COLUMN canonical_hostname_rule integer NULL;

-- one UPDATE per reviewed mapping row, generated from report.json
UPDATE <machines_table>
   SET canonical_hostname = $canonical, canonical_hostname_rule = 1
 WHERE hostname = $hostname AND canonical_hostname IS NULL;

CREATE TABLE <hostname_aliases_table> (
  machine_id text NOT NULL,          -- existing id, unchanged
  observed_hostname text NOT NULL,
  canonical_hostname text NOT NULL,
  rule_version integer NOT NULL,
  PRIMARY KEY (machine_id, observed_hostname)
);
```

The mapping is generated from the reviewed report rather than reimplemented
in SQL. A second implementation of the rule would drift from the Rust one.

### 4. Forward path

New uploads send `machine_name` (already canonical) alongside the raw
hostname, so the column fills itself without another backfill. A later rule
change bumps `CANONICAL_HOSTNAME_RULE_VERSION` and re-runs steps 1 to 3 for
rows on the old version.

## Open questions

- **Apostrophes.** Under the requested rule they become hyphens, so macOS's
  ComputerName (`Khaliq's MacBook Pro`) canonicalizes to
  `khaliq-s-macbook-pro`, while its LocalHostName (`Khaliqs-MacBook-Pro`)
  becomes `khaliqs-macbook-pro`. Dropping apostrophes instead would fold the
  two together. Locally both spellings resolve to one machine through
  `hostname_aliases` either way, but a cloud grouping by canonical name would
  see two names. Which spellings does the cloud data actually hold?
- **Dotted short names.** Taking the first DNS label turns `khaliqs.macbook.pro`
  into `khaliqs`. Is a dotted non-FQDN hostname present in the data?
