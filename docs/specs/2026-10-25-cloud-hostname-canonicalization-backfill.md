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
`CANONICAL_HOSTNAME_RULE_VERSION = 4`) is pure and deterministic:

1. Lowercase, NFC-normalize, and trim whitespace and trailing dots.
2. Strip network suffixes, repeatedly: `.local`, `.localdomain`, `.lan`,
   `.home`, `.home.arpa`, `.internal`. For a `*.internal` name, keep only the
   first label.
3. Per DNS label: decode an `xn--` label to Unicode, drop apostrophes
   (`'`, `‘`, `’`), and turn every run of anything but letters, digits and
   combining marks into one `-`. A label with non-ASCII letters is written
   whole in IDNA Punycode (`café-box` becomes `xn--caf-box-dya`). Labels
   are joined by `-`, so no label is lost.
4. Cut at 63 characters from the right, never inside an `xn--` label. A
   single label too long on its own becomes `h-` plus 16 hex digits of its
   hash.

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
   SET canonical_hostname = $canonical, canonical_hostname_rule = 4
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

## Decisions

- **Apostrophes:** dropped (rule 2, Khaliq, 2026-10-25). `Khaliq's MacBook Pro`
  and `Khaliqs-MacBook-Pro` both become `khaliqs-macbook-pro`.
- **Dotted names:** keep every label except network suffixes (rule 3, Khaliq,
  2026-10-25). `khaliqs.macbook.pro` becomes `khaliqs-macbook-pro`. As a
  consequence, an FQDN-named server keeps its domain:
  `build-01.corp.example.com` becomes `build-01-corp-example-com`.
- **Non-ASCII names:** NFC, then Punycode (rule 4, Khaliq, 2026-10-25). The
  CJK names that collapsed to `unknown-host` under rule 3 are now distinct,
  decodable `xn--` names.

## Observed data (2026-10-25)

A read-only dry run on production, approved by Khaliq and run by
query-agent with the tool at rule 3. No hostnames were shared or recorded
here. Findings:

- **Where hostnames are:** the cloud has no general machine or session
  hostname table. Only desktop sign-in rows carry one:
  `api_token_sessions.desktop_host_name` and
  `cli_device_authorizations.desktop_host_name`. Those are the
  `<machines_table>` placeholders above, until relayhistory's own upload
  grows a machine table.
- **Volume:** 11 distinct spellings and 11 canonical names (about 15 rows),
  with 0 merge groups and 1 `unknown-host`. It's too small a sample to prove
  much.
- **Non-ASCII names:** a hostname with no ASCII letters or digits (CJK, in
  this data) canonicalizes to `unknown-host`, so every such machine shares
  one name. Machine ids stay distinct through the install suffix, but
  grouping by canonical name would merge them. Mixed names are also lossy
  (`ñandú-box` becomes `and-box`).
- **Factory-default names** (`MacBook-Pro.local`, `mac.lan`) collapse to
  generic names. That's expected, and it's why the hostname alone is never
  the id.
