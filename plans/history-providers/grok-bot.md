# Grok Bot desktop transcript acquisition

Source `grok-bot` reads the Grok Bot macOS app's locally persisted
transcript replicas. It is a separate product from the admitted `grok` source
(Grok Build CLI, `~/.grok/sessions`): Grok Bot is the Anysphere "sand" Electron
app (`com.anysphere.sand`, signed in with a Cursor account), and nothing it
writes resembles the Grok Build layout.

Status: investigated 2026-10-14 on a live install (Grok Bot 0.70.2, Electron
42.1.0); admission tracked in [#500](https://github.com/AgentWorkforce/relayhistory/issues/500).

## Where the history lives

Everything is under the Electron user-data directory
`~/Library/Application Support/Grok Bot/` (Linux/Windows equivalents follow
Electron's `app.getPath('userData')` convention and are unverified).

```
sand-client-persistence/
  <base32(key)>.blob          one JSON envelope per persisted slice
  .migrated-from-local-storage
```

Each blob is `{"schemaVersion": N, "value": ...}`. The file name is the
lowercase, unpadded RFC 4648 base32 encoding of the slice key. The keys that
matter:

| Key | Content |
| --- | --- |
| `sand.client.slice.account.<slot>.transcript.replicas.<agentId>` | the transcript tail for one bot (`schemaVersion` 1) |
| `sand.client.slice.account.<slot>.roster.last-roster` | bot roster: `id`, `name`, `title`, `createdAt`, `harness`, `origin`, `lastActivityAt` (`schemaVersion` 4) |
| `sand.client.slice.account.<slot>.cloud-agents.infos` | Cursor cloud agents (`bc-…`) the bots delegated to |
| `sand.client.slice.client-meta.account-slot` | the active account slot |

`<slot>` is the percent-encoded identity, e.g. `google-oauth2%7Cuser_…` or
`github%7C1724137`; one machine can hold slices for several accounts.
`sand-secrets.json` beside the store holds Cursor account credentials and must
never be read or copied by a provider.

A transcript replica's `value` is:

```
{ entries: [...], epochHint, acceptedSequenceHint, persistedAt,
  cloudAgentPeerIds?: ["bc-…"], precedingTimestampMs?, unreadAnchor? }
```

## The window: this is a tail, not an archive

The app's own slice definition is `scope: "host-durable"`: the authoritative
transcript lives on the host, and the client persists a bounded tail. From the
renderer bundle (`index.eager-app-*.js`): a replica keeps at most **200
entries** and **768 KiB** of serialized entries, is dropped when `persistedAt`
is older than **7 days**, at most **24** agents are retained per account, and
writes are flushed about **2 s** after a change. `precedingTimestampMs` is set
when older entries were cut, and a first entry whose id is not `tbs0` is the
other truncation tell.

On the investigated install two of eight bots were at the cap (`entries=200`,
first ids `t68s324` and `t75s621`), so their earlier history is already gone
locally. The app fetches older pages from the host through its internal
main-process RPC `getAgentTranscriptTail({id, beforeSeq?, limit, sinceMs?,
untilMs})` → `{entries, nextBeforeSeq, precedingTimestampMs}` over an
authenticated websocket (`wss://api.x.ai/` is the dialable origin). That path
is not reachable by a passive local scan without the app's credentials, so it
is out of scope for the built-in adapter and would be an opt-in connector
plugin if ever wanted.

Consequence for the adapter: **repeated captures must accumulate, never
replace.** Write every entry with `insert_session_event` keyed by the native
entry id as `event_uid` (the `(source, session_id, event_uid)` upsert), and do
not use the identity-preserving reconcile pass that `grok`/`muse` use, because
that pass deletes rows the newest snapshot no longer contains. With the watch
root on `sand-client-persistence/` and a 2 s flush, a running relayhistory
sees every entry before it ages out; history from before the first sync is
recoverable only through the host.

## Entry shapes observed

Every entry has `kind`, `id`, `seq`, `timestampMs`. Ids are stable and unique
within a bot: turn-scoped (`t<turn>u`, `t<turn>s<n>`, `t<turn>ua<n>`), hashed
(`event-<sha256>`, `agent-inbound-<sha256>`, `agent-outbound-<sha256>`) or
prefixed (`ar-<uuid>`, `voice-call-<callId>`).

| `kind` / `message.type` | Count | Normalize as |
| --- | --- | --- |
| `send-message` / `text` | 445 | assistant text |
| `message`, `role: user`, no agent fields | 124 | user prompt → `history` + event (`clientNonce` prefixed `voice-nudge:` marks a voice-originated prompt) |
| `message`, `role: user`, `author`/`channel` | 41 | user prompt arriving via a channel (`author.kind: cursor_user`) |
| `message`, `role: assistant`, `toAgent` | 29 | outbound delegation text to a Cursor cloud agent (`bc-…`) |
| `message`, `role: user`, `fromAgent` | 24 | inbound result from that cloud agent; role stays `user` as the provider records it |
| `send-message` / `widget` | 22 | assistant question (`widget.prompt`, `options[]`), plus the user's `respondedValue` / `widgetSkipped` |
| `send-message` / `auto-review-approval` | 8 | approval request marker (`summary`, `surface`, `status`, `command`) |
| `event` (`automation-changed`, `name-changed`) | 11 | session markers |
| `user-attachment` | 8 | user attachment marker (`file_name`, `byteSize`, dimensions; `file_path` is a host path, not local) |
| `voice-call` | 7 | call marker plus one event per `conversation.turns[]` (`speaker`, `text`, `atMs`) |
| `send-message` / `cursor-agent` | 3 | delegation spawn (`bcId`, `title`) |
| `send-message` / `connector` | 1 | connector marker |

`richText` (ProseMirror JSON) duplicates `content` and is dropped. No model,
token or cost fields exist anywhere in the store, so usage accounting is
`none`. There are no tool calls in the local shape; the bot's tool activity
runs host-side and only surfaces as text, widgets and approvals.

## Identity and catalog

- `session_id` = the bot's `agentId` (one conversation per bot, unbounded in
  time). Title from the roster `name`/`title`; `first_ts` from the oldest
  entry held (or roster `createdAt`), `last_ts` from the newest.
- No `cwd`, project or git branch: these are hosted bots, not repo sessions.
- Relationships: `cursor-agent` sends and `toAgent`/`fromAgent` messages name
  a Cursor cloud agent `bc-…` with a title and spawn time; stable child
  identity, but the child's own transcript is not on this machine, so it stays
  unlinked evidence (`stable_child_identity: "sometimes"` is wrong here;
  `"always"` with no local child is the honest cell).
- Discovery fingerprint: the blob's size + mtime is sufficient because the app
  rewrites the whole file on each flush; include `persistedAt` in the shallow
  read to detect a rewrite with equal size.
- Watch root: `sand-client-persistence/` as a tree. Discovery enumerates only
  blobs whose decoded key contains `.transcript.replicas.`; it never opens
  `sand-secrets.json`, `Cookies`, LevelDB or cache directories.

## Admission checklist (per `docs/provider-intake.md`)

1. Open the admission issue; its number is the descriptor `order`.
2. `providers/history-sources/grok-bot.json` with `rust_id: grok_bot`,
   `variant: GrokBot`, `population.evidence: ["history", "session_event",
   "session_marker", "relationship"]`, `usage_accounting: "none"`.
3. `crates/ai-hist/src/providers/grok_bot/` from `providers/templates/provider.rs`
   with an `ADAPTER` (`resolve_roots` → `<userData>/sand-client-persistence`,
   overridable by `GROK_BOT_USER_DATA`), bounded shallow read (decode key,
   parse envelope header and the last entry only), and the upsert-only
   normalization above.
4. Synthetic fixture under `crates/ai-hist/tests/fixtures/grok-bot/` (short
   JSON files and a key manifest staged under base32 names at test time,
   every value invented) covering: a complete replica from `tbs0`, a truncated
   replica with `precedingTimestampMs`, a widget with `respondedValue`, a cloud-agent
   round trip, a voice call and a second account slot.
5. Snapshot, `provider_grok_bot.rs` conformance test, generator run, capture
   cells, changelog entry.

## Proof of concept

`grok_bot_export.py` (kept out of tree during investigation) decodes the
store, joins the roster and emits one JSONL per bot using the table above. On
the live install it produced 931 rows from 8 bots and flagged the two
truncated replicas; the row shape maps one-to-one onto `EventRow`.

## Alternatives considered

- **Extending `grok`.** Rejected: different product, store, identity model and
  no shared record shape; the capture matrix would misstate what `grok` proves.
- **Host connector.** Full history exists behind `getAgentTranscriptTail`, but
  it needs the app's Cursor credentials and an undocumented protocol. Possible
  later as an opt-in plugin; never as part of the passive scan.
- **Bot-side push.** Grok Bot can run routines and call connectors, so a bot
  could post its own transcript to a webhook. That is a user workflow, not a
  relayhistory source, and it cannot backfill what the window already dropped.
