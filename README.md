# Butterdrive

A cross-platform (Windows/macOS/Linux) desktop sync client for
[Buttercloud](https://github.com/Mantequilla-Soft/buttercloud) — a local
"drive" folder, in the same category as the OneDrive/Dropbox/Google Drive
desktop apps. Built in Rust, shell in Tauri.

## Layout

- `crates/sync-core` — the real logic, no UI dependency: the Buttercloud/S3
  client, OS-keychain credential storage, local filesystem watching, the
  sync-state database, and the sync engine itself. Fully headless/testable.
- `apps/desktop` — the Tauri v2 shell (tray icon, onboarding, status window)
  that drives `sync-core`.

## Before touching this, read

- [`llms.txt`](https://github.com/Mantequilla-Soft/buttercloud/blob/main/llms.txt)
  on the Buttercloud repo — the best first read on the API this client talks to.
- [`README.md`](https://github.com/Mantequilla-Soft/buttercloud/blob/main/README.md)
  on that same repo — full API reference, architecture diagram.
- Open issues this client depends on:
  [#13](https://github.com/Mantequilla-Soft/buttercloud/issues/13) (no recycle
  bin yet — deletes are permanent),
  [#14](https://github.com/Mantequilla-Soft/buttercloud/issues/14) (no
  presigned PUT),
  [#15](https://github.com/Mantequilla-Soft/buttercloud/issues/15) (no
  server-side rate limiting),
  [#16](https://github.com/Mantequilla-Soft/buttercloud/issues/16) (no
  device-pairing auth flow),
  [#17](https://github.com/Mantequilla-Soft/buttercloud/issues/17) (no
  change-notification/webhooks — this client polls instead).

Note: this client is **SigV4-only, end to end** — it never touches
Buttercloud's JWT/portal API (`GET /api/v1/*`), including for remote-change
detection (uses `ListObjectsV2` instead) and bucket discovery (the bucket
name is entered during onboarding instead of fetched).

## Development

Requires Rust (stable, 2024 edition) and Node for the Tauri CLI/frontend
tooling.

```bash
cd apps/desktop
npm install
npm run tauri dev
```

`cargo build --workspace` / `cargo test -p sync-core` from the repo root
build and test `sync-core` without touching the UI.

Credentials (access key, secret key, bucket name) are entered once at
onboarding and stored in the OS keychain — never in a config file.
