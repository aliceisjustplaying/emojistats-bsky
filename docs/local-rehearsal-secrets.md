# Local Rehearsal Secret Locations

This file documents where this machine keeps launch-rehearsal credentials. It intentionally
records paths and env var names only, not secret values.

## Canary HMAC

- File: `/home/agent/.secrets/emojistats-canary-hmac-key`
- Env var expected by Rust CLI: `EMOJISTATS_CANARY_HMAC_KEY`
- Local export pattern:
  `export EMOJISTATS_CANARY_HMAC_KEY="$(cat /home/agent/.secrets/emojistats-canary-hmac-key)"`
- Created locally on 2026-06-18 with mode `0600`.

## StorageBox Rclone

- Rclone config: `/home/agent/.secrets/emojistats-rclone-conf`
- Rclone remote name: `storagebox`
- Observed archive directory: `storagebox:emojistats-archive/`
- Rust CLI flags:
  `--archive-backend storage-box-rclone`
  `--storage-box-rclone-config /home/agent/.secrets/emojistats-rclone-conf`
  `--storage-box-rclone-remote storagebox`
  `--storage-box-root /emojistats-archive`
- Safe read-only probe:
  `nix shell nixpkgs#rclone -c rclone --config /home/agent/.secrets/emojistats-rclone-conf lsd storagebox: --max-depth 1`

## ClickHouse

- Local endpoint used for rehearsal: `http://localhost:8123`
- Rust CLI defaults: database `emojistats`, user `default`, password empty.
- Safe probe:
  `curl -sS --max-time 2 http://localhost:8123/ping`
- No live repo `.env` file was found during the 2026-06-18 rehearsal check; only package
  `.env.example` files were present.

## Notes

- Do not print either `/home/agent/.secrets/*` file contents in logs or chat.
- Rehearsal DID lists should use `.dids` or `/tmp`; this repo ignores `*.txt`.
