#!/usr/bin/env bash
# Muster gate for the rust/ workspace — the local stand-in for CI (no CI wired yet).
# Every gate must pass before code is shippable.
#   exit 0 = all gates green
#   exit 1 = a gate failed
#   exit 2 = a gate was skipped (its tool isn't installed) — run is INCOMPLETE
set -uo pipefail
cd "$(dirname "$0")" || exit 1

# cargo needs a linker (`cc`). The dev sandbox has none on PATH; fall back to a nix-store
# gcc-wrapper if one exists. On a normally-provisioned box `cc` is already present.
if ! command -v cc >/dev/null 2>&1; then
  wrapper="$(ls -d /nix/store/*gcc-wrapper*/bin 2>/dev/null | head -1)"
  if [ -n "${wrapper}" ]; then
    export PATH="${wrapper}:${PATH}"
    echo "note: no cc on PATH; using ${wrapper}"
  fi
fi

failed=0
incomplete=0
missing=()

have() { command -v "$1" >/dev/null 2>&1; }

run() { # run NAME -- CMD...
  local name="$1"; shift; [ "$1" = "--" ] && shift
  printf '\n=== %s ===\n' "$name"
  if "$@"; then echo "PASS: ${name}"; else echo "FAIL: ${name}"; failed=1; fi
}

gated() { # gated NAME TOOL -- CMD...
  local name="$1" tool="$2"; shift 2; [ "$1" = "--" ] && shift
  if have "$tool"; then
    run "$name" -- "$@"
  else
    printf '\n=== %s ===\nSKIP: %s not installed\n' "$name" "$tool"
    incomplete=1; missing+=("$tool")
  fi
}

run   "fmt"      -- cargo fmt --all -- --check
run   "clippy"   -- cargo clippy --workspace --all-targets --all-features -- -D warnings
run   "test"     -- cargo test --workspace --all-features
gated "nextest"  cargo-nextest  -- cargo nextest run --workspace --all-features
gated "deny"     cargo-deny     -- cargo deny check
gated "audit"    cargo-audit    -- cargo audit
gated "machete"  cargo-machete  -- cargo machete
gated "coverage" cargo-llvm-cov -- cargo llvm-cov nextest --workspace --all-features

echo
echo "================ summary ================"
if [ "${failed}" -ne 0 ]; then
  echo "RESULT: FAILED"; exit 1
elif [ "${incomplete}" -ne 0 ]; then
  echo "RESULT: INCOMPLETE — missing tools: ${missing[*]}"
  echo "install on NixOS, e.g. add to systemPackages / devShell: ${missing[*]}"
  exit 2
else
  echo "RESULT: PASS"; exit 0
fi
