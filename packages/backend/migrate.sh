#!/usr/bin/env bash
set -euo pipefail

error_exit() {
  echo "Error: $1"
  exit 1
}

if [ "$#" -ne 1 ]; then
  echo "Usage: $0 <migrations_directory>"
  error_exit "Migrations directory not specified."
fi

MIGRATIONS_DIR="$1"

if [[ ! -e "$MIGRATIONS_DIR" ]]; then
  error_exit "'$MIGRATIONS_DIR' does not exist."
elif [[ ! -d "$MIGRATIONS_DIR" ]]; then
  error_exit "'$MIGRATIONS_DIR' exists but is not a directory."
fi

ENV_FILE=".env"
if [ -f "$ENV_FILE" ]; then
  echo "Sourcing $ENV_FILE..."
  set -a
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +a
else
  echo "No .env file found. Proceeding without environment variables."
fi

if [ -z "${DATABASE_URL:-}" ]; then
  error_exit "DATABASE_URL environment variable is not set."
fi

if ! command -v psql >/dev/null 2>&1; then
  error_exit "'psql' command not found. Please install PostgreSQL client."
fi

mapfile -t SQL_FILES < <(find "$MIGRATIONS_DIR" -maxdepth 1 -type f -name "*.sql" | sort)

if [ "${#SQL_FILES[@]}" -eq 0 ]; then
  echo "No SQL files found in '$MIGRATIONS_DIR'. Nothing to migrate."
  exit 0
fi

for sql_file in "${SQL_FILES[@]}"; do
  echo "Applying migration: $sql_file"
  if ! psql "$DATABASE_URL" -f "$sql_file"; then
    error_exit "Failed to apply migration: $sql_file"
  fi
done

echo "All migrations applied successfully."
