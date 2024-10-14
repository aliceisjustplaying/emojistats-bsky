# Emoji stats for Bluesky

## Bootstrapping the backend on Debian 12

```bash
# Install Postgres and required packages
apt update && apt -y upgrade && apt install -y postgresql-common
/usr/share/postgresql-common/pgdg/apt.postgresql.org.sh
apt update && apt install tmux postgresql-17 postgresql postgresql-client-17 curl make gcc build-essential pkg-config libsystemd-dev libjemalloc-dev libssl-dev git vim unzip libpq-dev

tmux

# Install valkey
curl -LO https://github.com/valkey-io/valkey/archive/refs/tags/8.0.1.tar.gz && tar -xvzf 8.0.1.tar.gz && cd valkey-8.0.1/
make BUILD_TLS=yes USE_SYSTEMD=yes
make install
vim valkey.conf

# Append to valkey.conf
save 3600 1 300 10 60 100 10 10
appendonly yes
appendfsync everysec

# Start valkey
valkey-server ./valkey.conf

# Create DB
su - postgres

CREATE USER emojistats WITH PASSWORD 'replaceme';
CREATE DATABASE emojistats OWNER emojistats;
GRANT ALL PRIVILEGES ON DATABASE emojistats TO emojistats;

# Install Node and Bun
curl -fsSL https://fnm.vercel.app/install | bash
source ~/.bashrc
fnm use --install-if-missing
node -v # should print `v22.9.0`
curl -fsSL https://bun.sh/install | bash
source ~/.bashrc

# Clone the repo
git clone https://github.com/aliceisjustplaying/emojistats-bsky
bun i
# If needed, override cursor in ~/emojistats-bsky
vim CURSOR_OVERRIDE.TXT

# Set up .env, create DB tables, start
cd emojistats-bsky/packages/backend/
cp .env.example .env
vim .env
bun db:create
bun run start
```

## Current todos:

- [x] Cursor handling
- [x] Nicer tabs
- [x] Handle Weird Emojis
- [x] Initial blinking implementation
- [x] Postgres
- [ ] Backfill the entire network
- [ ] Better design
- [ ] Explore/move to SSE?
- [ ] Send updates efficiently
- [ ] More performant frontend
- [ ] etc.
