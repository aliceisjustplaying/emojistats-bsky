create extension if not exists timescaledb cascade;
create extension if not exists btree_gin;
create extension if not exists pgcrypto;

create table if not exists dim_language (
    lang_id      smallint generated always as identity primary key,
    bcp47        text not null unique,
    display_name text not null
);

create table if not exists dim_client (
    client_id    smallint generated always as identity primary key,
    identifier   text not null unique,
    display_name text
);

create table if not exists dim_emoji (
    emoji_id    smallint generated always as identity primary key,
    glyph       text not null unique,
    group_name  text not null,
    shortcodes  text[] not null default '{}'
);

create table if not exists emoji_post (
    post_uri         text not null,
    repo_did         text not null,
    rkey             text not null,
    seq              bigint not null,
    created_at       timestamptz not null,
    received_at      timestamptz not null default now(),
    lang_id          smallint not null references dim_language(lang_id),
    client_id        smallint references dim_client(client_id),
    emoji_ids        smallint[] not null,
    emoji_count      smallint generated always as (cardinality(emoji_ids)) stored,
    author_did       text not null,
    reply_root_uri   text,
    reply_parent_uri text,
    hidden           boolean not null default false,
    constraint emoji_ids_not_empty check (cardinality(emoji_ids) > 0),
    constraint emoji_post_pk primary key (repo_did, created_at, post_uri)
);

select create_hypertable(
    relation => 'emoji_post',
    time_column_name => 'created_at',
    partitioning_column => 'repo_did',
    number_partitions => 8,
    chunk_time_interval => interval '7 days',
    if_not_exists => true
);

create unique index if not exists idx_emoji_post_repo_rkey on emoji_post (repo_did, created_at, rkey);
create index if not exists idx_emoji_post_created_at_brin on emoji_post using brin (created_at) with (pages_per_range = 64);
create index if not exists idx_emoji_post_lang_created on emoji_post (lang_id, created_at desc);
create index if not exists idx_emoji_post_client_created on emoji_post (client_id, created_at desc);
create index if not exists idx_emoji_post_emoji_gin on emoji_post using gin (emoji_ids);
create index if not exists idx_emoji_post_visible_created on emoji_post (created_at) where hidden = false;

create table if not exists repo_progress (
    repo_did          text primary key,
    last_rev          text not null,
    last_seq          bigint not null,
    last_snapshot_row_count bigint,
    last_snapshot_path text,
    last_snapshot_parquet_count bigint,
    updated_at        timestamptz not null default now(),
    backfill_complete boolean not null default false,
    car_completed     boolean not null default false
);

alter table if exists repo_progress
    add column if not exists last_snapshot_row_count bigint;

alter table if exists repo_progress
    add column if not exists last_snapshot_path text;

alter table if exists repo_progress
    add column if not exists last_snapshot_parquet_count bigint;

alter table if exists repo_progress
    add column if not exists car_completed boolean not null default false;

create table if not exists repo_validation_log (
    validation_id    bigserial primary key,
    repo_did         text not null,
    validated_at     timestamptz not null default now(),
    processed_rows   bigint not null,
    inserted_rows    bigint not null,
    parquet_rows     bigint not null,
    existing_rows    bigint not null,
    total_rows       bigint not null,
    snapshot_path    text,
    extras_detected  boolean not null default false
);

create index if not exists idx_repo_validation_log_repo
    on repo_validation_log (repo_did, validated_at desc);

do $$
begin
    create type job_status as enum ('running', 'succeeded', 'failed');
exception when duplicate_object then
    null;
end
$$;

create table if not exists ingest_job_log (
    job_id      bigint generated always as identity primary key,
    job_type    text not null,
    status      job_status not null,
    started_at  timestamptz not null default now(),
    finished_at timestamptz,
    detail      jsonb
);

create table if not exists ingest_watermark (
    name           text primary key,
    max_created_at timestamptz not null,
    updated_at     timestamptz not null default now()
);

create materialized view if not exists emoji_hourly_stats
with (timescaledb.continuous) as
select time_bucket('1 hour', ep.created_at) as bucket,
       e.emoji_id,
       ep.lang_id,
       ep.client_id,
       count(*) as post_count,
       count(distinct ep.author_did) as author_count,
       min(ep.created_at) as first_seen
from emoji_post ep
join lateral unnest(ep.emoji_ids) as e(emoji_id) on true
where ep.hidden = false
group by bucket, e.emoji_id, ep.lang_id, ep.client_id;

select add_continuous_aggregate_policy(
    continuous_aggregate => 'emoji_hourly_stats',
    start_offset => interval '30 days',
    end_offset => interval '30 minutes',
    schedule_interval => interval '5 minutes'
);

create materialized view if not exists language_daily_totals
with (timescaledb.continuous) as
select time_bucket('1 day', created_at) as bucket,
       lang_id,
       count(*) as lang_post_count
from emoji_post
where hidden = false
group by bucket, lang_id;

select add_continuous_aggregate_policy(
    continuous_aggregate => 'language_daily_totals',
    start_offset => interval '400 days',
    end_offset => interval '1 day',
    schedule_interval => interval '30 minutes'
);

create materialized view if not exists emoji_daily_stats
with (timescaledb.continuous) as
select time_bucket('1 day', ep.created_at) as bucket,
       e.emoji_id,
       ep.lang_id,
       ep.client_id,
       count(*) as post_count,
       count(distinct ep.author_did) as author_count,
       min(ep.created_at) as first_seen
from emoji_post ep
join lateral unnest(ep.emoji_ids) as e(emoji_id) on true
where ep.hidden = false
group by bucket, e.emoji_id, ep.lang_id, ep.client_id;

select add_continuous_aggregate_policy(
    continuous_aggregate => 'emoji_daily_stats',
    start_offset => interval '400 days',
    end_offset => interval '1 day',
    schedule_interval => interval '1 hour'
);

create table if not exists emoji_trend_window (
    trend_id     bigserial primary key,
    window_name  text not null,
    window_start date not null,
    window_end   date not null,
    emoji_id     smallint not null references dim_emoji(emoji_id),
    lang_id      smallint not null references dim_language(lang_id),
    client_id    smallint references dim_client(client_id),
    post_count   bigint not null
);

create unique index if not exists idx_trend_window_unique
    on emoji_trend_window (window_name, window_start, emoji_id, lang_id, coalesce(client_id, -1));
