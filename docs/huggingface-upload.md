# Hugging Face Upload Runbook

Use `huggingface_hub.upload_large_folder` for this archive. It is resumable, splits large uploads into multiple commits, and stores local resume metadata under `.cache/.huggingface` in the uploaded folder.

## 1. Mount Storagebox on `emoji`

```bash
mkdir -p ~/emojistats-archive
/nix/store/ddblmkb5blg90awhgdw3rwm2fhh3yc14-rclone-1.72.1/bin/rclone \
  --config /run/secrets/emojistats-rclone-conf \
  mount storagebox:emojistats-archive ~/emojistats-archive
```

Keep that process running.

## 2. Install Upload Tooling

```bash
python3 -m venv ~/.venvs/hf-upload
source ~/.venvs/hf-upload/bin/activate
pip install -U "huggingface_hub"
export HF_TOKEN=hf_...
```

## 3. Dry Run

From the repo checkout on `emoji`:

```bash
python scripts/upload_hf_archive.py \
  --repo-id alice/emojistats-bsky \
  --archive-root ~/emojistats-archive \
  --dry-run
```

Expected current payload is roughly 6.7k parquet files and about 539 GiB, excluding `_meta` and `live`.

## 4. Upload

```bash
HF_XET_HIGH_PERFORMANCE=1 python scripts/upload_hf_archive.py \
  --repo-id alice/emojistats-bsky \
  --archive-root ~/emojistats-archive \
  --num-workers 4
```

Use a low worker count because the source is an rclone/FUSE mount. If interrupted, rerun the same command; do not delete the `.cache/.huggingface` upload cache unless intentionally starting over for a different repo.
