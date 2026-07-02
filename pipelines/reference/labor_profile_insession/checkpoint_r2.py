"""Step 6 — CHECKPOINT: durable R2 snapshot of the in-progress results (restore-on-crash).

Bundles every completed <scratch>/results/*.json into one JSONL and uploads to R2 staging:
    s3://<bucket>/<prefix>/opus_results_latest.jsonl        (overwritten each run)
    s3://<bucket>/<prefix>/snapshot_<N>calls.jsonl          (immutable per checkpoint)
Uses pyarrow.fs (same object-store lance uses). Restore = download the JSONL and re-explode into
<scratch>/results/<cid>.json (each line already carries its custom_id).

NOT the SoR — staging only; the fail-closed Lance materialization happens after all combos land.

Env:
    NPLP_SCRATCH          scratch root (default /tmp/nplp)
    NPLP_CHECKPOINT_PREFIX  R2 key prefix WITHOUT bucket (default staging/nplp_insession)
    R2_ACCESS_KEY_ID / R2_SECRET_ACCESS_KEY / (R2_ENDPOINT | R2_ACCOUNT_ID)   (Doppler core-x/prd)
"""
from __future__ import annotations

import json
import os

import pyarrow.fs as pafs

SCRATCH = os.environ.get("NPLP_SCRATCH", "/tmp/nplp")
RESULTS = os.path.join(SCRATCH, "results")
BUCKET = os.environ.get("NPLP_BUCKET", "data-sink")
PREFIX = os.environ.get("NPLP_CHECKPOINT_PREFIX", "staging/nplp_insession").strip("/")


def main() -> None:
    cids = json.load(open(f"{SCRATCH}/cids.json"))
    present = [c for c in cids if os.path.exists(f"{RESULTS}/{c}.json")]

    lines, bad = [], []
    for c in present:
        try:
            o = json.load(open(f"{RESULTS}/{c}.json"))
            o.setdefault("custom_id", c)
            lines.append(json.dumps(o, separators=(",", ":")))
        except Exception as e:  # noqa: BLE001
            bad.append((c, str(e)[:40]))
    body = ("\n".join(lines) + "\n").encode()
    local = f"{SCRATCH}/results_checkpoint.jsonl"
    open(local, "wb").write(body)
    print(f"bundled {len(lines)} calls, {len(bad)} unreadable -> {local} ({len(body):,} bytes)")
    if bad:
        print("  unreadable:", bad[:10])

    ep = os.environ.get("R2_ENDPOINT") or \
        f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com"
    host = ep.replace("https://", "").replace("http://", "")
    fs = pafs.S3FileSystem(access_key=os.environ["R2_ACCESS_KEY_ID"],
                           secret_key=os.environ["R2_SECRET_ACCESS_KEY"],
                           endpoint_override=host, scheme="https", region="auto")

    base = f"{BUCKET}/{PREFIX}"
    for key in (f"{base}/opus_results_latest.jsonl", f"{base}/snapshot_{len(lines)}calls.jsonl"):
        with fs.open_output_stream(key) as fh:
            fh.write(body)
        info = fs.get_file_info(key)
        print(f"uploaded s3://{key}  size={info.size:,}")
    print(f"CHECKPOINT OK: {len(lines)}/{len(cids)} calls durable in R2")


if __name__ == "__main__":
    main()
