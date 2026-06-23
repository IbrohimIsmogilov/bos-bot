"""Upload hls_output/dayN/* to the Cloudflare R2 bucket and configure public CORS.

Idempotent: skips files already present on R2 with a matching size, so it can
be safely re-run after an interruption.
"""
import os
import sys
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()

ACCESS_KEY = os.environ["R2_ACCESS_KEY_ID"]
SECRET_KEY = os.environ["R2_SECRET_ACCESS_KEY"]
ENDPOINT = os.environ["R2_ENDPOINT"]
BUCKET = os.environ["R2_BUCKET"]
PUBLIC_URL = os.environ["R2_PUBLIC_URL"]

HLS_DIR = Path(r"C:\bos-bot\hls_output")
DAYS = ["day1", "day2", "day3", "day4", "day5", "day6"]

CONTENT_TYPES = {
    ".m3u8": "application/vnd.apple.mpegurl",
    ".ts": "video/mp2t",
}


def get_client():
    return boto3.client(
        "s3",
        endpoint_url=ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        region_name="auto",
        config=Config(signature_version="s3v4", retries={"max_attempts": 5, "mode": "standard"}),
    )


def configure_cors(client):
    try:
        client.put_bucket_cors(
            Bucket=BUCKET,
            CORSConfiguration={
                "CORSRules": [{
                    "AllowedOrigins": ["*"],
                    "AllowedMethods": ["GET", "HEAD"],
                    "AllowedHeaders": ["*"],
                    "MaxAgeSeconds": 3600,
                }]
            },
        )
        print("CORS configured (GET/HEAD from any origin)")
    except ClientError as e:
        print(f"WARNING: could not set bucket CORS via API ({e.response['Error']['Code']}). "
              f"Configure it manually in the Cloudflare dashboard (R2 -> {BUCKET} -> Settings -> CORS Policy).")


def needs_upload(client, key, size):
    try:
        head = client.head_object(Bucket=BUCKET, Key=key)
        return head["ContentLength"] != size
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchKey"):
            return True
        raise


def upload_file(path: Path, key: str):
    client = upload_file.client
    if not needs_upload(client, key, path.stat().st_size):
        return key, "skip"
    content_type = CONTENT_TYPES.get(path.suffix, "application/octet-stream")
    client.upload_file(str(path), BUCKET, key, ExtraArgs={"ContentType": content_type})
    return key, "ok"


def main():
    only = sys.argv[1:] or DAYS
    client = get_client()
    configure_cors(client)

    jobs = []
    for day in only:
        day_dir = HLS_DIR / day
        for f in sorted(day_dir.glob("*")):
            jobs.append((f, f"{day}/{f.name}"))

    print(f"Uploading {len(jobs)} files...")
    upload_file.client = client
    done = 0
    skipped = 0
    with ThreadPoolExecutor(max_workers=16) as ex:
        futures = [ex.submit(upload_file, f, key) for f, key in jobs]
        for fut in as_completed(futures):
            key, status = fut.result()
            if status == "skip":
                skipped += 1
            done += 1
            if done % 200 == 0 or done == len(jobs):
                print(f"  {done}/{len(jobs)} (skipped {skipped})")

    print("\nPublic playlist URLs:")
    for day in only:
        print(f"  {day}: {PUBLIC_URL}/{day}/playlist.m3u8")


if __name__ == "__main__":
    main()
