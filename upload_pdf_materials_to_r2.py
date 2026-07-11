"""Upload course_materials/<course>/pdf/*.pdf to the same Cloudflare R2
bucket already used for HLS video (see upload_to_r2.py) and print public
URLs to paste into the seed script (e.g. seed_atm_course.py) as
storage_url values.

Idempotent: skips files already present on R2 with a matching size.

Usage:  python upload_pdf_materials_to_r2.py atm
"""
import os
import sys
from pathlib import Path

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

MATERIALS_DIR = Path(r"C:\bos-bot\course_materials")


def get_client():
    return boto3.client(
        "s3",
        endpoint_url=ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        region_name="auto",
        config=Config(signature_version="s3v4", retries={"max_attempts": 5, "mode": "standard"}),
    )


def needs_upload(client, key, size):
    try:
        head = client.head_object(Bucket=BUCKET, Key=key)
        return head["ContentLength"] != size
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchKey"):
            return True
        raise


def main():
    if len(sys.argv) != 2:
        print("Usage: python upload_pdf_materials_to_r2.py <course_id>")
        sys.exit(1)
    course_id = sys.argv[1]

    pdf_dir = MATERIALS_DIR / course_id / "pdf"
    files = sorted(pdf_dir.glob("*.pdf"))
    if not files:
        print(f"No PDFs found in {pdf_dir}")
        sys.exit(1)

    client = get_client()
    print(f"Uploading {len(files)} PDF(s) from {pdf_dir}...\n")

    urls = {}
    for f in files:
        key = f"course-materials/{course_id}/pdf/{f.name}"
        size = f.stat().st_size
        if needs_upload(client, key, size):
            client.upload_file(str(f), BUCKET, key, ExtraArgs={"ContentType": "application/pdf"})
            status = "uploaded"
        else:
            status = "already up to date, skipped"
        url = f"{PUBLIC_URL}/{key}"
        urls[f.name] = url
        print(f"  {f.name}: {status}\n    {url}")

    print("\nstorage_url values for the seed script:")
    for name, url in urls.items():
        print(f'  "{name}": "{url}",')


if __name__ == "__main__":
    main()
