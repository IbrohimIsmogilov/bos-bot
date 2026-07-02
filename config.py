import os
from urllib.parse import urlparse

BOT_TOKEN = os.environ["BOT_TOKEN"]
ADMIN_ID = int(os.environ.get("ADMIN_ID", "8188875246"))
WEBAPP_URL = os.environ.get("WEBAPP_URL", "https://kslmvv.github.io/bos-course/")
DATABASE_URL = os.environ["DATABASE_URL"]
PORT = int(os.environ.get("PORT", "8080"))

# Groq API key — powers both Whisper transcription and the Llama topic-
# grouping step of the automated lesson-ingestion pipeline (lesson_pipeline.py).
GROQ_API_KEY = os.environ["GROQ_API_KEY"]

# Telegram user IDs allowed to call the /api/admin/* endpoints, as a
# comma-separated list (e.g. "111111,222222"). Falls back to just ADMIN_ID
# (the bot's super-admin) if unset.
_admin_user_ids_raw = os.environ.get("ADMIN_USER_IDS", "")
ADMIN_USER_IDS = {int(x) for x in _admin_user_ids_raw.split(",") if x.strip()} or {ADMIN_ID}

# Origin (scheme + host) of the deployed WebApp, used to restrict CORS on the
# HTTP API to the one frontend that's allowed to call it.
_webapp_parsed = urlparse(WEBAPP_URL)
WEBAPP_ORIGIN = f"{_webapp_parsed.scheme}://{_webapp_parsed.netloc}"

# Max age (seconds) of Telegram WebApp initData before it's considered expired.
INIT_DATA_MAX_AGE = int(os.environ.get("INIT_DATA_MAX_AGE", str(24 * 60 * 60)))
