import os
from urllib.parse import urlparse

BOT_TOKEN = os.environ["BOT_TOKEN"]
ADMIN_ID = int(os.environ.get("ADMIN_ID", "8188875246"))
WEBAPP_URL = os.environ.get("WEBAPP_URL", "https://kslmvv.github.io/bos-course/")
DATABASE_URL = os.environ["DATABASE_URL"]
PORT = int(os.environ.get("PORT", "8080"))

# Origin (scheme + host) of the deployed WebApp, used to restrict CORS on the
# HTTP API to the one frontend that's allowed to call it.
_webapp_parsed = urlparse(WEBAPP_URL)
WEBAPP_ORIGIN = f"{_webapp_parsed.scheme}://{_webapp_parsed.netloc}"

# Max age (seconds) of Telegram WebApp initData before it's considered expired.
INIT_DATA_MAX_AGE = int(os.environ.get("INIT_DATA_MAX_AGE", str(24 * 60 * 60)))
