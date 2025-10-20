import os

AWS_REGION = os.getenv("AWS_REGION", "").strip()
S3_BUCKET = os.getenv("S3_BUCKET", "").strip()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

GH_TOKEN = os.getenv("GH_TOKEN", "").strip()
GH_REPO = os.getenv("GITHUB_REPOSITORY", "").strip() or os.getenv("GH_REPO", "").strip()  # e.g. "username/biosynthx-platform-"


WORKFLOW_FILE = ".github/workflows/data-pipeline.yml"


LATEST_MANIFEST_KEY = "datasets/latest/manifest.json"
CHANGELOG_KEY = "artifacts/changelog.jsonl"
TELEGRAM_OFFSET_KEY = "control/telegram_offset.txt"  
MEMORY_LOCAL = "ai_core/memory.jsonl"   

