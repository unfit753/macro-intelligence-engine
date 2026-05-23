import os
from datetime import datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]

DB_PATH = os.getenv(
    "MACRO_ENGINE_DB_PATH",
    str(REPO_ROOT / "data" / "macro_engine.db"),
)

LOG_DIR = os.getenv(
    "MACRO_ENGINE_LOG_DIR",
    str(REPO_ROOT / "data" / "logs"),
)

# Default Claude model for prediction, briefing, and macro-event forecast calls.
CLAUDE_MODEL = os.getenv(
    "MACRO_ENGINE_CLAUDE_MODEL",
    "claude-haiku-4-5-20251001",
)

def log(message: str, module: str = "general"):
    """
    Write a timestamped message to the configured module log.
    """
    os.makedirs(LOG_DIR, exist_ok=True)
    log_file = os.path.join(LOG_DIR, f"{module}.log")
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    full_message = f"[{timestamp}] [{module.upper()}] {message}"
    with open(log_file, "a") as f:
        f.write(full_message + "\n")
    print(full_message)
    
