from pathlib import Path
from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    DATABASE_URL: str = "sqlite+aiosqlite:///./data/health.db"
    UPLOAD_DIR: str = str(Path(__file__).resolve().parent.parent / "data" / "uploads")

    # GitHub Copilot SDK settings
    COPILOT_DEFAULT_MODEL: str = "gpt-5.4"
    COPILOT_MEASUREMENT_EXTRACTION_MODEL: str = "gpt-5.4-mini"
    # Measurement extraction behaves like OCR/structured parsing, so keep
    # reasoning disabled unless we have evidence it improves accuracy enough to
    # justify the latency.
    COPILOT_MEASUREMENT_EXTRACTION_REASONING_EFFORT: Literal["low", "medium", "high", "xhigh"] | None = None
    # Use the mini model for free-form document OCR so raw transcription and
    # translation do not consume the same premium model budget as the downstream
    # summary, which still uses the default model above.
    COPILOT_TEXT_EXTRACTION_MODEL: str = "gpt-5.4-mini"
    # Text extraction is OCR/transcription work rather than a task that
    # benefits from extra reasoning.
    COPILOT_TEXT_EXTRACTION_REASONING_EFFORT: Literal["low", "medium", "high", "xhigh"] | None = None
    COPILOT_NORMALIZATION_MODEL: str = "gpt-5.4-mini"
    # Keep mini-model normalization valid by default; the client only forwards
    # reasoning when the selected model explicitly advertises support.
    COPILOT_NORMALIZATION_REASONING_EFFORT: Literal["low", "medium", "high", "xhigh"] | None = None
    # Token is read from environment – set GITHUB_TOKEN before running
    GITHUB_TOKEN: str = ""


settings = Settings()
