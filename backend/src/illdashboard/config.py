from pathlib import Path
from typing import Literal

from pydantic import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy.engine import make_url

ProviderName = Literal["copilot", "mistral"]

BACKEND_DIR = Path(__file__).resolve().parents[2]


def _normalize_sqlite_url(database_url: str) -> str:
    url = make_url(database_url)
    database_path = url.database
    if url.get_backend_name() != "sqlite" or database_path in {None, "", ":memory:"}:
        return database_url

    if database_path.startswith("file:"):
        return database_url

    resolved_path = Path(database_path)
    if resolved_path.is_absolute():
        return database_url

    return url.set(database=str((BACKEND_DIR / resolved_path).resolve())).render_as_string(hide_password=False)


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=BACKEND_DIR / ".env", env_file_encoding="utf-8")

    DATABASE_URL: str = "sqlite+aiosqlite:///./data/health.db"
    MEDICATIONS_DATABASE_URL: str = "sqlite+aiosqlite:///./data/medications.db"
    UPLOAD_DIR: str = str(Path(__file__).resolve().parent.parent / "data" / "uploads")
    FRONTEND_DIST_DIR: str = str(Path(__file__).resolve().parents[3] / "frontend" / "dist")
    # Keep the first pass as a global provider switch so the durable pipeline can
    # reuse its existing job graph without adding per-request provider state.
    EXTRACTION_PROVIDER: ProviderName = "copilot"
    NORMALIZATION_PROVIDER: ProviderName = "copilot"

    # GitHub Copilot SDK settings
    # Terra balances general-purpose quality and cost for summaries; Luna keeps
    # repeated OCR, translation, and normalization batches lightweight. This is
    # a workload/cost policy, not a healthcare accuracy benchmark.
    COPILOT_DEFAULT_MODEL: str = "gpt-5.6-terra"
    COPILOT_MEASUREMENT_EXTRACTION_MODEL: str = "gpt-6-luna"
    # None preserves each model's default reasoning, rather than disabling it.
    # Avoid forcing extra effort on every batch without measured quality gains.
    COPILOT_MEASUREMENT_EXTRACTION_REASONING_EFFORT: Literal["low", "medium", "high", "xhigh"] | None = None
    COPILOT_TEXT_EXTRACTION_MODEL: str = "gpt-6-luna"
    COPILOT_TEXT_EXTRACTION_REASONING_EFFORT: Literal["low", "medium", "high", "xhigh"] | None = None
    COPILOT_NORMALIZATION_MODEL: str = "gpt-6-luna"
    COPILOT_NORMALIZATION_REASONING_EFFORT: Literal["low", "medium", "high", "xhigh"] | None = None
    # Token is read from environment – set GITHUB_TOKEN before running
    GITHUB_TOKEN: str = ""

    # Mistral settings. The OCR model handles both document OCR and document
    # annotations; the chat model backs translation and normalization prompts.
    MISTRAL_API_BASE_URL: str = "https://api.mistral.ai"
    MISTRAL_API_KEY: str = ""
    MISTRAL_OCR_MODEL: str = "mistral-ocr-latest"
    MISTRAL_CHAT_MODEL: str = "mistral-large-latest"

    @model_validator(mode="after")
    def _normalize_sqlite_database_urls(self) -> "Settings":
        # Anchor SQLite files to the backend directory so reloads, tests, and
        # alternate launch shells do not silently fork the app onto a fresh DB.
        self.DATABASE_URL = _normalize_sqlite_url(self.DATABASE_URL)
        self.MEDICATIONS_DATABASE_URL = _normalize_sqlite_url(self.MEDICATIONS_DATABASE_URL)
        return self


settings = Settings()
