from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    DATABASE_URL: str = "sqlite+aiosqlite:///./data/health.db"
    UPLOAD_DIR: str = str(Path(__file__).resolve().parent.parent / "data" / "uploads")

    # GitHub Copilot SDK settings
    COPILOT_MODEL: str = "gpt-5.4"
    # Token is read from environment – set GITHUB_TOKEN before running
    GITHUB_TOKEN: str = ""


settings = Settings()
