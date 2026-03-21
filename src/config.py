"""
Configuration management via pydantic-settings.
All secrets loaded from .env file. Never hardcoded.
"""
from functools import lru_cache
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # Google Sheets
    google_service_account_json: str = "credentials/google-service-account.json"
    spreadsheet_id: str = ""

    # Unipile (LinkedIn API)
    unipile_api_key: str = ""
    unipile_dsn: str = "api21.unipile.com:15157"
    unipile_account_id: str = ""

    # Search
    openai_api_key: str = ""
    tavily_api_key: str = ""
    perplexity_api_key: str = ""

    # ZeroBounce
    zerobounce_api_key: str = ""

    # n8n
    n8n_webhook_url: str = ""
    n8n_submission_delay: int = 60

    # Pipeline
    checkpoint_db: str = "checkpoints/pipeline.db"
    log_dir: str = "logs"
    linkedin_delay: int = 5
    zerobounce_credit_warning: int = 50

    # UI Server
    ui_host: str = "0.0.0.0"
    ui_port: int = 8080

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


@lru_cache
def get_settings() -> Settings:
    return Settings()
