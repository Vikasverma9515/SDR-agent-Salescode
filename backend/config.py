"""
Configuration management via pydantic-settings.
All secrets loaded from .env file. Never hardcoded.

Relative paths (e.g. credentials/..., checkpoints/...) are resolved relative
to the project root — the directory that contains the .env file — rather than
the current working directory. This makes the backend safe to invoke from any
working directory.
"""
from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

# Project root = directory containing this config.py's parent package (backend/)
# i.e.  /path/to/v1_sales_pipeline/
_PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _resolve(path: str) -> str:
    """Return an absolute path, resolving relative paths from project root."""
    p = Path(path)
    if p.is_absolute():
        return str(p)
    resolved = _PROJECT_ROOT / p
    return str(resolved)


class Settings(BaseSettings):
    # Google Sheets
    # Local: path to JSON file (e.g. credentials/google-service-account.json)
    # Cloud: leave as default and set GOOGLE_SERVICE_ACCOUNT_JSON_CONTENT to the full JSON string
    google_service_account_json: str = "credentials/google-service-account.json"
    # Cloud deployment: paste the entire service account JSON as a single-line string
    google_service_account_json_content: str = ""
    spreadsheet_id: str = ""

    # Unipile (LinkedIn API)
    unipile_api_key: str = ""
    unipile_dsn: str = "api21.unipile.com:15157"
    unipile_account_id: str = ""
    unipile_account_ids: str = ""  # comma-separated list of all account IDs for round-robin

    # Search / LLM
    openai_api_key: str = ""
    tavily_api_key: str = ""
    perplexity_api_key: str = ""

    # AWS Bedrock (Claude fallback)
    aws_bearer_token_bedrock: str = ""
    aws_bedrock_region: str = "us-east-1"

    # ZeroBounce
    zerobounce_api_key: str = ""

    # n8n
    n8n_webhook_url: str = ""
    n8n_submission_delay: int = 60

    # LinkedIn (for Sales Navigator scraping via Scrapling)
    # Get this from your browser: DevTools → Application → Cookies → .linkedin.com → li_at
    linkedin_li_at_cookie: str = ""

    # Pipeline
    checkpoint_db: str = "checkpoints/pipeline.db"
    log_dir: str = "logs"
    linkedin_delay: int = 5
    zerobounce_credit_warning: int = 50

    # UI Server
    ui_host: str = "0.0.0.0"
    ui_port: int = 8080

    # CORS — comma-separated list of allowed origins (defaults to * for dev)
    # Example for production: https://yourapp.vercel.app,https://yourapp.com
    allowed_origins: str = "*"

    model_config = SettingsConfigDict(
        env_file=str(_PROJECT_ROOT / ".env"),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    @property
    def google_service_account_json_abs(self) -> str:
        """Absolute path to the service account JSON, resolved from project root."""
        return _resolve(self.google_service_account_json)

    @property
    def checkpoint_db_abs(self) -> str:
        """Absolute path to the checkpoint DB, resolved from project root."""
        return _resolve(self.checkpoint_db)

    @property
    def log_dir_abs(self) -> str:
        """Absolute path to the log directory, resolved from project root."""
        return _resolve(self.log_dir)


@lru_cache
def get_settings() -> Settings:
    return Settings()

