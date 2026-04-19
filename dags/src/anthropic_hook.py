import os

import anthropic


class AnthropicHook:
    """Airflow hook for Anthropic. Resolves the API key from an Airflow connection
    (password field) with a fallback to ANTHROPIC_API_KEY env var for local runs."""

    def __init__(self, conn_id: str) -> None:
        self.conn_id = conn_id
        self._client: anthropic.Anthropic | None = None

    @property
    def client(self) -> anthropic.Anthropic:
        if self._client is None:
            self._client = anthropic.Anthropic(api_key=self._resolve_api_key())
        return self._client

    def _resolve_api_key(self) -> str:
        try:
            from airflow.hooks.base import BaseHook

            return BaseHook.get_connection(self.conn_id).password
            # return os.environ.get("ANTHROPIC_API_KEY")
        except Exception:
            api_key = os.environ.get("ANTHROPIC_API_KEY")
            if not api_key:
                raise EnvironmentError(
                    f"Could not resolve Airflow connection '{self.conn_id}' and "
                    "ANTHROPIC_API_KEY env var is not set."
                )
            return api_key
