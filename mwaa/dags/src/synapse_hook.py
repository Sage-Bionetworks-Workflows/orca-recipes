import os

import synapseclient


class SynapseHook:
    """Airflow hook for Synapse. Resolves credentials from an Airflow connection
    (password field = auth token) with a fallback to SYNAPSE_AUTH_TOKEN env var
    for local runs."""

    def __init__(self, conn_id: str) -> None:
        self.conn_id = conn_id
        self._client: synapseclient.Synapse | None = None

    @property
    def client(self) -> synapseclient.Synapse:
        if self._client is None:
            self._client = self._login()
        return self._client

    def _login(self) -> synapseclient.Synapse:
        auth_token = self._resolve_token()
        syn = synapseclient.Synapse()
        syn.login(authToken=auth_token, silent=True)
        return syn

    def _resolve_token(self) -> str:
        try:
            from airflow.hooks.base import BaseHook

            return BaseHook.get_connection(self.conn_id).password
        except Exception:
            token = os.environ.get("SYNAPSE_AUTH_TOKEN")
            if not token:
                raise EnvironmentError(
                    f"Could not resolve Airflow connection '{self.conn_id}' and "
                    "SYNAPSE_AUTH_TOKEN env var is not set."
                )
            return token
