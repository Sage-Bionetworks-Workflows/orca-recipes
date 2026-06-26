import os
from typing import Union

import synapseclient
from synapseclient.models import query, SubmissionStatus


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

    def get_submissions_with_status(
        self, submission_view: str, submission_status: str = "RECEIVED"
    ) -> list[str]:
        query(f"select * from {submission_view} limit 1", synapse_client=self.client)
        df = query(
            f"select id from {submission_view} where status = '{submission_status}'",
            synapse_client=self.client,
        )
        return df["id"].tolist()

    def update_submission_status(
        self,
        submission_id: Union[int, str],
        submission_status: str,
    ) -> None:
        if type(submission_id) not in [str, int]:
            raise TypeError("submission_id must be a string or int.")
        sub_status = SubmissionStatus(id=submission_id).get()
        sub_status.status = submission_status
        sub_status.store()

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
