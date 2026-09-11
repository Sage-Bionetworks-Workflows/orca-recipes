"""Standalone integration smoke test for SynapseHook.

Fetches the "Sage Bionetworks" Synapse team to verify that SynapseHook can
authenticate and query real team data end-to-end against a real Synapse
account.

This is a plain runnable script, not a pytest test: it hits a real external
service and requires real credentials for `synapse_conn_id`. Its DAG function
is intentionally not named with a `test_` prefix (unlike the file name) so
pytest's `testpaths = tests` config doesn't try to collect and run it as a
real test.

Run directly with: python3 tests/integration/test_synapse_hook.py
"""
import _bootstrap  # noqa: F401  (sets up sys.path for dags.*/src.* imports)
from airflow.models import Param
from airflow.decorators import dag, task
from orca.services.synapse import SynapseHook
from synapseclient.models import Team

dag_params = {
  "synapse_conn_id": Param("SYNAPSE_ORCA_SERVICE_ACCOUNT_CONN", type="string"),
}
dag_config = {
    "schedule": None,
    "params": dag_params,
}

@dag(**dag_config)
def synapse_hook_test_dag():
    """Smoke-test SynapseHook by fetching the "Sage Bionetworks" team.

    DAG Parameters:

    - `synapse_conn_id`: Connection ID for the Synapse service account.
    """

    @task()
    def get_data_from_synapse(**context):
        """Fetch the "Sage Bionetworks" team and sanity-check its data.

        Arguments:
            context: Airflow context dictionary containing DAG parameters
                - synapse_conn_id: Connection ID for Synapse

        Returns:
            None
        """
        hook = SynapseHook(context["params"]["synapse_conn_id"])
        team = Team(name="Sage Bionetworks").get(synapse_client=hook.client)
        # Sanity-check that we actually got real team data back
        assert team.can_public_join is False
        assert team.name == "Sage Bionetworks"
        assert team.can_request_membership is True
    get_data_from_synapse()



dag = synapse_hook_test_dag()

if __name__ == "__main__":
    dag.test()

