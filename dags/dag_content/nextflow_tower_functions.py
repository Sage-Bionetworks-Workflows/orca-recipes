from airflow.models import Variable
from sagetasks.nextflowtower.utils import TowerUtils


def create_and_open_tower_workspace(tower_secret_key : str, platform: str, workspace_id: str) -> TowerUtils:
    """Creates and opens Nextflow Tower Workspace using sagetasks TowerUtils class

    Args:
        tower_secret_key (str): Key to the secret that tells you which Nextflow Tower Access Token to run workflow on
        platform (str): Which Nextflow Tower instance to run the workflow on.
        workspace_id (str): The ID number of the workspace you wish to run the workflow in

    Returns:
        tower_utils (TowerUtils): Authenticated Tower Utils class

    """
    client_args = TowerUtils.bundle_client_args(
        auth_token=Variable.get(tower_secret_key), platform=platform
    )
    tower_utils = TowerUtils(client_args)
    tower_utils.open_workspace(workspace_id=workspace_id)
    return tower_utils
