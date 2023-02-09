from airflow.models import Variable
from sagetasks.nextflowtower.utils import TowerUtils


def create_and_open_tower_workspace(platform: str, workspace_id: str) -> TowerUtils:
    """Creates and opens Nextflow Tower Workspace using sagetasks TowerUtils class

    Args:
        platform (str): Which Nextflow Tower instance to run the workflow on.
        workspace_id (str): The ID number of the workspace you wish to run the workflow in

    Returns:
        tower_utils (TowerUtils): Authenticated Tower Utils class

    """
    client_args = TowerUtils.bundle_client_args(
        auth_token=Variable.get("TOWER_ACCESS_TOKEN"), platform=platform
    )
    tower_utils = TowerUtils(client_args)
    tower_utils.open_workspace(workspace_id=workspace_id)
    return tower_utils
