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
    config_dict = {
        #"sage": {
        # "token": Variable.get("TOWER_ACCESS_TOKEN"),
        # } placeholder for prod platform, will need refactor with migration
        "sage-dev": {
            "token": Variable.get("TOWER_ACCESS_TOKEN"),
        }
    }

    if platform not in config_dict.keys():
        raise KeyError(f"Platform '{platform}' is not currently supported. Please select from {str(list(config_dict.keys()))}")
    
    client_args = TowerUtils.bundle_client_args(
        auth_token=config_dict[platform]["token"], platform=platform
    )
    tower_utils = TowerUtils(client_args)
    tower_utils.open_workspace(workspace_id=workspace_id)
    return tower_utils
