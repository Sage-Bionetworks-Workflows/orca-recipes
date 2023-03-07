from typing import Dict, Optional

from airflow.models import Variable
from sagetasks.nextflowtower.utils import TowerUtils

# add new compute env models here
COMPUTE_ENV_MODELS = {"EC2": "ondemand", "SPOT": "spot"}

def create_and_open_tower_workspace(
    tower_secret_key: str, platform: str, workspace_id: str
) -> TowerUtils:
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


def get_latest_compute_environment(
    tower_utils: TowerUtils,
    compute_env_model: str,
    stack_name: str,
    workspace_id: str,
) -> str:
    """Gets the latest compute environment in the Nextflow Tower Workspace for given stack

    Args:
        tower_utils (TowerUtils): Authenticated TowerUtils object that contains tower client for use
        compute_env_model (str): Short name of the compute environment model to use
        stack_name (str): Name of the Tower stack
        workspace_id (str): The ID number of the workspace you wish to retrieve the env for

    Returns:
        compute_env_id (str): Latest compute environment id for the given args specified
    """
    available_envs = get_available_compute_environments(
        tower_utils=tower_utils, workspace_id=workspace_id
    )
    compute_env_prefix = f"{stack_name}-{COMPUTE_ENV_MODELS[compute_env_model]}-v"
    # gets the latest compute environment version number
    try:
        max_ce_version = max(
            list(
                set(
                    [
                        int(comp_env.split("-")[-1].replace("v", ""))
                        for comp_env in available_envs.keys()
                        if comp_env.startswith(compute_env_prefix)
                    ]
                )
            )
        )
    except Exception as error:
        message = "Cannot get compute environment version from Tower API request"
        raise ValueError(message) from error

    compute_env_name = (
        f"{stack_name}-{COMPUTE_ENV_MODELS[compute_env_model]}-v{max_ce_version}"
    )

    try:
        compute_env_id = available_envs[compute_env_name]
    except Exception as error:
        message = (
            f"{compute_env_name} doesn't exist as an available compute environment. "
            f"See available compute environments here:{list(available_envs.keys())}"
        )
        raise KeyError(message) from error
    return compute_env_id


def get_available_compute_environments(
    tower_utils: TowerUtils,
    workspace_id: str,
) -> dict:
    """Gets the available compute environments for the given workspace

    Args:
        tower_utils (TowerUtils): Authenticated TowerUtils object that contains tower client for use
        workspace_id (str):  The ID number of the workspace you wish to retrieve the env for

    Returns:
       dict: dictionary where the keys are the compute environment names and the values are the compute environment ids
    """
    endpoint = "/compute-envs"
    params = {"workspaceId": workspace_id, "status": "AVAILABLE"}
    response = tower_utils.client.request("GET", endpoint, params=params)
    # filter for requirements in env
    available_envs = {
        comp_env["name"]: comp_env["id"]
        for comp_env in response["computeEnvs"]
        if comp_env["platform"] == "aws-batch"
    }
    return available_envs
