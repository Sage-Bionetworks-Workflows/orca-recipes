from typing import Dict, Iterator, List, Optional, Sequence, Set, Tuple

from airflow.models import Variable
from sagetasks.nextflowtower.utils import TowerUtils


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
    stack_name: str,
    workspace_id: str,
) -> Dict[str, Optional[str]]:
    """Gets the latest compute environments in the Nextflow Tower Workspace for a given stack
    and workspace

    Args:
        tower_utils (TowerUtils): Authenticated TowerUtils object that contains tower client for use
        stack_name (str): Name of the Tower stack
        workspace_id (str): The ID number of the workspace you wish to retrieve the compute env for

    Returns:
        compute_env_ids (Dict): Latest SPOT and EC2 compute environments in a dictionary
    """
    compute_env_ids: dict[str, Optional[str]] = {"SPOT": None, "EC2": None}
    endpoint = "/compute-envs"
    params = {"workspaceId": workspace_id}
    response = tower_utils.client.request("GET", endpoint, params=params)

    # figure out highest compute env version by name
    try:
        ce_version = max(
            [
                int(comp_env["name"].split("-")[-1].replace("v", ""))
                for comp_env in response["computeEnvs"]
                if comp_env["platform"] == "aws-batch"
                and comp_env["status"] == "AVAILABLE"
            ]
        )
    except:
        raise ValueError(
            f"Cannot get compute environment version from Tower API request"
        )
    # set env names
    comp_env_spot = f"{stack_name}-spot-v{ce_version}"
    comp_env_ec2 = f"{stack_name}-ondemand-v{ce_version}"

    # check for compute env id
    for comp_env in response["computeEnvs"]:
        if comp_env["platform"] == "aws-batch" and comp_env["status"] == "AVAILABLE":
            if comp_env["name"] == comp_env_spot:
                compute_env_ids["SPOT"] = comp_env["id"]
            elif comp_env["name"] == comp_env_ec2:
                compute_env_ids["EC2"] = comp_env["id"]
    assert (
        compute_env_ids["SPOT"] is not None and compute_env_ids["EC2"] is not None
    ), "You have no available active compute environments for this workspace"
    return compute_env_ids
