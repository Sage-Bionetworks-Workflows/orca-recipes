import asyncio
import logging

from orca.services.nextflowtower import NextflowTowerOps

logger = logging.getLogger(__name__)


async def monitor_run(ops: NextflowTowerOps, run_id: str):
    """Wait until workflow run completes."""
    workflow = ops.get_workflow(run_id)
    wf_repr = f"{workflow.run_name} ({run_id})"
    logger.info(f"Starting to monitor workflow: {wf_repr}")

    status, is_done = ops.get_workflow_status(run_id)
    while not is_done:
        logger.info(f"{wf_repr} not done yet ({status.value}). Checking again in 5 min...")
        await asyncio.sleep(60 * 5)
        status, is_done = ops.get_workflow_status(run_id)

    logger.info(f"{wf_repr} is done! Final status is '{status.value}'.")
    return status


def configure_logging():
    """Configure logging for Orca and Airflow."""
    # Silence Airflow logging
    logging.getLogger("airflow").setLevel(logging.ERROR)
