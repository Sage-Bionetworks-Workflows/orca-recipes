from orca.services.nextflowtower import NextflowTowerOps


def monitor_run(ops: NextflowTowerOps, run_id: str):
    """Wait until workflow run completes."""
    from time import sleep

    workflow = ops.get_workflow(run_id)
    print(f"Starting to monitor workflow: {workflow.run_name} ({run_id})")

    status, is_done = ops.get_workflow_status(run_id)
    while not is_done:
        print(f"Not done yet. Status is '{status.value}'. Checking again in 5 min...")
        sleep(60 * 5)
        status, is_done = ops.get_workflow_status(run_id)

    print(f"Done! Final status is '{status.value}'.")
    return status


def configure_logging():
    """Configure logging for Orca and Airflow."""
    import logging

    # Remove timestamps from Orca log statements
    logger = logging.getLogger("orca")
    logger.propagate = False
    logger.handlers.clear()
    handler = logging.StreamHandler()
    formatter = logging.Formatter("%(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    # Silence Airflow logging
    logging.getLogger("airflow").setLevel(logging.ERROR)
