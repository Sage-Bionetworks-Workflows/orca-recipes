#!/usr/bin/env python3

import sys

from orca.services.nextflowtower import NextflowTowerOps
from orca.services.nextflowtower.models import LaunchInfo

from utils import monitor_run, configure_logging


def nextflow_stage_data(run_name: str, input: str, outdir: str):
    """Launches nf-synstage workflow in Nextflow Tower"""
    ops = NextflowTowerOps()
    info = LaunchInfo(
        run_name=run_name,
        pipeline="https://github.com/Sage-Bionetworks-Workflows/nf-synstage",
        revision="main",
        workspace_secrets=["SYNAPSE_AUTH_TOKEN"],
        params={
            "input": input,
            "outdir": outdir,
        },
    )
    run_id = ops.launch_workflow(info, "ondemand")
    monitor_run(ops, run_id)


def main():
    run_name = sys.argv[1]  # "immune_subtype_classifier_staging"
    input = sys.argv[
        2
    ]  # "s3://iatlas-project-tower-bucket/immune_subtype_classifier/input.csv"
    outdir = sys.argv[
        3
    ]  # "s3://iatlas-project-tower-bucket/immune_subtype_classifier/"
    configure_logging()
    nextflow_stage_data(run_name=run_name, input=input, outdir=outdir)


if __name__ == "__main__":
    main()
