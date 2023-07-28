#!/usr/bin/env python3

import sys

from orca.services.nextflowtower import NextflowTowerOps
from orca.services.nextflowtower.models import LaunchInfo

from utils import monitor_run, configure_logging


def nextflow_execute_workflow(run_name: str, s3_file: str, cwl_file: str):
    """Launches nf-synstage workflow in Nextflow Tower"""
    ops = NextflowTowerOps()
    info = LaunchInfo(
        run_name=run_name,
        pipeline="https://github.com/Sage-Bionetworks-Workflows/nf-cwl-wrap",
        revision="main",
        params={
            "s3_file": s3_file,
            "cwl_file": cwl_file,
        },
    )
    run_id = ops.launch_workflow(info, "ondemand")
    monitor_run(ops, run_id)


def main():
    run_name = sys.argv[1]  # "immune_subtype_classifier_execution"
    s3_file = sys.argv[
        2
    ]  # "s3://iatlas-project-tower-bucket/immune_subtype_classifier/synstage/input.csv"
    cwl_file = sys.argv[
        3
    ]  # "https://raw.githubusercontent.com/CRI-iAtlas/iatlas-workflows/d94151d3aaadee96b52f27e4ce84692c0366fe75/Immune_Subtype_Classifier/workflow/steps/immune_subtype_classifier/immune_subtype_classifier.cwl"
    configure_logging()
    nextflow_execute_workflow(run_name=run_name, s3_file=s3_file, cwl_file=cwl_file)


if __name__ == "__main__":
    main()
