# MWAA

Files used by the Amazon MWAA (Managed Airflow) environments. The environments are
created by [eks-stack](https://github.com/Sage-Bionetworks-Workflows/eks-stack)
(`modules/mwaa`).

## Deploying DAGs

`.github/workflows/mwaa-dags.yml` copies `dags/` to the `dags/` prefix of the MWAA
bucket with `aws s3 sync --delete`, so the bucket matches the deployed commit exactly.

- **Prod**: every push to `main` deploys to MWAA prod.
- **Dev**: run the workflow from the Actions tab ("Deploy DAGs to MWAA" > Run workflow)
  and enter a branch, tag or commit SHA of this repo. Leave "Use workflow from" on
  `main`. Dev is shared and keeps whatever was deployed last, so run it with `main` when
  you are done testing.

Before anything is copied, every DAG must import under the MWAA Airflow and Python
versions (`tests/dags/test_all_dags.py`). The deployed ref, commit, and who deployed it
are written to `dags/.deployed-ref` in the bucket and to the run summary.

Each GitHub environment, `mwaa-dev` and `mwaa-prod`, holds two variables, copied from
the eks-stack MWAA stack outputs: `MWAA_BUCKET` and `MWAA_ROLE_ARN`. The deploy is
skipped while either is empty.

New DAGs are paused when MWAA first sees them. Unpause them in the Airflow UI.

### Before you deploy to dev

Dev has one copy of `dags/`. Each deploy replaces all of it, including the DAGs of a
branch that another developer is testing.

1. In the Actions tab, open the most recent "Deploy DAGs to MWAA" run on dev and read
   its summary. It shows the ref that is on dev and who deployed it.
2. If the ref is not `main` or your branch, ask in the team channel before you deploy.

The challenge DAGs read `dags/challenge_configs.yaml` from `main` on GitHub, not from
the bucket. Changes to that file on a branch do not show on dev until they merge.

### Rolling back

To remove a bad change from prod, revert its pull request. The push to `main` deploys
the reverted DAGs. To roll back dependencies, revert the SHA bump in eks-stack.

## Changing dependencies

A DAG that needs a new or changed package must not merge before MWAA has the package.
The DAG deploy is immediate, but MWAA installs dependencies only from the commit that
eks-stack pins. Put the dependency change in its own pull request:

1. Open a pull request in this repo that changes `requirements-airflow.txt` (and
   `mwaa/constraints.txt` if needed). Check it as described in
   [Changing a dependency](#changing-a-dependency-in-requirements-airflowtxt).
2. In eks-stack, set `mwaa_orca_recipes_sha` in `deployments/main.tf` to the latest
   commit of the pull request branch, and merge it.
3. In Spacelift, confirm the dev MWAA stack run only. The update takes 20–30 minutes.
   Check the `requirements_install_*` log stream in the dev Scheduler log group.
4. Deploy your branch to dev and test the DAGs that use the change.
5. Merge the pull request. A squash or rebase merge makes a new commit, so set
   `mwaa_orca_recipes_sha` to the new commit on `main`, merge it, and confirm the dev
   run again.
6. Confirm the prod MWAA stack run. The prod plan fails while the SHA is not on `main`.
7. Merge the pull request with the DAG changes that use the new dependency.

## How dependencies are installed

MWAA installs `requirements-airflow.txt` from the root of this repo, the same file used
by the EKS Airflow image and CI. eks-stack pins a commit SHA of this repo, downloads
`requirements-airflow.txt` at that SHA, adds a `--constraint` line pointing at
`mwaa/constraints.txt` at the same SHA, and uploads the result to the MWAA bucket as
`requirements.txt`. A dependency change reaches MWAA only when that SHA is bumped in
eks-stack.

MWAA always installs with a constraints file. If `requirements.txt` has none, MWAA
uses the upstream Airflow constraints, which cannot install synapseclient 4.14. That
is why this repo ships its own.

## `constraints.txt`

The upstream Airflow constraints for the MWAA Airflow and Python versions, with a small
number of pins overridden. The header of the file lists every override with the
upstream version, the version used here, and why. Every overridden line ends in
`# override`:

```bash
grep override mwaa/constraints.txt
```

### Changing a dependency in `requirements-airflow.txt`

1. Make the change. Version ranges must resolve under both the EKS image
   (Airflow 2.10.5, Python 3.10, no constraints) and MWAA (Airflow 2.11.2,
   Python 3.12, `mwaa/constraints.txt`). Do not pin `apache-airflow`.
2. Check it resolves against MWAA's constraints:

   ```bash
   uv venv -p 3.12 /tmp/mwaa-check
   uv pip install -p /tmp/mwaa-check/bin/python pip \
     "apache-airflow[amazon,celery,snowflake]==2.11.2" \
     -c https://raw.githubusercontent.com/apache/airflow/constraints-2.11.2/constraints-3.12.txt
   /tmp/mwaa-check/bin/python -m pip install -r requirements-airflow.txt -c mwaa/constraints.txt
   /tmp/mwaa-check/bin/python -m pip check
   ```

3. If pip reports a conflict with a constraint, override that pin as described
   below. Prefer the lowest version that resolves the conflict.

### Overriding a pin

1. Change the version on the package's line and append `  # override`.
2. Add a row to the table in the header: package, upstream version, new version,
   and the requirement that forces it.
3. Re-run the check above.

### Upgrading the MWAA Airflow version

1. Download the new upstream file:
   `https://raw.githubusercontent.com/apache/airflow/constraints-<airflow>/constraints-<python>.txt`
2. Replace everything below the header with it, and update the `Source:` URL and
   versions in the header.
3. Re-apply each override in the header table. Drop any whose upstream version now
   satisfies the reason given, and remove its row.
4. Re-run the check above with the new Airflow and Python versions.
5. Bump the airflow version and the pinned SHA of this repo together in eks-stack.
