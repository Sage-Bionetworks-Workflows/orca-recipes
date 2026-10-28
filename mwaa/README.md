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
- **Dev from a pull request**: add the `deploy-to-dev` label to a pull request into
  `main`. Its head commit deploys to dev, and so does every later push while the label is
  on. When you are done testing, remove the label and run the workflow with `main`. Pull
  requests from forks are not deployed.

Before anything is copied, every DAG must import under the MWAA Airflow and Python
versions (`tests/dags/test_all_dags.py`). The deployed ref, commit, and who deployed it
are written to `dags/.deployed-ref` in the bucket and to the run summary.

Each GitHub environment, `mwaa-dev` and `mwaa-prod`, holds two variables, copied from
the eks-stack MWAA stack outputs: `MWAA_BUCKET` and `MWAA_ROLE_ARN`. The deploy is
skipped while either is empty.

New DAGs are paused when MWAA first sees them. Unpause them in the Airflow UI, opened
as described in eks-stack's
[Accessing the Airflow UI](https://github.com/Sage-Bionetworks-Workflows/eks-stack/blob/main/modules/mwaa/README.md#accessing-the-airflow-ui).

### Before you deploy to dev

Dev has one copy of `dags/`. Each deploy replaces all of it, including the DAGs of a
branch that another developer is testing.

1. In the Actions tab, open the most recent "Deploy DAGs to MWAA" run on dev and read
   its summary. It shows the ref that is on dev and who deployed it.
2. If the ref is not `main` or your branch, ask in the team channel before you deploy
   or add the label.

### Rolling back

To remove a bad change from prod,
[revert its pull request](https://docs.github.com/en/pull-requests/how-tos/merge-and-close-pull-requests/reverting-a-pull-request).
The push to `main` deploys the reverted DAGs. To roll back dependencies, revert the SHA
bump (PR or commit) in eks-stack.

## Changing dependencies

A DAG that needs a new or changed package must not merge before MWAA has the package.
The DAG deploy is immediate, but MWAA installs dependencies only from the commit that
eks-stack pins. You need two pull requests in this repo: a dependency pull request with
only the dependency change, and a DAG pull request with the DAG changes. The DAG pull
request must also contain the dependency change, because the deploy workflow checks DAG
imports with the `requirements-airflow.txt` of the deployed commit.

Before you start, make sure the DAG pull request has been reviewed and its DAGs run
locally with the new dependency. Each pass through these steps waits for a 20–30 minute
MWAA update, so a wrong dependency is slow to fix.

1. Open the dependency pull request that changes `requirements-airflow.txt` (and
   `mwaa/constraints.txt` if needed). Check it as described in
   [Changing a dependency](#changing-a-dependency-in-requirements-airflowtxt).
2. In the [eks-stack repo](https://github.com/Sage-Bionetworks-Workflows/eks-stack), open
   a pull request that sets `mwaa_orca_recipes_sha` in `deployments/main.tf` to the latest
   commit of the dependency pull request branch, and merge it. The MWAA stacks only get
   the SHA from eks-stack `main`.
3. In Spacelift, confirm the dev MWAA stack run only. The update takes 20–30 minutes.
   Check the `requirements_install_*` log stream in the dev Scheduler log group.
4. Deploy the DAG pull request to dev and test the DAGs that use the change.
5. Merge the dependency pull request with a merge commit. Its commit is then on `main`,
   so the pinned SHA stays valid. If you squash or rebase instead, the merged commit is
   new: set `mwaa_orca_recipes_sha` to it through another eks-stack pull request and
   confirm the dev run again.
6. Confirm the prod MWAA stack run. The prod plan fails while the SHA is not on `main`.
7. Merge the DAG pull request.

## How dependencies are installed

MWAA installs `requirements-airflow.txt` from the root of this repo, the same file used
by CI. eks-stack pins a commit SHA of this repo, downloads
`requirements-airflow.txt` at that SHA, adds a `--constraint` line pointing at
`mwaa/constraints.txt` at the same SHA, and uploads the result to the MWAA bucket as
`requirements.txt`. A dependency change reaches MWAA only when that SHA is bumped in
eks-stack.

MWAA always installs with a constraints file. If `requirements.txt` has none, MWAA
uses the upstream Airflow constraints, which conflict with `requirements-airflow.txt` and
pin packages that have vulnerability alerts. That is why this repo ships its own.

## `constraints.txt`

The upstream Airflow constraints for the MWAA Airflow and Python versions, with a small
number of pins overridden. The header of the file contains a table that lists every override with the
upstream version, the version used here, and why. Every overridden line ends in
`# override`:

```bash
grep override mwaa/constraints.txt
```

### Vulnerability alerts

Dependabot does not read `constraints.txt`. On every push to `main` that changes it or
`requirements-airflow.txt`, `.github/workflows/mwaa-dependency-submission.yml` resolves
the packages MWAA installs and submits them to the dependency graph, so Dependabot
raises alerts for them under `mwaa/constraints.txt`. Fix an alert by overriding the pin
as described in [Overriding a pin](#overriding-a-pin).

### Changing a dependency in `requirements-airflow.txt`

1. Make the change. Do not pin `apache-airflow`.
2. Find the package in `mwaa/constraints.txt`:
   - Not listed, or listed at a version inside your new range: leave
     `mwaa/constraints.txt` unchanged.
   - Listed at a version outside your new range: override that pin as described in
     [Overriding a pin](#overriding-a-pin).
3. Check it resolves against our custom MWAA constraints file:

   ```bash
   uv venv -p 3.12 /tmp/mwaa-check
   uv pip install -p /tmp/mwaa-check/bin/python pip \
     "apache-airflow[amazon,celery,snowflake]==3.3.1" \
     -c https://raw.githubusercontent.com/apache/airflow/constraints-3.3.1/constraints-3.12.txt
   /tmp/mwaa-check/bin/python -m pip install -r requirements-airflow.txt -c mwaa/constraints.txt
   /tmp/mwaa-check/bin/python -m pip check
   ```

4. If pip reports a conflict with another constraint, override that pin too. Prefer
   the lowest version that resolves the conflict.

Once this resolves cleanly, continue with the deployment steps in
[Changing dependencies](#changing-dependencies).

### Overriding a pin

1. Change the version on the package's line and append `  # override`.
2. Add a row to the Overrides table in the header: package, upstream version, new
   version, and the requirement that forces it.
3. Re-run the [check](#changing-a-dependency-in-requirements-airflowtxt).

### Upgrading the MWAA Airflow version

1. Pick the Airflow version and its Python version from the
   [MWAA supported versions](https://docs.aws.amazon.com/mwaa/latest/userguide/airflow-versions.html).
2. Download the new upstream file:
   `https://raw.githubusercontent.com/apache/airflow/constraints-<airflow>/constraints-<python>.txt`
3. Replace everything below the `Upstream Airflow constraints file below` line with it,
   and update the `Source:` URL and versions in the header.
4. For each row in the Overrides table, set the package's line to the version in the
   `Here` column and append `  # override`. If the new upstream version already
   satisfies the `Reason`, keep the upstream version and remove the row.
5. Re-run the [check](#changing-a-dependency-in-requirements-airflowtxt) with the new
   Airflow and Python versions.
6. Update the versions written out in docs and comments (`git grep` for the old
   versions). The workflows and `scripts/install_local_airflow.sh` read the versions from
   the `Source:` line in `mwaa/constraints.txt` with `scripts/mwaa_versions.sh`.
   `mwaa-dags.yml` parses the line itself, because it runs from `main` against deployed
   commits that may not have the script. Keep the line in this format:
   `# Source: https://raw.githubusercontent.com/apache/airflow/constraints-<airflow>/constraints-<python>.txt`
7. Bump the airflow version and the pinned SHA of this repo together in eks-stack.
