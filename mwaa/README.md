# MWAA

Files used by the Amazon MWAA (Managed Airflow) environments. The environments are
created by [eks-stack](https://github.com/Sage-Bionetworks-Workflows/eks-stack)
(`modules/mwaa`).

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
number of pins overridden. The header of the file contains a table that lists every override with the
upstream version, the version used here, and why. Every overridden line ends in
`# override`:

```bash
grep override mwaa/constraints.txt
```

### Changing a dependency in `requirements-airflow.txt`

1. Make the change. Until the migration to MWAA is complete, version ranges must
   resolve under both the EKS image (Airflow 2.10.5, Python 3.10, no constraints) and
   MWAA (Airflow 2.11.2, Python 3.12, `mwaa/constraints.txt`). Do not pin
   `apache-airflow`.
2. Find the package in `mwaa/constraints.txt`:
   - Not listed, or listed at a version inside your new range: leave
     `mwaa/constraints.txt` unchanged.
   - Listed at a version outside your new range: override that pin as described in
     [Overriding a pin](#overriding-a-pin).
3. Check it resolves against our custom MWAA constraints file:

   ```bash
   uv venv -p 3.12 /tmp/mwaa-check
   uv pip install -p /tmp/mwaa-check/bin/python pip \
     "apache-airflow[amazon,celery,snowflake]==2.11.2" \
     -c https://raw.githubusercontent.com/apache/airflow/constraints-2.11.2/constraints-3.12.txt
   /tmp/mwaa-check/bin/python -m pip install -r requirements-airflow.txt -c mwaa/constraints.txt
   /tmp/mwaa-check/bin/python -m pip check
   ```

4. If pip reports a conflict with another constraint, override that pin too. Prefer
   the lowest version that resolves the conflict.

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
6. Bump the airflow version and the pinned SHA of this repo together in eks-stack.
