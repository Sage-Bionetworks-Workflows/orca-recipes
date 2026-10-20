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
