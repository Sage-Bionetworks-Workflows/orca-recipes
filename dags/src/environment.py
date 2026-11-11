"""Choose per-environment values in DAGs.

The MWAA environments set AIRFLOW__ORCA__ENVIRONMENT (from eks-stack's
`deployment_environment`) to "dev" or "prod". Unset means "dev", so code that
runs anywhere else never touches production.
"""

import os
from typing import TypeVar

T = TypeVar("T")

ENVIRONMENT = os.environ.get("AIRFLOW__ORCA__ENVIRONMENT", "dev")


def by_environment(*, dev: T, prod: T, **others: T) -> T:
    """Return the value for ENVIRONMENT, or the dev value if it has none.

    Pass other environments by name, e.g. `by_environment(dev=1, prod=2, staging=3)`.
    """
    return {"dev": dev, "prod": prod, **others}.get(ENVIRONMENT, dev)
