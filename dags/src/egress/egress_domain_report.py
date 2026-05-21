"""Gmail-user company -> domain resolution.

Pulls all Synapse users with a `@gmail.com` email and a non-null company string,
asks Claude (via Anthropic batch) for the actual organizational domain of each
company, and writes a (user_id, email, company, resolved_domain) lookup table
back to Snowflake. Downstream aggregation against download events lives in
Snowflake as a regular SQL query against that table.
"""

import logging
import re
from typing import TYPE_CHECKING

import anthropic
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas

if TYPE_CHECKING:
    import snowflake.connector

logger = logging.getLogger(__name__)

# Mirrors the Snowflake REGEXP_LIKE check used to gate the original AI_COMPLETE
# call: only ask the model about company strings that look like plausible org
# names.
_COMPANY_ALLOWED_RE = re.compile(r"^[\w\s\-.,&/()]+$")
_COMPANY_STRIP_RE = re.compile(r"[^\w\s\-.,&/()]")

# Cheap classification task -> Haiku.
_MODEL = "claude-haiku-4-5-20251001"


def extract_gmail_users_with_company(
    conn: "snowflake.connector.SnowflakeConnection",
) -> pd.DataFrame:
    """All gmail-registered Synapse users that have a non-null company string."""
    sql = """
SELECT
    id AS user_id,
    email,
    company
FROM synapse_data_warehouse.synapse.userprofile_latest
WHERE email LIKE '%@gmail.com'
    AND company IS NOT NULL
;
"""
    with conn.cursor() as cs:
        cs.execute(sql)
        df = cs.fetch_pandas_all()
    df.columns = [c.lower() for c in df.columns]
    df.drop_duplicates(subset=["user_id"], inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df


def _is_resolvable_company(company: object) -> bool:
    """Mirror the Snowflake gate: non-empty, 2-199 chars, allowed-char regex."""
    if not isinstance(company, str):
        return False
    trimmed = company.strip()
    if not (1 < len(trimmed) < 200):
        return False
    return bool(_COMPANY_ALLOWED_RE.match(trimmed))


def _build_domain_prompt(company: str) -> str:
    cleaned = _COMPANY_STRIP_RE.sub("", company.strip())
    return (
        "What is the actual website domain of this organization? "
        "Return ONLY the domain (e.g. chalmers.se, mit.edu, pfizer.com, nih.gov, "
        "yarsi.ac.id). No explanation. If unknown return NULL. "
        f"Organization: {cleaned}"
    )


def submit_domain_resolution_batch(
    df: pd.DataFrame,
    anthropic_client: anthropic.Anthropic,
) -> str:
    """Submit one Anthropic batch request per user with a resolvable company.

    Returns the batch ID, or empty string if no rows qualify.
    """
    eligible_idx = [idx for idx in df.index if _is_resolvable_company(df.at[idx, "company"])]
    if not eligible_idx:
        logger.info("No users with resolvable company strings; skipping batch.")
        return ""

    logger.info("Submitting domain resolution batch for %d users.", len(eligible_idx))
    requests = []
    for idx in eligible_idx:
        prompt = _build_domain_prompt(str(df.at[idx, "company"]))
        requests.append(
            anthropic.types.message_create_params.MessageCreateParamsNonStreaming(
                custom_id=str(idx),
                params={
                    "model": _MODEL,
                    "max_tokens": 64,
                    "messages": [{"role": "user", "content": prompt}],
                },
            )
        )

    batch = anthropic_client.messages.batches.create(requests=requests)
    logger.info("Submitted batch %s with %d requests.", batch.id, len(requests))
    return batch.id


def _parse_domain_response(text: str) -> str | None:
    """Pull a domain out of Claude's response. Return None if NULL/unparseable."""
    raw = text.strip().strip("`").strip()
    if not raw or raw.upper() == "NULL":
        return None
    token = raw.split()[0].strip(".,;:'\"")
    if not token or "." not in token:
        return None
    return token.lower()


def collect_domain_resolution_results(
    df: pd.DataFrame,
    batch_id: str,
    anthropic_client: anthropic.Anthropic,
) -> pd.DataFrame:
    """Add a `resolved_domain` column to df based on batch results.

    Users whose company didn't qualify, or whose batch entry failed/returned
    NULL, are left with `resolved_domain = None` — the downstream Snowflake
    query is responsible for the gmail.com fallback (e.g. via COALESCE).
    """
    df = df.copy()
    df["resolved_domain"] = None

    if not batch_id:
        return df

    for result in anthropic_client.messages.batches.results(batch_id):
        idx = int(result.custom_id)
        if result.result.type != "succeeded":
            logger.warning(
                "Domain resolution failed for user_id=%s: %s",
                df.at[idx, "user_id"],
                result.result.error,
            )
            continue
        text = next(
            (b.text for b in result.result.message.content if b.type == "text"),
            "",
        )
        domain = _parse_domain_response(text)
        if domain:
            df.at[idx, "resolved_domain"] = domain
        else:
            logger.info(
                "No domain resolved for user_id=%s company=%r (response=%r)",
                df.at[idx, "user_id"],
                df.at[idx, "company"],
                text[:100],
            )

    return df


def load_to_snowflake(
    df: pd.DataFrame,
    conn: "snowflake.connector.SnowflakeConnection",
    database: str,
    schema: str,
    table: str,
) -> int:
    """Overwrite the target Snowflake table with the resolved-domain lookup."""
    upper = df.copy()
    upper.columns = [c.upper() for c in upper.columns]
    success, _, nrows, _ = write_pandas(
        conn,
        upper,
        table,
        database=database,
        schema=schema,
        auto_create_table=True,
        overwrite=True,
        quote_identifiers=False,
    )
    if not success:
        raise RuntimeError(
            f"write_pandas reported failure for {database}.{schema}.{table}"
        )
    return nrows
