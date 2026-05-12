"""Pull monthly active users from GA4 for a given month."""

import argparse
import calendar
import csv
import json
import re
import sys
from datetime import date

from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Filter,
    FilterExpression,
    FilterExpressionList,
    Metric,
    RunReportRequest,
)
from google.oauth2 import service_account


# GA4 Property ID from https://analytics.google.com/analytics/web/#/a29804340p311611973
PROPERTY_ID = "311611973"


# Sage-owned apex domains (suffix match). Anything not matching is dropped as
# spoofed/scraped GA4 hits. Add new apexes here when Sage stands up new portals.
ALLOW_HOST_PATTERN = re.compile(
    "|".join([
        r"(^|\.)synapse\.org$",
        r"(^|\.)sagebionetworks\.org$",
        r"(^|\.)sageit\.org$",
        r"(^|\.)adknowledgeportal\.org$",
        r"(^|\.)ampadportal\.org$",
        r"(^|\.)nfdataportal\.org$",
        r"(^|\.)eliteportal\.org$",
    ])
)

# Pre-prod subdomains that DO match an allowed apex (e.g. staging.synapse.org)
# but shouldn't pollute production analytics.
PREPROD_HOST_PATTERN = re.compile(
    r"^(staging|tst|dev|staging-signin|cdn-www|dev-signin|portal-dev\.dev)\."
)


GROUP_STRIP_PREFIXES = ("www.", "prod.")


def normalize_hostname(host: str) -> str:
    """Strip leading www./prod. so variants of the same site collapse together."""
    changed = True
    while changed:
        changed = False
        for prefix in GROUP_STRIP_PREFIXES:
            if host.startswith(prefix):
                host = host[len(prefix):]
                changed = True
    return host


def get_client(credentials_path: str) -> BetaAnalyticsDataClient:
    creds = service_account.Credentials.from_service_account_file(
        credentials_path,
        scopes=["https://www.googleapis.com/auth/analytics.readonly"],
    )
    return BetaAnalyticsDataClient(credentials=creds)


def get_monthly_active_users(
    credentials_path: str,
    start_date: str = "2026-03-01",
    end_date: str = "2026-03-31",
    property_id: str = PROPERTY_ID,
) -> dict:
    """Return active users aggregated over the given date range.

    Args:
        credentials_path: Path to GA4 service account JSON file.
        start_date: ISO date string (YYYY-MM-DD).
        end_date: ISO date string (YYYY-MM-DD).
        property_id: GA4 numeric property ID.

    Returns:
        Dict with date range and activeUsers count.
    """
    client = get_client(credentials_path)

    request = RunReportRequest(
        property=f"properties/{property_id}",
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
        metrics=[Metric(name="activeUsers")],
    )

    response = client.run_report(request)

    total_active_users = 0
    for row in response.rows:
        total_active_users += int(row.metric_values[0].value)

    return {
        "property_id": property_id,
        "start_date": start_date,
        "end_date": end_date,
        "active_users": total_active_users,
    }


def _allowed_host_filter() -> FilterExpression:
    """GA4 dimension filter: hostName is in the allowlist and not a pre-prod variant."""
    return FilterExpression(
        and_group=FilterExpressionList(
            expressions=[
                FilterExpression(filter=Filter(
                    field_name="hostName",
                    string_filter=Filter.StringFilter(
                        match_type=Filter.StringFilter.MatchType.PARTIAL_REGEXP,
                        value=ALLOW_HOST_PATTERN.pattern,
                    ),
                )),
                FilterExpression(not_expression=FilterExpression(filter=Filter(
                    field_name="hostName",
                    string_filter=Filter.StringFilter(
                        match_type=Filter.StringFilter.MatchType.PARTIAL_REGEXP,
                        value=PREPROD_HOST_PATTERN.pattern,
                    ),
                ))),
            ]
        )
    )


def get_allowed_active_users(
    credentials_path: str,
    start_date: str = "2026-03-01",
    end_date: str = "2026-03-31",
    property_id: str = PROPERTY_ID,
) -> dict:
    """Active users across Sage-owned domains, deduplicated by GA4.

    Differs from summing per-domain rows: a user who visited synapse.org and
    accounts.synapse.org is counted once here, twice if you sum.
    """
    client = get_client(credentials_path)
    request = RunReportRequest(
        property=f"properties/{property_id}",
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
        metrics=[Metric(name="activeUsers")],
        dimension_filter=_allowed_host_filter(),
    )
    response = client.run_report(request)
    active = int(response.rows[0].metric_values[0].value) if response.rows else 0
    return {
        "property_id": property_id,
        "start_date": start_date,
        "end_date": end_date,
        "active_users": active,
    }


def get_allowed_hostnames(
    credentials_path: str,
    start_date: str,
    end_date: str,
    property_id: str = PROPERTY_ID,
) -> list[str]:
    """Raw GA4 hostnames passing the allow/preprod filter, *without* www./prod. grouping.

    Useful for inspecting what's actually being captured before normalize_hostname
    collapses variants.
    """
    client = get_client(credentials_path)
    request = RunReportRequest(
        property=f"properties/{property_id}",
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
        dimensions=[Dimension(name="hostName")],
        metrics=[Metric(name="activeUsers")],
        dimension_filter=_allowed_host_filter(),
    )
    response = client.run_report(request)
    return sorted(row.dimension_values[0].value for row in response.rows)


def get_active_domains(
    credentials_path: str,
    start_date: str = "2026-01-01",
    end_date: str = "today",
    property_id: str = PROPERTY_ID,
    exclude: bool = True,
    group: bool = True,
) -> list[dict]:
    """Return active users broken down by domain (hostName).

    Args:
        credentials_path: Path to GA4 service account JSON file.
        start_date: ISO date string (YYYY-MM-DD) or relative (e.g. "today").
        end_date: ISO date string (YYYY-MM-DD) or relative (e.g. "today").
        property_id: GA4 numeric property ID.
        exclude: Keep only hostnames matching ALLOW_HOST_PATTERN (Sage-owned
            apex domains) and drop pre-prod subdomains (staging./dev./tst./...).
            Defends against GA4 measurement-ID spoofing by AI-chat scrapers and
            other unrelated sites that copy the public tag.
        group: Collapse www./prod. variants onto the same row by summing
            activeUsers under the normalized hostname.

    Returns:
        List of dicts with hostname and active_users, sorted descending
        by active_users.
    """
    client = get_client(credentials_path)

    request = RunReportRequest(
        property=f"properties/{property_id}",
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
        dimensions=[Dimension(name="hostName")],
        metrics=[Metric(name="activeUsers")],
    )

    response = client.run_report(request)

    rows = [
        {
            "hostname": row.dimension_values[0].value,
            "active_users": int(row.metric_values[0].value),
        }
        for row in response.rows
    ]

    if exclude:
        rows = [
            r for r in rows
            if ALLOW_HOST_PATTERN.search(r["hostname"])
            and not PREPROD_HOST_PATTERN.search(r["hostname"])
        ]

    if not group:
        return sorted(rows, key=lambda r: r["active_users"], reverse=True)

    # Bucket variants under their normalized key.
    variants: dict[str, list[str]] = {}
    for r in rows:
        variants.setdefault(normalize_hostname(r["hostname"]), []).append(r["hostname"])

    # activeUsers is a deduplicated unique count — summing across hostnames would
    # double-count users who hit multiple variants. For multi-variant groups,
    # re-query GA4 with an inListFilter so the API returns a deduplicated count.
    single_user_count = {r["hostname"]: r["active_users"] for r in rows}
    grouped_rows: list[dict] = []
    for key, hostnames in variants.items():
        if len(hostnames) == 1:
            count = single_user_count[hostnames[0]]
        else:
            group_request = RunReportRequest(
                property=f"properties/{property_id}",
                date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
                metrics=[Metric(name="activeUsers")],
                dimension_filter=FilterExpression(
                    filter=Filter(
                        field_name="hostName",
                        in_list_filter=Filter.InListFilter(values=hostnames),
                    )
                ),
            )
            group_response = client.run_report(group_request)
            count = (
                int(group_response.rows[0].metric_values[0].value)
                if group_response.rows
                else 0
            )
        grouped_rows.append({"hostname": key, "active_users": count})

    return sorted(grouped_rows, key=lambda r: r["active_users"], reverse=True)


def last_n_full_months(reference: date, n: int = 12) -> list[tuple[str, str, str]]:
    """Return [(start_date, end_date, "YYYY-MM")] for the n full months before reference's month.

    The current (in-progress) month is excluded so all months returned are complete.
    """
    months: list[tuple[str, str, str]] = []
    for i in range(n):
        m = reference.month - 1 - i
        y = reference.year
        while m <= 0:
            m += 12
            y -= 1
        last_day = calendar.monthrange(y, m)[1]
        months.append((
            f"{y:04d}-{m:02d}-01",
            f"{y:04d}-{m:02d}-{last_day:02d}",
            f"{y:04d}-{m:02d}",
        ))
    months.reverse()
    return months


TOTAL_DOMAIN_LABEL = "(all allowed)"


def get_monthly_active_domains(
    credentials_path: str,
    months: list[tuple[str, str, str]],
    property_id: str = PROPERTY_ID,
    exclude: bool = True,
    group: bool = True,
    include_total: bool = True,
) -> list[dict]:
    """Active users per (month, domain) by calling get_active_domains for each month.

    Args:
        months: List of (start_date, end_date, label) tuples (e.g. from last_n_full_months).
        include_total: If True, prepend a TOTAL_DOMAIN_LABEL row to each month with
            the GA4-deduplicated active-user count across all allowed domains.
        Other args: see get_active_domains.

    Returns:
        Long-format list of dicts: {"month", "domain", "active_users"}, ordered by
        month ascending then active_users descending within each month.
    """
    out: list[dict] = []
    for start, end, label in months:
        if include_total:
            total = get_allowed_active_users(credentials_path, start, end, property_id)
            out.append({
                "month": label,
                "domain": TOTAL_DOMAIN_LABEL,
                "active_users": total["active_users"],
            })
        rows = get_active_domains(
            credentials_path,
            start_date=start,
            end_date=end,
            property_id=property_id,
            exclude=exclude,
            group=group,
        )
        for r in rows:
            out.append({
                "month": label,
                "domain": r["hostname"],
                "active_users": r["active_users"],
            })
    return out


def get_data_catalog_segment(
    credentials_path: str,
    start_date: str = "2026-01-01",
    end_date: str = "today",
    page_path: str = "/DataCatalog:0",
    property_id: str = PROPERTY_ID,
) -> dict:
    """Active users on the DataCatalog page, outbound clicks, and internal navs.

    Three GA4 queries:
      1) activeUsers filtered to pagePath == page_path.
      2) eventCount of `click` events on the page, by linkUrl — outbound
         links only (GA4 Enhanced Measurement's `click` event ignores
         same-domain links).
      3) screenPageViews + activeUsers grouped by pagePath where pageReferrer
         contains page_path — captures same-domain navigations like
         /DataCatalog:0 → /Synapse:syn52623570 that #2 misses. Depends on
         SPA route-change tracking being enabled.

    Args:
        credentials_path: Path to GA4 service account JSON file.
        start_date: ISO date string (YYYY-MM-DD) or relative.
        end_date: ISO date string (YYYY-MM-DD) or relative.
        page_path: Path on www.synapse.org to scope the segment to.
        property_id: GA4 numeric property ID.

    Returns:
        Dict with active_users for the segment and outbound_links list
        (link_url + event_count, sorted desc).
    """
    client = get_client(credentials_path)

    page_filter = FilterExpression(
        filter=Filter(
            field_name="pagePath",
            string_filter=Filter.StringFilter(
                match_type=Filter.StringFilter.MatchType.EXACT,
                value=page_path,
            ),
        )
    )

    users_request = RunReportRequest(
        property=f"properties/{property_id}",
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
        metrics=[Metric(name="activeUsers")],
        dimension_filter=page_filter,
    )
    users_response = client.run_report(users_request)
    active_users = (
        int(users_response.rows[0].metric_values[0].value)
        if users_response.rows
        else 0
    )

    click_filter = FilterExpression(
        and_group=FilterExpressionList(
            expressions=[
                page_filter,
                FilterExpression(
                    filter=Filter(
                        field_name="eventName",
                        string_filter=Filter.StringFilter(
                            match_type=Filter.StringFilter.MatchType.EXACT,
                            value="click",
                        ),
                    )
                ),
            ]
        )
    )

    links_request = RunReportRequest(
        property=f"properties/{property_id}",
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
        dimensions=[Dimension(name="linkUrl")],
        metrics=[Metric(name="eventCount")],
        dimension_filter=click_filter,
    )
    links_response = client.run_report(links_request)

    outbound_links = sorted(
        (
            {
                "link_url": row.dimension_values[0].value,
                "event_count": int(row.metric_values[0].value),
            }
            for row in links_response.rows
        ),
        key=lambda r: r["event_count"],
        reverse=True,
    )

    # Internal navigations: page_view events whose previous page (pageReferrer)
    # was the data catalog. Catches same-domain clicks like /DataCatalog:0 →
    # /Synapse:syn52623570 that GA4's outbound `click` event misses. Requires
    # SPA route-change tracking to be on in Enhanced Measurement.
    referrer_filter = FilterExpression(
        filter=Filter(
            field_name="pageReferrer",
            string_filter=Filter.StringFilter(
                match_type=Filter.StringFilter.MatchType.CONTAINS,
                value=page_path,
            ),
        )
    )

    nav_request = RunReportRequest(
        property=f"properties/{property_id}",
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
        dimensions=[Dimension(name="pagePath")],
        metrics=[Metric(name="screenPageViews"), Metric(name="activeUsers")],
        dimension_filter=referrer_filter,
    )
    nav_response = client.run_report(nav_request)

    internal_navigations = sorted(
        (
            {
                "page_path": row.dimension_values[0].value,
                "page_views": int(row.metric_values[0].value),
                "active_users": int(row.metric_values[1].value),
            }
            for row in nav_response.rows
            if row.dimension_values[0].value != page_path  # exclude self-refresh
        ),
        key=lambda r: r["page_views"],
        reverse=True,
    )

    return {
        "page_path": page_path,
        "start_date": start_date,
        "end_date": end_date,
        "active_users": active_users,
        "outbound_link_count": sum(r["event_count"] for r in outbound_links),
        "outbound_links": outbound_links,
        "internal_navigation_count": sum(r["page_views"] for r in internal_navigations),
        "internal_navigations": internal_navigations,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pull GA4 monthly active users")
    parser.add_argument("credentials", help="Path to service account JSON file")
    parser.add_argument("--start", default="2026-03-01", help="Start date YYYY-MM-DD")
    parser.add_argument("--end", default="2026-03-31", help="End date YYYY-MM-DD")
    parser.add_argument("--property-id", default=PROPERTY_ID)
    parser.add_argument(
        "--domains",
        action="store_true",
        help="Break down by domain (hostName) instead of aggregate",
    )
    parser.add_argument(
        "--monthly-domains",
        action="store_true",
        help="Per-domain active users for each of the last 12 full months, as CSV on stdout",
    )
    parser.add_argument(
        "--data-catalog",
        action="store_true",
        help="Active users + outbound clicks for the DataCatalog page segment",
    )
    parser.add_argument(
        "--page-path",
        default="/DataCatalog:0",
        help="Path used by --data-catalog (default: /DataCatalog:0)",
    )
    args = parser.parse_args()

    if args.data_catalog:
        result = get_data_catalog_segment(
            args.credentials,
            args.start,
            args.end,
            page_path=args.page_path,
            property_id=args.property_id,
        )
        print(json.dumps(result, indent=2))
    elif args.monthly_domains:
        months = last_n_full_months(date.today(), 12)
        rows = get_monthly_active_domains(
            args.credentials, months, args.property_id
        )
        writer = csv.DictWriter(
            sys.stdout, fieldnames=["month", "domain", "active_users"]
        )
        writer.writeheader()
        writer.writerows(rows)

        hosts = get_allowed_hostnames(
            args.credentials, months[0][0], months[-1][1], args.property_id
        )
        print(
            f"\nAllowed hostnames observed {months[0][2]}…{months[-1][2]} "
            f"(raw, pre-grouping, n={len(hosts)}):",
            file=sys.stderr,
        )
        for h in hosts:
            print(f"  {h}", file=sys.stderr)
    elif args.domains:
        rows = get_active_domains(
            args.credentials, args.start, args.end, args.property_id
        )
        print(json.dumps(rows, indent=2))
    else:
        result = get_monthly_active_users(
            args.credentials, args.start, args.end, args.property_id
        )
        print(json.dumps(result, indent=2))
