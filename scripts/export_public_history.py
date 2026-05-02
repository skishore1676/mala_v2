#!/usr/bin/env python3
"""Export Public.com account history through the official read path.

Requires a Public personal secret token in PUBLIC_SECRET_TOKEN, or enter it at
the prompt. The script mints a short-lived access token, lists accounts, then
pages through each account's history and writes raw JSON plus a flattened CSV.
"""

from __future__ import annotations

import argparse
import csv
import getpass
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests


API_BASE = "https://api.public.com"
DEFAULT_OUTDIR = Path("data/personal_imports/raw_exports")


def mint_access_token(secret: str, validity_minutes: int) -> str:
    response = requests.post(
        f"{API_BASE}/userapiauthservice/personal/access-tokens",
        json={"secret": secret, "validityInMinutes": validity_minutes},
        timeout=30,
    )
    response.raise_for_status()
    access_token = response.json().get("accessToken")
    if not access_token:
        raise RuntimeError("Public API did not return accessToken")
    return access_token


def api_get(path: str, access_token: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    response = requests.get(
        f"{API_BASE}{path}",
        headers={"Authorization": f"Bearer {access_token}"},
        params=params,
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


def list_accounts(access_token: str) -> list[dict[str, Any]]:
    payload = api_get("/userapigateway/trading/account", access_token)
    accounts = payload.get("accounts")
    if not isinstance(accounts, list):
        raise RuntimeError("Public API account response did not contain accounts[]")
    return accounts


def fetch_history(
    access_token: str,
    account_id: str,
    *,
    start: str | None,
    end: str | None,
    page_size: int,
) -> list[dict[str, Any]]:
    transactions: list[dict[str, Any]] = []
    next_token: str | None = None

    while True:
        params: dict[str, Any] = {"pageSize": page_size}
        if start:
            params["start"] = start
        if end:
            params["end"] = end
        if next_token:
            params["nextToken"] = next_token

        payload = api_get(
            f"/userapigateway/trading/{account_id}/history",
            access_token,
            params=params,
        )
        page_transactions = payload.get("transactions", [])
        if not isinstance(page_transactions, list):
            raise RuntimeError(f"Public API history response for {account_id} did not contain transactions[]")

        transactions.extend(page_transactions)
        next_token = payload.get("nextToken")
        if not next_token:
            return transactions


def flatten_transactions(
    accounts: list[dict[str, Any]],
    histories: dict[str, list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    account_by_id = {account.get("accountId"): account for account in accounts}
    rows: list[dict[str, Any]] = []

    for account_id, transactions in histories.items():
        account = account_by_id.get(account_id, {})
        for transaction in transactions:
            row = {
                "accountId": account_id,
                "accountType": account.get("accountType", ""),
                "brokerageAccountType": account.get("brokerageAccountType", ""),
                "optionsLevel": account.get("optionsLevel", ""),
                "tradePermissions": account.get("tradePermissions", ""),
            }
            for key, value in transaction.items():
                if isinstance(value, (dict, list)):
                    row[key] = json.dumps(value, sort_keys=True)
                else:
                    row[key] = value
            rows.append(row)

    return rows


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = sorted({key for row in rows for key in row.keys()})
    preferred = [
        "timestamp",
        "id",
        "accountId",
        "accountNumber",
        "type",
        "subType",
        "symbol",
        "securityType",
        "side",
        "description",
        "quantity",
        "netAmount",
        "principalAmount",
        "fees",
        "direction",
    ]
    ordered = [field for field in preferred if field in fieldnames]
    ordered.extend(field for field in fieldnames if field not in ordered)

    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=ordered)
        writer.writeheader()
        writer.writerows(rows)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export Public.com account history to local JSON and CSV.")
    parser.add_argument("--outdir", type=Path, default=DEFAULT_OUTDIR)
    parser.add_argument("--start", help="ISO 8601 start timestamp, e.g. 2022-01-01T00:00:00Z")
    parser.add_argument("--end", help="ISO 8601 end timestamp, e.g. 2026-04-30T23:59:59Z")
    parser.add_argument("--page-size", type=int, default=500)
    parser.add_argument("--validity-minutes", type=int, default=15)
    parser.add_argument(
        "--account-id",
        action="append",
        help="Specific Public accountId to export. Repeat for multiple accounts. Defaults to all accounts.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    secret = os.environ.get("PUBLIC_SECRET_TOKEN") or getpass.getpass("Public personal secret token: ")
    if not secret:
        raise SystemExit("PUBLIC_SECRET_TOKEN is required")

    access_token = mint_access_token(secret, args.validity_minutes)
    accounts = list_accounts(access_token)

    requested_accounts = set(args.account_id or [])
    accounts_to_export = [
        account for account in accounts if not requested_accounts or account.get("accountId") in requested_accounts
    ]
    if requested_accounts and len(accounts_to_export) != len(requested_accounts):
        found = {account.get("accountId") for account in accounts_to_export}
        missing = sorted(requested_accounts - found)
        raise SystemExit(f"Unknown accountId(s): {', '.join(missing)}")

    histories: dict[str, list[dict[str, Any]]] = {}
    for account in accounts_to_export:
        account_id = account.get("accountId")
        if not account_id:
            continue
        histories[account_id] = fetch_history(
            access_token,
            account_id,
            start=args.start,
            end=args.end,
            page_size=args.page_size,
        )

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    args.outdir.mkdir(parents=True, exist_ok=True)

    raw_path = args.outdir / f"public_history_raw_{stamp}.json"
    csv_path = args.outdir / f"public_history_flat_{stamp}.csv"

    raw_path.write_text(
        json.dumps({"accounts": accounts, "histories": histories}, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    rows = flatten_transactions(accounts, histories)
    write_csv(csv_path, rows)

    print(f"accounts: {len(accounts_to_export)}")
    print(f"transactions: {len(rows)}")
    print(f"raw_json: {raw_path}")
    print(f"csv: {csv_path}")


if __name__ == "__main__":
    main()
