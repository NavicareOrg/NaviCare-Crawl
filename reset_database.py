#!/usr/bin/env python3
"""Utility script to wipe NaviCare Supabase tables."""

import argparse
import asyncio
import logging
from typing import Iterable, Tuple, Union

from dotenv import load_dotenv
from postgrest import APIError

from supabase_client import SupabaseClient


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


DUMMY_UUID = "00000000-0000-0000-0000-000000000000"
DUMMY_TEXT = "__DELETE_ALL__"
DUMMY_INT = -1


ResetStep = Tuple[str, Tuple[str, Union[str, int]]]


CORE_TABLES: Tuple[ResetStep, ...] = (
    ("facility_service_availability", ("id", DUMMY_INT)),
    ("facility_booking_channels", ("id", DUMMY_INT)),
    ("facility_hours", ("id", DUMMY_INT)),
    ("facility_service_offerings", ("facility_id", DUMMY_UUID)),
    ("facility_specialties", ("facility_id", DUMMY_UUID)),
    ("facility_languages", ("facility_id", DUMMY_UUID)),
    ("facility_tags", ("facility_id", DUMMY_UUID)),
    ("user_favorites", ("facility_id", DUMMY_UUID)),
    ("facilities", ("id", DUMMY_UUID)),
)


REFERENCE_TABLES: Tuple[ResetStep, ...] = (
    ("services", ("id", DUMMY_INT)),
    ("specialties", ("id", DUMMY_UUID)),
    ("languages", ("code", DUMMY_TEXT)),
)


def _delete_table_rows(client: SupabaseClient, table: str, column: str, sentinel: Union[str, int]) -> int:
    """Delete all rows from a table using a negative filter."""
    try:
        query = client.client.table(table).delete().neq(column, sentinel)
        response = query.execute()
        deleted_count = len(response.data) if response.data else 0
        logger.info("Cleared %s (deleted %d rows)", table, deleted_count)
        return deleted_count
    except APIError as exc:
        logger.error("Failed to clear %s: %s", table, exc)
        raise


async def reset_database(include_reference: bool = False) -> None:
    """Remove data from Supabase tables in dependency order."""
    load_dotenv()

    client = SupabaseClient()
    if not await client.test_connection():
        raise RuntimeError("Unable to connect to Supabase; check environment variables")

    tables: Iterable[ResetStep] = CORE_TABLES
    if include_reference:
        tables = (*CORE_TABLES, *REFERENCE_TABLES)

    for table, (column, sentinel) in tables:
        _delete_table_rows(client, table, column, sentinel)

    logger.info("Database reset complete")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Reset NaviCare Supabase data")
    parser.add_argument(
        "--include-reference",
        action="store_true",
        help="Also clear reference tables (services, specialties, languages)."
    )
    return parser.parse_args()


async def main() -> None:
    args = parse_args()
    await reset_database(include_reference=args.include_reference)


if __name__ == "__main__":
    asyncio.run(main())