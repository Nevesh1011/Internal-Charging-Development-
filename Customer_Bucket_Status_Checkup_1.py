#!/usr/bin/env python3
import os
import sys
import argparse
import logging
from datetime import datetime

import psycopg
from psycopg.rows import dict_row


# ---------------------------------------------------------------------
# DATABASE CONFIGURATION
# ---------------------------------------------------------------------
DB_HOST = os.getenv("db_host", "127.0.0.1")
DB_PORT = int(os.getenv("db_port", "5432"))
DB_NAME = os.getenv("db_name", "")
DB_USER = os.getenv("db_user", "")
DB_PASSWORD = os.getenv("db_password", "")
DB_SCHEMA = os.getenv("DB_SCHEMA", "public")


# ---------------------------------------------------------------------
# LOG CONFIGURATION
# ---------------------------------------------------------------------
LOG_DIR = os.getenv("BUCKET_LOG_DIR", "/home/edgerating/bucket_logs/bucket_status_logs")
os.makedirs(LOG_DIR, exist_ok=True)

RUN_TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
LOG_FILE = os.path.join(
    LOG_DIR,
    f"customer_bucket_expiry_{RUN_TIMESTAMP}.log"
)

logger = logging.getLogger("customer_bucket_expiry")
logger.setLevel(logging.INFO)
logger.handlers.clear()

formatter = logging.Formatter(
    "%(asctime)s | %(levelname)s | %(message)s",
    "%Y-%m-%d %H:%M:%S"
)

file_handler = logging.FileHandler(LOG_FILE)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)


# ---------------------------------------------------------------------
# DATABASE CONNECTION
# ---------------------------------------------------------------------
def get_connection():
    if not DB_NAME or not DB_USER:
        raise RuntimeError(
            "db_name/db_user not found. "
            "Run: source /home/edgerating/.profile"
        )

    return psycopg.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        row_factory=dict_row
    )


# ---------------------------------------------------------------------
# VALIDATE REQUIRED TABLE / COLUMNS
# ---------------------------------------------------------------------
def validate_database_structure(conn):
    required_columns = {
        "bucket_id",
        "customer_ban",
        "subscr_id",
        "status",
        "valid_start_time",
        "valid_end_time",
        "last_updated_at"
    }

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s
              AND table_name = 'customer_bucket'
            """,
            (DB_SCHEMA,)
        )

        existing_columns = {row["column_name"] for row in cur.fetchall()}

    missing = sorted(required_columns - existing_columns)

    if missing:
        raise RuntimeError(
            f"Missing required CUSTOMER_BUCKET columns: {missing}"
        )

    logger.info("DATABASE VALIDATION SUCCESS !!")


# ---------------------------------------------------------------------
# FIND EXPIRED BUCKET CANDIDATES
# ---------------------------------------------------------------------
def get_expiry_candidates(conn):

    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT
                bucket_id,
                customer_ban,
                subscr_id,
                status,
                valid_start_time,
                valid_end_time,
                bucket_initial_value,
                bucket_remaining_value
            FROM {DB_SCHEMA}.customer_bucket
            WHERE UPPER(TRIM(status)) IN ('ACTIVE', 'DEPLETING')
              AND valid_end_time IS NOT NULL
              AND valid_end_time <= CURRENT_TIMESTAMP
            ORDER BY valid_end_time, bucket_id
            """
        )

        return cur.fetchall()


# ---------------------------------------------------------------------
# VALIDATE STATUS
# ---------------------------------------------------------------------
def validate_expiry_candidates(rows):
    """
    Extra application-side validation.
    This does not modify the database.
    """

    valid_rows = []
    invalid_rows = []

    for row in rows:
        status = str(row["status"]).strip().upper()

        if status not in ("ACTIVE", "DEPLETING"):
            invalid_rows.append(
                (
                    row,
                    f"Current status {status} is not eligible for EXPIRED"
                )
            )
            continue

        if row["valid_end_time"] is None:
            invalid_rows.append(
                (
                    row,
                    "valid_end_time is NULL"
                )
            )
            continue

        valid_rows.append(row)

    return valid_rows, invalid_rows


# ---------------------------------------------------------------------
# UPDATE STATUS TO EXPIRED
# ---------------------------------------------------------------------
def update_expired_buckets(conn):
    """
    Atomic database-side update.

    The WHERE condition is repeated during UPDATE so a bucket whose
    status changed between SELECT and UPDATE will not be overwritten.
    """

    with conn.cursor() as cur:
        cur.execute(
            f"""
            UPDATE {DB_SCHEMA}.customer_bucket
            SET status = 'EXPIRED',
                last_updated_at = CURRENT_TIMESTAMP
            WHERE UPPER(TRIM(status)) IN ('ACTIVE', 'DEPLETING')
              AND valid_end_time IS NOT NULL
              AND valid_end_time <= CURRENT_TIMESTAMP
            RETURNING
                bucket_id,
                customer_ban,
                subscr_id,
                status,
                valid_end_time
            """
        )

        return cur.fetchall()


# ---------------------------------------------------------------------
# DISPLAY CHECK RESULTS
# ---------------------------------------------------------------------
def print_candidates(rows):
    if not rows:
        print("No buckets are currently eligible for EXPIRED status.")
        return

    print()
    print("=" * 100)
    print("CUSTOMER BUCKET EXPIRY CANDIDATES")
    print("=" * 100)

    for row in rows:
        print(
            f"bucket_id={row['bucket_id']} | "
            f"BAN={row['customer_ban']} | "
            f"SUBSCR={row['subscr_id']} | "
            f"status={row['status']} | "
            f"valid_end_time={row['valid_end_time']} | "
            f"remaining={row.get('bucket_remaining_value')}"
        )

    print("=" * 100)
    print(f"Total expiry candidates: {len(rows)}")


# ---------------------------------------------------------------------
# CHECK MODE
# ---------------------------------------------------------------------
def run_check():
    with get_connection() as conn:
        validate_database_structure(conn)

        rows = get_expiry_candidates(conn)
        valid_rows, invalid_rows = validate_expiry_candidates(rows)

        print_candidates(valid_rows)

        if invalid_rows:
            print()
            print("Validation warnings:")
            for row, reason in invalid_rows:
                print(
                    f"bucket_id={row['bucket_id']} | {reason}"
                )

        logger.info(
            "CHECK COMPLETE | candidates=%s invalid=%s",
            len(valid_rows),
            len(invalid_rows)
        )


# ---------------------------------------------------------------------
# UPDATE MODE
# ---------------------------------------------------------------------
def run_update():
    with get_connection() as conn:
        validate_database_structure(conn)

        # Show exactly what is eligible before update.
        candidates = get_expiry_candidates(conn)
        valid_rows, invalid_rows = validate_expiry_candidates(candidates)

        print_candidates(valid_rows)

        if invalid_rows:
            print()
            print("Validation warnings:")
            for row, reason in invalid_rows:
                print(
                    f"bucket_id={row['bucket_id']} | {reason}"
                )

        if not valid_rows:
            logger.info("UPDATE COMPLETE | expired=0")
            return

        updated_rows = update_expired_buckets(conn)
        conn.commit()

        print()
        print("=" * 100)
        print("EXPIRY UPDATE SUCCESS")
        print("=" * 100)

        for row in updated_rows:
            print(
                f"bucket_id={row['bucket_id']} | "
                f"BAN={row['customer_ban']} | "
                f"SUBSCR={row['subscr_id']} | "
                f"status={row['status']} | "
                f"valid_end_time={row['valid_end_time']}"
            )

        print("=" * 100)
        print(f"Total buckets updated to EXPIRED: {len(updated_rows)}")

        logger.info(
            "UPDATE COMPLETE | expired=%s | bucket_ids=%s",
            len(updated_rows),
            [row["bucket_id"] for row in updated_rows]
        )


# ---------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Validate or update expired CUSTOMER_BUCKET records."
    )

    mode = parser.add_mutually_exclusive_group(required=True)

    mode.add_argument(
        "--check",
        action="store_true",
        help="Only check buckets eligible for EXPIRED. No DB update."
    )

    mode.add_argument(
        "--update",
        action="store_true",
        help="Update eligible ACTIVE/DEPLETING buckets to EXPIRED."
    )

    args = parser.parse_args()

    try:
        if args.check:
            run_check()
        elif args.update:
            run_update()

        print()
        print(f"Log file: {LOG_FILE}")

    except Exception as exc:
        logger.exception("EXPIRY SCRIPT FAILED")
        print(f"ERROR: {type(exc).__name__}: {exc}")
        print(f"Check log: {LOG_FILE}")
        sys.exit(1)


if __name__ == "__main__":
    main()
