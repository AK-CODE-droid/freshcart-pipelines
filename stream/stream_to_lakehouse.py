"""Collect FreshCart order events from RabbitMQ and load them into Snowflake gold."""

import argparse
import json
import os
import sys
import time
from datetime import date
from pathlib import Path

import pika


EXCHANGE_NAME = "freshcart-orders"
QUEUE_NAME = "freshcart-lakehouse"


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--elt-dir", required=True, help="FreshCart ELT folder containing extract.py and sql/.")
    parser.add_argument("--seconds", type=int, default=60, help="How long to collect events (default: 60).")
    parser.add_argument("--run-id", default="bonus_stream_demo", help="Stable ID used to replay the same batch.")
    parser.add_argument("--run-date", default="2026-09-30", help="Landing date, YYYY-MM-DD.")
    parser.add_argument("--replay", help="Existing landing file to load again without consuming RabbitMQ.")
    parser.add_argument(
        "--amqp-url",
        default=os.environ.get("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/%2F"),
        help="RabbitMQ connection URL (or set RABBITMQ_URL).",
    )
    return parser.parse_args()


def collect_orders(amqp_url, seconds):
    """Collect valid JSON events and leave them unacknowledged until landing succeeds."""
    connection = pika.BlockingConnection(pika.URLParameters(amqp_url))
    channel = connection.channel()
    channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type="topic", durable=True)
    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    channel.queue_bind(exchange=EXCHANGE_NAME, queue=QUEUE_NAME, routing_key="orders.web.#")

    records, delivery_tags = [], []
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        method, _properties, body = channel.basic_get(queue=QUEUE_NAME, auto_ack=False)
        if method is None:
            time.sleep(1)
            continue
        try:
            records.append(json.loads(body))
            delivery_tags.append(method.delivery_tag)
        except json.JSONDecodeError:
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            print("discarded invalid JSON event")

    return connection, channel, records, delivery_tags


def gold_totals(snowflake_io, connection):
    return snowflake_io.query_one(
        connection,
        "SELECT COUNT(*), COALESCE(SUM(amount_inr), 0), COALESCE(SUM(loyalty_points), 0) FROM gold.fct_orders",
    )


def transform(snowflake_io, connection, elt_dir, run_id, run_date):
    sql_dir = Path(elt_dir) / "sql"
    variables = {"run_id": run_id, "run_date": run_date}
    for filename in (
        "10_silver_orders_clean.sql",
        "20_silver_orders_dedup.sql",
        "30_dim_customers_scd2.sql",
        "31_ref_fx_daily.sql",
        "32_ref_pincode_geo.sql",
        "40_gold_fct_orders.sql",
    ):
        snowflake_io.run_sql_file(connection, sql_dir / filename, variables)


def main():
    args = parse_args()
    if args.seconds < 1:
        raise ValueError("--seconds must be at least 1")
    run_date = date.fromisoformat(args.run_date)
    elt_dir = Path(args.elt_dir).resolve()
    if not elt_dir.is_dir():
        raise FileNotFoundError(f"ELT directory not found: {elt_dir}")
    sys.path.insert(0, str(elt_dir))
    import extract
    import snowflake_io

    connection = snowflake_io.connect()
    try:
        before = gold_totals(snowflake_io, connection)
        if args.replay:
            landing_file = Path(args.replay).resolve()
            records_loaded = "replay"
        else:
            rabbit_connection, channel, records, delivery_tags = collect_orders(args.amqp_url, args.seconds)
            try:
                print(f"collected {len(records)} FreshCart events")
                landing_file = extract.write_landing("web", records, args.run_id, run_date)
                if landing_file is None:
                    raise RuntimeError("No events collected; publish orders before running the consumer.")
                for delivery_tag in delivery_tags:
                    channel.basic_ack(delivery_tag=delivery_tag)
                records_loaded = len(records)
            finally:
                rabbit_connection.close()

        stage_dir = f"stream_orders/{run_date.isoformat()}/{args.run_id}"
        copied = snowflake_io.load_file(connection, landing_file, "bronze.orders_raw", stage_dir)
        transform(snowflake_io, connection, elt_dir, args.run_id, run_date.isoformat())
        after = gold_totals(snowflake_io, connection)

        print(f"landing_file={landing_file}")
        print(f"events_collected={records_loaded}; bronze_rows_loaded={copied}")
        print(f"gold_before={before}")
        print(f"gold_after={after}")
    finally:
        connection.close()


if __name__ == "__main__":
    main()
