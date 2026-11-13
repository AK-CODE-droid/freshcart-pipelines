"""Publish sample FreshCart web-order events for the lakehouse bonus demo."""

import argparse
import json
import os
from datetime import datetime, timezone

import pika


EXCHANGE_NAME = "freshcart-orders"
ROUTING_KEY = "orders.web.new"
SAMPLE_ORDERS = (
    {
        "order_id": "ORD-STREAM-9001",
        "cust_email": "priya.r@gmail.com",
        "cust_name": "Priya R",
        "order_date": "2026-09-03",
        "amount": "1,500.00",
        "currency": "INR",
        "pincode": "600041",
        "updated_at": "2026-09-03 18:00:00",
    },
    {
        "order_id": "ORD-STREAM-9002",
        "cust_email": "arjun.k@yahoo.in",
        "cust_name": "Arjun K",
        "order_date": "2026-09-01",
        "amount": "50.00",
        "currency": "USD",
        "pincode": "560034",
        "updated_at": "2026-09-01 19:00:00",
    },
)


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--amqp-url",
        default=os.environ.get("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/%2F"),
        help="RabbitMQ connection URL (or set RABBITMQ_URL).",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    connection = pika.BlockingConnection(pika.URLParameters(args.amqp_url))
    channel = connection.channel()
    channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type="topic", durable=True)

    for order in SAMPLE_ORDERS:
        message = {**order, "streamed_at": datetime.now(timezone.utc).isoformat()}
        channel.basic_publish(
            exchange=EXCHANGE_NAME,
            routing_key=ROUTING_KEY,
            body=json.dumps(message),
            properties=pika.BasicProperties(
                content_type="application/json",
                delivery_mode=pika.DeliveryMode.Persistent,
            ),
        )
        print(f"published {ROUTING_KEY}: {message['order_id']}")
    connection.close()


if __name__ == "__main__":
    main()
