"""Publish simulated food and ride requests to the RabbitMQ topic exchange."""

import argparse
import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

import pika


EXCHANGE_NAME = "rapido-dispatch"
LOCATIONS = {
    "bangalore": ("koramangala", "indiranagar", "whitefield"),
    "chennai": ("tnagar", "whitefield"),
}
REQUEST_TYPES = ("ride", "food")


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--amqp-url",
        default=os.environ.get("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/%2F"),
        help="RabbitMQ connection URL (or set RABBITMQ_URL).",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=2.0,
        help="Seconds between messages (default: 2).",
    )
    parser.add_argument(
        "--bad-every",
        type=int,
        default=10,
        help="Publish one message without zone after this many messages (default: 10).",
    )
    parser.add_argument(
        "--proof-key",
        help="Optionally publish this city.zone.type request once before random dispatching.",
    )
    return parser.parse_args()


def build_request(message_number, bad_every):
    city = random.choice(tuple(LOCATIONS))
    zone = random.choice(LOCATIONS[city])
    request_type = random.choice(REQUEST_TYPES)
    routing_key = f"{city}.{zone}.{request_type}"
    request = {
        "id": str(uuid.uuid4()),
        "city": city,
        "zone": zone,
        "type": request_type,
        "ts": datetime.now(timezone.utc).isoformat(),
    }

    is_bad = bad_every > 0 and message_number % bad_every == 0
    if is_bad:
        request.pop("zone")

    return routing_key, request, is_bad


def build_proof_request(routing_key):
    city, zone, request_type = routing_key.split(".")
    return {
        "id": str(uuid.uuid4()),
        "city": city,
        "zone": zone,
        "type": request_type,
        "ts": datetime.now(timezone.utc).isoformat(),
    }


def main():
    args = parse_args()
    if args.interval < 0:
        raise ValueError("--interval must be zero or greater")

    connection = pika.BlockingConnection(pika.URLParameters(args.amqp_url))
    channel = connection.channel()
    channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type="topic", durable=True)

    print(f"Publishing to {EXCHANGE_NAME}. Press Ctrl+C to stop.")
    if args.proof_key:
        try:
            proof_request = build_proof_request(args.proof_key)
        except ValueError as error:
            raise ValueError("--proof-key must have the format city.zone.type") from error
        channel.basic_publish(
            exchange=EXCHANGE_NAME,
            routing_key=args.proof_key,
            body=json.dumps(proof_request),
            properties=pika.BasicProperties(content_type="application/json"),
        )
        print(f"PROOF published {args.proof_key}: {proof_request}")

    message_number = 0
    try:
        while True:
            message_number += 1
            routing_key, request, is_bad = build_request(message_number, args.bad_every)
            channel.basic_publish(
                exchange=EXCHANGE_NAME,
                routing_key=routing_key,
                body=json.dumps(request),
                properties=pika.BasicProperties(
                    content_type="application/json",
                    delivery_mode=pika.DeliveryMode.Persistent,
                ),
            )
            label = "BAD " if is_bad else ""
            print(f"{label}published {routing_key}: {request}")
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("\nDispatcher stopped.")
    finally:
        connection.close()


if __name__ == "__main__":
    main()
