"""Consume requests from a private RabbitMQ queue bound to a topic pattern."""

import argparse
import json
import os

import pika


EXCHANGE_NAME = "rapido-dispatch"


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("name", help="Driver name shown in console output.")
    parser.add_argument("pattern", help="Topic binding pattern, for example bangalore.*.food.")
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

    result = channel.queue_declare(queue="", exclusive=True, auto_delete=True)
    queue_name = result.method.queue
    channel.queue_bind(exchange=EXCHANGE_NAME, queue=queue_name, routing_key=args.pattern)
    channel.basic_qos(prefetch_count=1)

    def handle_request(channel, method, _properties, body):
        try:
            request = json.loads(body)
        except json.JSONDecodeError:
            print(f"{args.name}: invalid JSON; NACKing request")
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            return

        if not request.get("zone"):
            print(f"{args.name}: missing zone; NACKing request {request}")
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            return

        print(f"{args.name}: received {method.routing_key}: {request}")
        channel.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_consume(queue=queue_name, on_message_callback=handle_request, auto_ack=False)
    print(f"{args.name} listening on {queue_name} with pattern {args.pattern}. Press Ctrl+C to stop.")
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print(f"\n{args.name} stopped.")
    finally:
        if connection.is_open:
            connection.close()


if __name__ == "__main__":
    main()
