from flask import Flask, request, jsonify
import pika
import json
from pika.adapters.blocking_connection import BlockingChannel
import os
from dotenv import load_dotenv
load_dotenv()


app = Flask(__name__)

RABBIT_USER = os.getenv("RABBITMQ_DEFAULT_USER")
RABBIT_PASS = os.getenv("RABBITMQ_DEFAULT_PASS")
RABBIT_HOST = os.getenv("RABBIT_HOST")


def get_channel() -> BlockingChannel:
    """Create and return a blocking channel to RabbitMQ."""
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host=str(RABBIT_HOST),
            credentials=pika.PlainCredentials(
                username=str(RABBIT_USER),
                password=str(RABBIT_PASS)
            )
        ))
    channel: BlockingChannel = connection.channel()
    return channel


def send_message(queue_name: str, body) -> None:  # type: ignore
    if isinstance(body, (dict, list)):
        body = json.dumps(body)
    elif not isinstance(body, (str, bytes)):
        body = str(body)  # type: ignore

    if isinstance(body, str):
        body = body.encode()
    channel = get_channel()

    # Declare queue (creates if not exists)
    channel.queue_declare(queue=queue_name, durable=True)  # type: ignore

    # Send message
    channel.basic_publish(
        exchange="",  # default exchange
        routing_key=queue_name,
        body=body,
        properties=pika.BasicProperties(
            delivery_mode=2  # make message persistent
        ),
    )
    channel.close()  # type: ignore


@app.route("/publish/<queue_name>", methods=["POST"])
def publish(queue_name: str):
    data = request.get_json(force=True)
    send_message(queue_name, data)
    return jsonify({"status": "sent", "queue": queue_name, "data": data})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
