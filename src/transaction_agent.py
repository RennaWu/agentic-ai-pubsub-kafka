"""TransactionAgent

Publishes synthetic transactions to the `transactions` topic and subscribes to
`notification_acks` to learn when a downstream agent has handled a fraud alert.

Run:
  python -m src.transaction_agent
"""

from __future__ import annotations

import random
import time
from typing import Dict

from kafka import KafkaConsumer, KafkaProducer

from .common import Topics, base_event, bootstrap, json_dumps, json_loads, log


AGENT = "TransactionAgent"

MERCHANTS = [
    "CoffeeCorner",
    "GadgetGalaxy",
    "BookBarn",
    "QuickMart",
    "LuxuryWatches",
    "CryptoExchange",
]


def gen_transaction() -> Dict:
    tx_id = f"tx_{random.randint(100000, 999999)}"
    amount = round(random.uniform(700, 1500), 2)
    user_id = f"u_{random.randint(1000, 9999)}"
    merchant = random.choice(MERCHANTS)

    payload = {
        "transaction_id": tx_id,
        "user_id": user_id,
        "merchant": merchant,
        "amount": amount,
        "currency": "USD",
    }
    return base_event("transaction", AGENT, payload)


def main() -> None:
    topics = Topics()
    bs = bootstrap()

    producer = KafkaProducer(
        bootstrap_servers=bs,
        acks="all",
        retries=3,
        value_serializer=json_dumps,
        key_serializer=lambda k: k.encode("utf-8"),
    )

    consumer = KafkaConsumer(
        topics.notification_acks,
        bootstrap_servers=bs,
        group_id="transaction-agent",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=json_loads,
        key_deserializer=lambda b: b.decode("utf-8") if b else None,
    )

    log(AGENT, f"Starting (bootstrap={bs}). Publishing to '{topics.transactions}', subscribing to '{topics.notification_acks}'.")

    while True:
        # Non-blocking check for acknowledgements
        polled = consumer.poll(timeout_ms=100)
        for _tp, records in polled.items():
            for record in records:
                ack = record.value
                tx_id = (ack.get("payload") or {}).get("transaction_id")
                status = (ack.get("payload") or {}).get("status")
                log(AGENT, f"Received ack for transaction_id={tx_id} status={status}")

        tx = gen_transaction()
        tx_id = tx["payload"]["transaction_id"]
        amount = tx["payload"]["amount"]
        merchant = tx["payload"]["merchant"]

        producer.send(topics.transactions, key=tx_id, value=tx)
        producer.flush(timeout=1)
        log(AGENT, f"Published transaction_id={tx_id} amount={amount} merchant={merchant}")

        time.sleep(5)


if __name__ == "__main__":
    main()
