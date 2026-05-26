"""NotificationAgent

Subscribes to `fraud_alerts`, simulates sending an alert, and publishes an
acknowledgement to `notification_acks`.

Run:
  python -m src.notification_agent
"""

from __future__ import annotations

import time

from kafka import KafkaConsumer, KafkaProducer

from .common import Topics, base_event, bootstrap, json_dumps, json_loads, log


AGENT = "NotificationAgent"


def main() -> None:
    topics = Topics()
    bs = bootstrap()

    consumer = KafkaConsumer(
        topics.fraud_alerts,
        bootstrap_servers=bs,
        group_id="notification-agent",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=json_loads,
        key_deserializer=lambda b: b.decode("utf-8") if b else None,
    )

    producer = KafkaProducer(
        bootstrap_servers=bs,
        acks="all",
        retries=3,
        value_serializer=json_dumps,
        key_serializer=lambda k: k.encode("utf-8"),
    )

    log(AGENT, f"Starting (bootstrap={bs}). Subscribing to '{topics.fraud_alerts}', publishing to '{topics.notification_acks}'.")

    for msg in consumer:
        alert_event = msg.value
        p = alert_event.get("payload") or {}
        tx_id = str(p.get("transaction_id"))
        risk_score = p.get("risk_score")
        reason = p.get("reason")

        # Simulate downstream side effect (email/SMS/push)
        log(AGENT, f"Sending alert for transaction_id={tx_id} risk_score={risk_score} reason={reason}")
        time.sleep(0.2)

        ack_payload = {"transaction_id": tx_id, "status": "NOTIFIED"}
        ack_event = base_event("notification_ack", AGENT, ack_payload)
        producer.send(topics.notification_acks, key=tx_id, value=ack_event)
        producer.flush(timeout=1)
        log(AGENT, f"Published notification_ack for transaction_id={tx_id}")


if __name__ == "__main__":
    main()
