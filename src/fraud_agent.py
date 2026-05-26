"""FraudAgent

Subscribes to `transactions`, applies a simple rule-based risk score, and
publishes fraud alerts to `fraud_alerts`.

Run:
  python -m src.fraud_agent
"""

from __future__ import annotations

import random

from kafka import KafkaConsumer, KafkaProducer

from .common import Topics, base_event, bootstrap, json_dumps, json_loads, log


AGENT = "FraudAgent"

SUSPICIOUS_MERCHANTS = {"LuxuryWatches", "CryptoExchange"}


def score_transaction(tx_event: dict) -> tuple[int, str]:
    p = tx_event.get("payload") or {}
    amount = float(p.get("amount", 0))
    merchant = str(p.get("merchant", ""))

    # Very simple heuristics for a class demo
    if amount >= 1000:
        return 90, "high_amount"
    if merchant in SUSPICIOUS_MERCHANTS and amount >= 200:
        return 80, "suspicious_merchant"
    if random.random() < 0.05:
        return 70, "random_spot_check"
    return 10, "low_risk"


def main() -> None:
    topics = Topics()
    bs = bootstrap()

    consumer = KafkaConsumer(
        topics.transactions,
        bootstrap_servers=bs,
        group_id="fraud-agent",
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

    log(AGENT, f"Starting (bootstrap={bs}). Subscribing to '{topics.transactions}', publishing to '{topics.fraud_alerts}'.")

    for msg in consumer:
        tx_event = msg.value
        p = tx_event.get("payload") or {}
        tx_id = str(p.get("transaction_id"))

        risk_score, reason = score_transaction(tx_event)
        log(AGENT, f"Consumed transaction_id={tx_id} risk_score={risk_score} reason={reason}")

        if risk_score >= 70:
            alert_payload = {
                "transaction_id": tx_id,
                "risk_score": risk_score,
                "reason": reason,
                "user_id": p.get("user_id"),
                "amount": p.get("amount"),
                "merchant": p.get("merchant"),
            }
            alert_event = base_event("fraud_alert", AGENT, alert_payload)
            producer.send(topics.fraud_alerts, key=tx_id, value=alert_event)
            producer.flush(timeout=1)
            log(AGENT, f"Published fraud_alert for transaction_id={tx_id} (risk_score={risk_score})")


if __name__ == "__main__":
    main()
