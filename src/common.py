import json
import os
import time
import uuid
from dataclasses import dataclass
from typing import Any, Dict


def now_ms() -> int:
    return int(time.time() * 1000)


def new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:12]}"


def json_dumps(obj: Any) -> bytes:
    return json.dumps(obj, ensure_ascii=False).encode("utf-8")


def json_loads(b: bytes) -> Any:
    return json.loads(b.decode("utf-8"))


def bootstrap() -> str:
    return os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")


@dataclass(frozen=True)
class Topics:
    transactions: str = "transactions"
    fraud_alerts: str = "fraud_alerts"
    notification_acks: str = "notification_acks"


def log(agent: str, msg: str) -> None:
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] [{agent}] {msg}", flush=True)


def base_event(event_type: str, agent: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "event_id": new_id(event_type),
        "event_type": event_type,
        "ts_ms": now_ms(),
        "agent": agent,
        "payload": payload,
    }
