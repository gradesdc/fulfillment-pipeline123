import json
import os
import time
import uuid
from datetime import datetime, timezone, date

import psycopg2
from psycopg2.extras import Json
from kafka import KafkaConsumer


# =============================================================================
# 환경변수
# =============================================================================
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "event")  # producer와 동일해야 함
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "order-reader")
AUTO_OFFSET_RESET = os.getenv("AUTO_OFFSET_RESET", "earliest")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "fulfillment")
POSTGRES_USER = os.getenv("POSTGRES_USER", "admin")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "admin")


# =============================================================================
# 유틸: 시간/날짜 파싱
# =============================================================================
def now_utc():
    return datetime.now(timezone.utc)


def parse_occurred_at(value):
    """
    occurred_at 방어 파서
    - ISO 문자열("2026-02-02T07:37:35Z" 등)
    - datetime
    - 없거나 이상하면 now_utc()
    """
    if not value:
        return now_utc()

    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)

    if isinstance(value, str):
        v = value.strip()
        try:
            if v.endswith("Z"):
                v = v[:-1] + "+00:00"
            dt = datetime.fromisoformat(v)
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except Exception:
            return now_utc()

    return now_utc()


def parse_date(value):
    """
    promised_delivery_date 방어 파서 (DB는 date)
    - "YYYY-MM-DD" -> date
    - datetime/date -> date
    - 이상하면 None
    """
    if not value:
        return None

    if isinstance(value, date) and not isinstance(value, datetime):
        return value

    if isinstance(value, datetime):
        return value.date()

    if isinstance(value, str):
        v = value.strip()
        try:
            return date.fromisoformat(v)
        except Exception:
            return None

    return None


def safe_str(value):
    """텍스트 컬럼에 dict/list 같은 게 들어오는 사고 방지"""
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)
    return str(value)


# =============================================================================
# DB 연결 (재시도)
# =============================================================================
def connect_db_with_retry():
    while True:
        try:
            conn = psycopg2.connect(
                host=POSTGRES_HOST,
                port=POSTGRES_PORT,
                dbname=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD,
            )
            conn.autocommit = False
            print("✅ Postgres 연결 성공")
            return conn
        except Exception as e:
            print(f"⏳ Postgres 연결 실패: {e} (3초 후 재시도)")
            time.sleep(3)


# =============================================================================
# SQL (최신 스키마 반영)
# =============================================================================
SQL_INSERT_EVENTS = """
INSERT INTO events (
  event_id,
  order_id,
  event_type,
  reason_code,
  occurred_at,
  ingested_at,
  source,
  payload_json,
  shipping_address,
  user_id,
  ops_status,
  ops_note,
  ops_operator,
  ops_updated_at
) VALUES (
  %(event_id)s,
  %(order_id)s,
  %(event_type)s,
  %(reason_code)s,
  %(occurred_at)s,
  %(ingested_at)s,
  %(source)s,
  %(payload_json)s,
  %(shipping_address)s,
  %(user_id)s,
  %(ops_status)s,
  %(ops_note)s,
  %(ops_operator)s,
  %(ops_updated_at)s
)
ON CONFLICT (event_id) DO NOTHING;
"""

SQL_UPSERT_ORDERS = """
INSERT INTO orders (
  order_id,
  product_id,
  product_name,
  current_stage,
  current_status,
  hold_reason_code,
  last_event_type,
  last_occurred_at,
  tracking_no,
  promised_delivery_date,
  updated_at,
  hold_ops_status,
  hold_ops_note,
  hold_ops_operator,
  hold_ops_updated_at
) VALUES (
  %(order_id)s,
  %(product_id)s,
  %(product_name)s,
  %(current_stage)s,
  %(current_status)s,
  %(hold_reason_code)s,
  %(last_event_type)s,
  %(last_occurred_at)s,
  %(tracking_no)s,
  %(promised_delivery_date)s,
  %(updated_at)s,
  %(hold_ops_status)s,
  %(hold_ops_note)s,
  %(hold_ops_operator)s,
  %(hold_ops_updated_at)s
)
ON CONFLICT (order_id)
DO UPDATE SET
  product_id = EXCLUDED.product_id,
  product_name = EXCLUDED.product_name,
  current_stage = EXCLUDED.current_stage,
  current_status = EXCLUDED.current_status,
  hold_reason_code = EXCLUDED.hold_reason_code,
  last_event_type = EXCLUDED.last_event_type,
  last_occurred_at = EXCLUDED.last_occurred_at,
  tracking_no = EXCLUDED.tracking_no,
  promised_delivery_date = EXCLUDED.promised_delivery_date,
  updated_at = EXCLUDED.updated_at,
  hold_ops_status = EXCLUDED.hold_ops_status,
  hold_ops_note = EXCLUDED.hold_ops_note,
  hold_ops_operator = EXCLUDED.hold_ops_operator,
  hold_ops_updated_at = EXCLUDED.hold_ops_updated_at;
"""


# =============================================================================
# 메인
# =============================================================================
def main():
    print("📨 Kafka Consumer 시작 (최신 DB 스키마 반영)")
    print("=" * 60)

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
        group_id=KAFKA_GROUP_ID,
        auto_offset_reset=AUTO_OFFSET_RESET,
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )

    conn = connect_db_with_retry()
    cur = conn.cursor()

    try:
        for msg in consumer:
            event = msg.value if isinstance(msg.value, dict) else {}

            # -----------------------------------------------------------------
            # (A) 최소 보정/정규화
            # -----------------------------------------------------------------
            # event_id 없으면 생성 (events PK)
            event_id = event.get("event_id") or str(uuid.uuid4())

            order_id = event.get("order_id")
            current_stage = event.get("current_stage")
            current_status = event.get("current_status")

            # hold_reason_code / reason_code 둘 중 하나로 들어올 수 있으니 흡수
            hold_reason_code = event.get("hold_reason_code") or event.get("reason_code")

            occurred_at = parse_occurred_at(event.get("occurred_at"))
            ingested_at = now_utc()

            tracking_no = event.get("tracking_no")
            promised_delivery_date = parse_date(event.get("promised_delivery_date"))

            product_id = event.get("product_id")
            product_name = event.get("product_name")

            # ops (events)
            ops_status = safe_str(event.get("ops_status"))
            ops_note = safe_str(event.get("ops_note"))
            ops_operator = safe_str(event.get("ops_operator"))
            ops_updated_at = parse_occurred_at(event.get("ops_updated_at")) if event.get("ops_updated_at") else None

            # hold_ops (orders)
            hold_ops_status = safe_str(event.get("hold_ops_status"))
            hold_ops_note = safe_str(event.get("hold_ops_note"))
            hold_ops_operator = safe_str(event.get("hold_ops_operator"))
            hold_ops_updated_at = (
                parse_occurred_at(event.get("hold_ops_updated_at"))
                if event.get("hold_ops_updated_at")
                else None
            )

            # ✅ 핵심 규칙(너가 강조한 것):
            # orders.last_event_type 값을 기준으로 events.event_type을 채운다
            # (producer가 last_event_type 보내면 최우선)
            last_event_type = (
                event.get("last_event_type")
                or event.get("event_type")
                or current_status
                or "UNKNOWN"
            )

            print("✅ 메시지 수신")
            print(f"   order_id        : {order_id}")
            print(f"   current_status  : {current_status}")
            print(f"   last_event_type : {last_event_type}")
            print(f"   partition       : {msg.partition}")
            print(f"   offset          : {msg.offset}")
            print()

            # payload_json은 원문을 최대한 보존하는 게 디버깅에 유리
            payload_for_db = dict(event)
            payload_for_db["event_id"] = event_id
            payload_for_db["occurred_at"] = occurred_at.isoformat()

            # -----------------------------------------------------------------
            # (B) 1) events는 무조건 저장 (원장)
            # -----------------------------------------------------------------
            try:
                cur.execute(
                    SQL_INSERT_EVENTS,
                    {
                        "event_id": event_id,
                        "order_id": order_id,
                        # ✅ 여기: events.event_type = orders.last_event_type
                        "event_type": last_event_type,
                        "reason_code": hold_reason_code,
                        "occurred_at": occurred_at,
                        "ingested_at": ingested_at,
                        "source": safe_str(event.get("source")) or "kafka-producer",
                        "payload_json": Json(payload_for_db),
                        "shipping_address": safe_str(event.get("shipping_address")),
                        "user_id": safe_str(event.get("user_id")),
                        "ops_status": ops_status,
                        "ops_note": ops_note,
                        "ops_operator": ops_operator,
                        "ops_updated_at": ops_updated_at,
                    },
                )
                conn.commit()
            except Exception as e:
                conn.rollback()
                print(f"❌ [events 저장 실패] event_id={event_id} error={e}")
                continue

            # -----------------------------------------------------------------
            # (C) 2) orders 스냅샷 UPSERT (필수값 없으면 skip)
            # -----------------------------------------------------------------
            missing = []
            if not order_id:
                missing.append("order_id")
            if not current_stage:
                missing.append("current_stage")
            if not current_status:
                missing.append("current_status")

            if missing:
                print(f"⚠️ [SKIP orders] 필수값 누락: {', '.join(missing)} (event_id={event_id})")
                continue

            try:
                cur.execute(
                    SQL_UPSERT_ORDERS,
                    {
                        "order_id": order_id,
                        "product_id": product_id,
                        "product_name": product_name,
                        "current_stage": current_stage,
                        "current_status": current_status,
                        "hold_reason_code": hold_reason_code,
                        "last_event_type": last_event_type,
                        "last_occurred_at": occurred_at,
                        "tracking_no": tracking_no,
                        "promised_delivery_date": promised_delivery_date,
                        "updated_at": ingested_at,
                        "hold_ops_status": hold_ops_status,
                        "hold_ops_note": hold_ops_note,
                        "hold_ops_operator": hold_ops_operator,
                        "hold_ops_updated_at": hold_ops_updated_at,
                    },
                )
                conn.commit()
            except Exception as e:
                conn.rollback()
                print(f"❌ [orders 갱신 실패] order_id={order_id} event_id={event_id} error={e}")
                continue

    except KeyboardInterrupt:
        print("\n🛑 Consumer 종료")
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass
        consumer.close()
        print("✅ DB / Consumer 정상 종료")


if __name__ == "__main__":
    main()