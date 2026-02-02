import json
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple
from uuid import uuid4

from .repository import PostgresRepository


# -----------------------------------------------------------------------------
# 시간/파싱 유틸
# -----------------------------------------------------------------------------
def _utcnow() -> datetime:
    """UTC now (timestamptz로 넣기 좋게 tz-aware)."""
    return datetime.now(timezone.utc)


def _parse_dt(value: Any) -> datetime:
    """
    occurred_at을 datetime으로 변환
    - ISO8601 문자열(예: 2026-02-02T10:00:00Z, 2026-02-02T10:00:00+09:00)
    - datetime 객체
    - None -> 현재 UTC
    """
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)

    if value is None:
        return _utcnow()

    s = str(value).strip()
    # 'Z'는 UTC 의미 -> +00:00으로 변환
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"

    # fromisoformat이 대부분의 ISO8601 처리 가능
    dt = datetime.fromisoformat(s)
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


# -----------------------------------------------------------------------------
# 이벤트 타입 -> (stage, status) 매핑 (MVP용)
# -----------------------------------------------------------------------------
def _stage_status(event_type: str) -> Tuple[str, str]:
    """
    order_current에 넣을 stage/status를 이벤트 타입으로부터 추론
    - MVP에서는 너무 빡세게 규칙 만들 필요 없음
    - 대신 Unknown도 안전하게 들어가도록 처리
    """
    et = (event_type or "").upper()

    if et == "ORDER_CREATED":
        return "ORDER", "CREATED"
    if et == "HOLD":
        return "ORDER", "HOLD"

    # 문자열 포함 기반의 느슨한 매핑 (추후 팀 규칙으로 정교화 가능)
    if "PAY" in et:
        return "PAYMENT", et
    if "PICK" in et or "PACK" in et:
        return "FULFILLMENT", et
    if "SHIP" in et:
        return "SHIPMENT", et
    if "DELIVER" in et:
        return "DELIVERY", et

    return "UNKNOWN", et if et else "UNKNOWN"


# -----------------------------------------------------------------------------
# payload 안전 변환 (jsonb 저장용)
# -----------------------------------------------------------------------------
def _safe_payload_json(payload: Any) -> Dict[str, Any]:
    """
    payload_json을 무조건 dict로 맞춰서 jsonb에 넣기 쉽게 만든다.
    - dict면 그대로
    - str이면 json.loads 시도, 실패하면 {"raw": "..."}
    - 그 외 타입도 {"raw": str(payload)}로 감싼다
    """
    if payload is None:
        return {}
    if isinstance(payload, dict):
        return payload
    if isinstance(payload, str):
        try:
            return json.loads(payload)
        except Exception:
            return {"raw": payload}
    return {"raw": str(payload)}


# -----------------------------------------------------------------------------
# 메인 핸들러
# -----------------------------------------------------------------------------
def handle_message(
    event: Dict[str, Any],
    repo: PostgresRepository,
    kafka_meta: Optional[Dict[str, Any]] = None,
) -> bool:
    """
    ✅ 메시지 1개 처리 함수 (main.py에서 호출)

    반환값 규칙:
    - True  => "처리 완료"로 간주 (main이 offset commit 가능)
    - False => "일시적 장애 가능" (main이 commit하지 않고 재처리하게 두는 게 안전)

    이 함수가 하는 일 (순서 중요):
    1) JSON 파싱 실패/스키마 누락 같은 문제도 events 테이블에 남긴다 (파이프라인을 막지 않기 위해)
    2) 정상 이벤트는 events에 먼저 insert (idempotent: event_id 충돌 시 DO NOTHING)
    3) 신규 insert일 때만 order_current를 업서트 (중복 이벤트면 스킵)
    """

    kafka_meta = kafka_meta or {}

    # -------------------------------------------------------------------------
    # 0) main.py에서 JSON 파싱 실패 시 {"__raw__": "...", "__error__": "..."} 형태로 들어옴
    #    => PARSE_ERROR 이벤트로 events 테이블에 저장하고 True 반환
    # -------------------------------------------------------------------------
    if "__raw__" in event:
        parse_error_event = {
            "event_id": str(uuid4()),          # 원본 event_id가 없으니 새로 생성
            "order_id": None,                  # 원본에서 order_id를 못 뽑는 케이스가 많아서 None
            "event_type": "PARSE_ERROR",
            "reason_code": None,
            "occurred_at": _utcnow(),
            "source": "consumer",
            "payload_json": {
                "error": event.get("__error__"),
                "raw": event.get("__raw__"),
                # 디버깅용: raw 파일 경로 + kafka 메타데이터 같이 저장
                "raw_file_path": kafka_meta.get("raw_file_path"),
                "kafka": {
                    "topic": kafka_meta.get("topic"),
                    "partition": kafka_meta.get("partition"),
                    "offset": kafka_meta.get("offset"),
                    "timestamp": kafka_meta.get("timestamp"),
                },
            },
        }
        repo.insert_event_only(parse_error_event)
        return True

    # -------------------------------------------------------------------------
    # 1) 입력 이벤트를 "유연하게" 파싱 (producer가 키 이름을 조금 다르게 줄 수도 있음)
    # -------------------------------------------------------------------------
    event_id = event.get("event_id") or event.get("id")
    event_type = event.get("event_type") or event.get("type")
    occurred_at = event.get("occurred_at") or event.get("occurredAt") or event.get("timestamp")
    source = event.get("source") or event.get("producer") or "unknown"

    order_id = event.get("order_id") or event.get("orderId")
    reason_code = event.get("reason_code") or event.get("reasonCode")

    # payload_json 우선, 없으면 payload 사용
    payload_obj = event.get("payload_json")
    if payload_obj is None:
        payload_obj = event.get("payload")

    payload_json = _safe_payload_json(payload_obj)

    # -------------------------------------------------------------------------
    # 2) payload에 kafka_meta를 같이 넣어서 "나중에 분석/디버깅"에 도움 되게
    #    (events 테이블 payload_json 컬럼이 jsonb라 이런 메타 추가가 편함)
    # -------------------------------------------------------------------------
    payload_json = {
        **payload_json,
        "_kafka": {
            "topic": kafka_meta.get("topic"),
            "partition": kafka_meta.get("partition"),
            "offset": kafka_meta.get("offset"),
            "timestamp": kafka_meta.get("timestamp"),
            "raw_file_path": kafka_meta.get("raw_file_path"),
        },
    }

    # -------------------------------------------------------------------------
    # 3) 필수 필드 누락 처리 (스키마가 깨진 메시지)
    #    => SCHEMA_MISSING 이벤트로 events에 기록 후 True 반환
    #
    #    이유:
    #    - "문제 메시지 때문에 소비가 멈추는" 상황을 피하려고
    #    - 나중에 events 테이블에서 어떤 메시지가 왜 실패했는지 분석 가능
    # -------------------------------------------------------------------------
    missing = []
    if not event_id:
        missing.append("event_id")
    if not event_type:
        missing.append("event_type")
    if not occurred_at:
        missing.append("occurred_at")
    if not source:
        missing.append("source")

    if missing:
        schema_missing_event = {
            "event_id": str(uuid4()),
            "order_id": str(order_id) if order_id is not None else None,
            "event_type": "SCHEMA_MISSING",
            "reason_code": None,
            "occurred_at": _utcnow(),
            "source": "consumer",
            "payload_json": {
                "missing": missing,
                "original": event,
                "_kafka": payload_json.get("_kafka"),
            },
        }
        repo.insert_event_only(schema_missing_event)
        return True

    # -------------------------------------------------------------------------
    # 4) 정상 이벤트 처리: events insert -> 신규일 때만 order_current upsert
    #    여기서 DB 작업이 실패하면 False 반환해서 main이 commit하지 않게 만든다.
    # -------------------------------------------------------------------------
    try:
        normalized = {
            "event_id": str(event_id),
            "order_id": str(order_id) if order_id is not None else None,
            "event_type": str(event_type),
            "reason_code": str(reason_code) if reason_code is not None else None,
            "occurred_at": _parse_dt(occurred_at),
            "source": str(source),
            "payload_json": payload_json,
        }

        # ✅ 4-1) events에 먼저 insert (감사로그/재처리 근거)
        # - event_id가 PK면 중복 insert는 DO NOTHING
        inserted = repo.insert_event_and_return_inserted(normalized)

        # ✅ 4-2) 중복 이벤트면(order_id 최신상태를 또 갱신할 필요가 없으면) 스킵
        # - at-least-once 특성상 "중복 이벤트"는 발생한다고 가정하는 설계
        if not inserted:
            return True

        # ✅ 4-3) order_id가 없으면 order_current를 만들 수 없으니 events 저장만 하고 끝
        if normalized["order_id"] is None:
            return True

        # stage/status 추론
        stage, status = _stage_status(normalized["event_type"])

        # payload에서 자주 쓰는 필드만 "가볍게" 추출 (스키마 강제 X)
        tracking_no: Optional[str] = None
        promised_delivery_date: Optional[str] = None

        # payload_json이 dict인 건 보장됨(_safe_payload_json)
        if "tracking_no" in payload_json:
            tracking_no = str(payload_json.get("tracking_no"))
        elif "trackingNo" in payload_json:
            tracking_no = str(payload_json.get("trackingNo"))

        if "promised_delivery_date" in payload_json:
            promised_delivery_date = str(payload_json.get("promised_delivery_date"))
        elif "promisedDeliveryDate" in payload_json:
            promised_delivery_date = str(payload_json.get("promisedDeliveryDate"))

        # ✅ 4-4) order_current 업서트
        # - repository에서 "last_occurred_at이 더 최신일 때만 update" 조건을 걸어 out-of-order 방지
        repo.upsert_order_current(
            order_id=normalized["order_id"],
            current_stage=stage,
            current_status=status,
            hold_reason_code=normalized["reason_code"] if status == "HOLD" else None,
            last_event_type=normalized["event_type"],
            last_occurred_at=normalized["occurred_at"],
            tracking_no=tracking_no,
            promised_delivery_date=promised_delivery_date,
        )

        return True

    except Exception:
        # DB 장애 등 일시적 문제 가능성이 높음 -> commit하지 않게 False
        return False