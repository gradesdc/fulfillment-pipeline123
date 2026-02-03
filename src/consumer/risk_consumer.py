import json
import uuid
import psycopg2
from datetime import datetime
from kafka import KafkaConsumer

# ---------------------------------------------------------
# ⚙️ DB 및 Kafka 설정
# ---------------------------------------------------------
DB_CONFIG = {
    "host": "192.168.239.40",
    "database": "fulfillment",
    "user": "admin",
    "password": "admin"
}

BOOTSTRAP_SERVERS = 'localhost:9092'
TOPIC_NAME = 'event'
GROUP_ID = 'risk-management-group'

abuse_tracker = {}

# ---------------------------------------------------------
# ⚖️ 리스크 판단 로직
# ---------------------------------------------------------
def check_risk(order_data, tracker):
    uid = order_data['customer_id']
    pid = order_data['product_id']
    addr = str(order_data['address'])
    curr_time = datetime.fromisoformat(order_data['last_occurred_at'])

    # 1. 주소 오류
    bad_keywords = ["?", "Unknown", "123", "NULL"]
    if any(k in addr for k in bad_keywords) or len(addr) < 5:
        return 'INVALID_ADDRESS'

    # 2. 단일 유저 도배 (1초 이내)
    key = (uid, pid)
    if key in tracker:
        last_time = tracker[key]
        if (curr_time - last_time).total_seconds() < 1.0:
            return 'ABUSE_DETECTED'
    
    tracker[key] = curr_time
    return None

# ---------------------------------------------------------
# 💾 DB 저장 및 소급 격리 (에러 수정됨)
# ---------------------------------------------------------
def save_to_db(cur, data, status, reason=None):
    """단일 주문 건 적재"""
    cur.execute("""
        INSERT INTO orders (
            order_id, product_id, product_name, current_stage, current_status, 
            hold_reason_code, last_event_type, last_occurred_at, shipping_address, user_id, updated_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        data['order_id'], data['product_id'], data['product_name'], 
        data['current_stage'], status, reason, data['last_event_type'], 
        data['last_occurred_at'], data['address'], data['customer_id'], datetime.now()
    ))

    cur.execute("""
        INSERT INTO events (
            event_id, order_id, event_type, reason_code, occurred_at, source, payload_json
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, (
        str(uuid.uuid4()), data['order_id'], status, reason, 
        datetime.now(), 'RISK_CONSUMER', json.dumps(data, ensure_ascii=False)
    ))

def quarantine_retroactive(cur, uid, pid):
    """🔥 [FIX] SQL 내부에서 gen_random_uuid()를 사용하여 중복 방지"""
    # 1. 이전 PAID 주문들 HOLD로 전환
    cur.execute("""
        UPDATE orders 
        SET current_status = 'HOLD', hold_reason_code = 'RETROACTIVE_ABUSE', updated_at = NOW()
        WHERE user_id = %s AND product_id = %s AND current_status = 'PAID'
        AND last_occurred_at >= (NOW() - INTERVAL '10 seconds')
    """, (uid, pid))
    
    # 2. 변경된 모든 행에 대해 각각 고유한 event_id 생성하여 기록
    # gen_random_uuid()는 PostgreSQL v13 이상에서 기본 제공됩니다.
    cur.execute("""
        INSERT INTO events (event_id, order_id, event_type, reason_code, occurred_at, source, payload_json)
        SELECT gen_random_uuid()::text, order_id, 'HOLD', 'RETROACTIVE_ABUSE_HOLD', NOW(), 'RISK_SYSTEM', '{}'::jsonb
        FROM orders 
        WHERE user_id = %s AND product_id = %s AND current_status = 'HOLD'
        AND updated_at >= (NOW() - INTERVAL '1 second')
    """, (uid, pid))

# ---------------------------------------------------------
# 🚀 메인 실행부
# ---------------------------------------------------------
if __name__ == "__main__":
    conn = psycopg2.connect(**DB_CONFIG)
    # 자동 커밋 비활성화 (트랜잭션 관리 위해)
    conn.autocommit = False 

    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=[BOOTSTRAP_SERVERS],
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id=GROUP_ID,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    print(f"📡 [Risk Consumer] PK 중복 해결 버전 가동 중...")

    try:
        for message in consumer:
            order = message.value
            risk_reason = None
            
            if order.get('current_status') == 'PAID':
                risk_reason = check_risk(order, abuse_tracker)

            final_status = 'HOLD' if risk_reason else order['current_status']

            try:
                with conn.cursor() as cur:
                    save_to_db(cur, order, final_status, risk_reason)

                    if risk_reason == 'ABUSE_DETECTED':
                        quarantine_retroactive(cur, order['customer_id'], order['product_id'])
                        print(f"🚨 [QUARANTINE] {order['customer_id']} | 전수 HOLD 전환")

                    conn.commit()
                
                if final_status == 'HOLD':
                    print(f"🛑 [HOLD] {order['customer_id']} | 사유: {risk_reason}")
                else:
                    print(f"✅ [PASS] {final_status} | {order['product_name']}")

            except Exception as e:
                conn.rollback()
                print(f"🔥 DB Error: {e}")

    except KeyboardInterrupt:
        conn.close()
        consumer.close()