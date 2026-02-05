import json
import uuid
import psycopg2
from datetime import datetime, timedelta, timezone
from collections import deque, defaultdict
from kafka import KafkaConsumer

# =========================================================
# 1. ⚙️ 설정 및 상수 정의
# =========================================================
DB_CONFIG = {
    "host": "192.168.239.40",
    "database": "fulfillment",
    "user": "admin",
    "password": "admin"
}
BOOTSTRAP_SERVERS = 'localhost:9092'
TOPIC_NAME = 'event'
GROUP_ID = 'risk-management-group'
KST = timezone(timedelta(hours=9))

# [리스크 감지 임계값]
USER_BURST_WINDOW = 10.0   
USER_BURST_LIMIT = 5       
PROD_BURST_WINDOW = 1.0    
PROD_BURST_LIMIT = 4       
STOCK_LIMIT = 0

# [사유 코드]
CODE_VALID = 'FUL-VALID'
CODE_FRAUD_USER = 'FUL-FRAUD-USER' 
CODE_FRAUD_PROD = 'FUL-FRAUD-PROD'
CODE_STOCK_OUT = 'FUL-INV'
CODE_SYSTEM_HOLD = 'SYSTEM_HOLD'

# =========================================================
# 2. 📝 SQL 쿼리 정의
# =========================================================
SQL_CHECK_STOCK = "SELECT stock FROM products WHERE product_id = %s"

SQL_INSERT_RAW = """
    INSERT INTO orders_raw (raw_payload, kafka_offset, ingested_at) 
    VALUES (%s, %s, %s) RETURNING raw_id
"""

SQL_UPSERT_ORDER = """
    INSERT INTO orders (
        order_id, user_id, product_id, product_name, shipping_address,
        current_stage, current_status, hold_reason_code, 
        last_event_type, last_occurred_at, raw_reference_id, created_at
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (order_id) DO UPDATE SET
        current_stage = EXCLUDED.current_stage,
        current_status = EXCLUDED.current_status,
        hold_reason_code = EXCLUDED.hold_reason_code,
        raw_reference_id = EXCLUDED.raw_reference_id;
"""

SQL_INSERT_EVENT = """
    INSERT INTO events (event_id, order_id, event_type, current_status, reason_code, occurred_at)
    VALUES (%s, %s, %s, %s, %s, %s)
"""

# -------------------------------------------------------------------------
# [소급 적용 SQL] Orders와 Events를 동시에 업데이트
# -------------------------------------------------------------------------

# 1. Orders 테이블 업데이트 (유저 도배)
SQL_QUARANTINE_USER_ORDERS = """
    UPDATE orders 
    SET current_status = 'HOLD', 
        hold_reason_code = %s
    WHERE user_id = %s AND product_id = %s AND order_id != %s 
      AND created_at >= (%s - INTERVAL '30 seconds')
"""

# 2. Events 테이블 업데이트 (유저 도배)
# events 테이블엔 user_id가 없으므로 orders 테이블과 조인(서브쿼리)해서 업데이트
SQL_QUARANTINE_USER_EVENTS = """
    UPDATE events
    SET current_status = 'HOLD',
        reason_code = %s
    WHERE order_id IN (
        SELECT order_id FROM orders 
        WHERE user_id = %s AND product_id = %s 
          AND created_at >= (%s - INTERVAL '30 seconds')
          AND order_id != %s
    )
"""

# 3. Orders 테이블 업데이트 (상품 폭주)
SQL_QUARANTINE_PROD_ORDERS = """
    UPDATE orders 
    SET current_status = 'HOLD', 
        hold_reason_code = %s
    WHERE product_id = %s AND order_id != %s
      AND created_at >= (%s - INTERVAL '5 seconds')
"""

# 4. Events 테이블 업데이트 (상품 폭주)
SQL_QUARANTINE_PROD_EVENTS = """
    UPDATE events
    SET current_status = 'HOLD',
        reason_code = %s
    WHERE order_id IN (
        SELECT order_id FROM orders
        WHERE product_id = %s
          AND created_at >= (%s - INTERVAL '5 seconds')
          AND order_id != %s
    )
"""

# =========================================================
# 3. 🛠️ 유틸리티 함수
# =========================================================
def get_kst_now():
    return datetime.now(KST)

def parse_iso_datetime(value):
    if not value: return datetime.now(timezone.utc)
    if isinstance(value, datetime): return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str):
        try:
            v = value.strip()
            if v.endswith("Z"): v = v[:-1] + "+00:00"
            dt = datetime.fromisoformat(v)
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except: pass
    return datetime.now(timezone.utc)

# =========================================================
# 4. ⚖️ 리스크 감지 로직
# =========================================================
def check_stock_anomaly(conn, pid):
    if not pid: return False
    with conn.cursor() as cur:
        cur.execute(SQL_CHECK_STOCK, (str(pid),))
        row = cur.fetchone()
    if row is None: return True
    return row[0] is not None and row[0] <= STOCK_LIMIT

def check_burst_anomaly(order_data, product_tracker):
    pid = order_data.get("product_id")
    if not pid: return False
    now_dt = parse_iso_datetime(order_data.get("last_occurred_at") or order_data.get("occurred_at"))
    q = product_tracker[pid]
    q.append(now_dt)
    
    cutoff = now_dt.timestamp() - PROD_BURST_WINDOW
    while q and q[0].timestamp() < cutoff: q.popleft()
    
    if len(q) > 5000:
        while len(q) > 5000: q.popleft()
        
    return len(q) >= PROD_BURST_LIMIT

# =========================================================
# 5. 🛡️ 메인 리스크 판단
# =========================================================
def check_risk(order_data, abuse_tracker, product_tracker, conn):
    uid = str(order_data.get('user_id', ''))
    pid = str(order_data.get('product_id', ''))
    addr = str(order_data.get('shipping_address', ''))
    curr_time = get_kst_now()

    # 1. 필수값
    bad_keywords = ["?", "Unknown", "123", "NULL"]
    if not uid or not addr or len(addr) < 5 or any(k in addr for k in bad_keywords):
        return CODE_VALID

    # 2. 유저 도배
    user_risk_detected = False
    key = (uid, pid)
    if key not in abuse_tracker:
        abuse_tracker[key] = {'count': 1, 'start_time': curr_time}
    else:
        record = abuse_tracker[key]
        elapsed = (curr_time - record['start_time']).total_seconds()
        if elapsed > USER_BURST_WINDOW:
            abuse_tracker[key] = {'count': 1, 'start_time': curr_time}
        else:
            record['count'] += 1
            if record['count'] > USER_BURST_LIMIT:
                user_risk_detected = True

    # 3. 상품 폭주
    prod_risk_detected = check_burst_anomaly(order_data, product_tracker)

    # 결정
    if user_risk_detected: return CODE_FRAUD_USER
    if check_stock_anomaly(conn, pid): return CODE_STOCK_OUT
    if prod_risk_detected: return CODE_FRAUD_PROD
    
    return None

# =========================================================
# 6. 💾 DB 처리 (Events 포함 업데이트)
# =========================================================
def save_to_db(cur, data, is_hold, risk_reason, kafka_offset):
    current_timestamp = get_kst_now()
    
    target_stage = data.get('current_stage', 'PAYMENT')
    
    if is_hold:
        target_status = 'HOLD'
        final_reason = risk_reason if risk_reason else CODE_SYSTEM_HOLD
    else:
        target_status = data.get('current_status', 'PAID')
        final_reason = None

    cur.execute(SQL_INSERT_RAW, (json.dumps(data, ensure_ascii=False), kafka_offset, current_timestamp))
    raw_id = cur.fetchone()[0]

    cur.execute(SQL_UPSERT_ORDER, (
        data['order_id'], data['user_id'], data['product_id'], data['product_name'], data['shipping_address'],
        target_stage, target_status, final_reason,
        data.get('last_event_type', 'ORDER_CREATED'), data.get('last_occurred_at'),
        raw_id, current_timestamp
    ))

    cur.execute(SQL_INSERT_EVENT, (
        str(uuid.uuid4()), 
        data['order_id'], 
        target_stage,   
        target_status,  
        final_reason,   
        current_timestamp
    ))
    return target_status

def quarantine_retroactive_user(cur, uid, pid, current_order_id):
    current_timestamp = get_kst_now()
    
    # 1. Orders 테이블 업데이트
    cur.execute(SQL_QUARANTINE_USER_ORDERS, (
        CODE_FRAUD_USER, str(uid), str(pid), str(current_order_id), current_timestamp
    ))
    row_count = cur.rowcount
    
    # 2. Events 테이블 업데이트 (✅ 추가됨)
    #    동일한 조건의 order_id를 찾아 events도 똑같이 HOLD로 만듦
    cur.execute(SQL_QUARANTINE_USER_EVENTS, (
        CODE_FRAUD_USER,            # SET reason_code
        str(uid), str(pid),         # WHERE (subquery)
        current_timestamp,          # WHERE time
        str(current_order_id)       # exclude current
    ))
    
    return row_count

def quarantine_retroactive_prod(cur, pid, current_order_id):
    current_timestamp = get_kst_now()
    
    # 1. Orders 테이블 업데이트
    cur.execute(SQL_QUARANTINE_PROD_ORDERS, (
        CODE_FRAUD_PROD, str(pid), str(current_order_id), current_timestamp
    ))
    row_count = cur.rowcount
    
    # 2. Events 테이블 업데이트 (✅ 추가됨)
    cur.execute(SQL_QUARANTINE_PROD_EVENTS, (
        CODE_FRAUD_PROD,            # SET reason_code
        str(pid),                   # WHERE (subquery)
        current_timestamp,          # WHERE time
        str(current_order_id)       # exclude current
    ))
    
    return row_count

# =========================================================
# 7. 🚀 실행
# =========================================================
if __name__ == "__main__":
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False 
    
    abuse_tracker = {}
    product_rate_tracker = defaultdict(lambda: deque())

    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=[BOOTSTRAP_SERVERS],
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id=GROUP_ID,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    print(f"📡 [Risk Consumer] Events 소급 적용까지 완벽 동기화")

    try:
        for message in consumer:
            order = message.value
            risk_reason = check_risk(order, abuse_tracker, product_rate_tracker, conn)
            is_hold = True if (risk_reason or order.get('current_stage') == 'HOLD') else False

            try:
                with conn.cursor() as cur:
                    final_status = save_to_db(cur, order, is_hold, risk_reason, message.offset)

                    if risk_reason == CODE_FRAUD_USER:
                        # 유저 도배: Orders, Events 모두 업데이트
                        count = quarantine_retroactive_user(cur, order['user_id'], order['product_id'], order['order_id'])
                        if count > 0:
                            print(f"🚩 [QUARANTINE-USER] {count}건 (Orders+Events) 강제 HOLD")

                    elif risk_reason == CODE_FRAUD_PROD:
                        # 상품 폭주: Orders, Events 모두 업데이트
                        count = quarantine_retroactive_prod(cur, order['product_id'], order['order_id'])
                        if count > 0:
                            print(f"🚩 [QUARANTINE-PROD] {count}건 (Orders+Events) 강제 HOLD")

                    conn.commit()
                print(f"[{final_status}] {order['order_id']} | Reason: {risk_reason}")

            except Exception as e:
                conn.rollback()
                print(f"❌ DB Error: {e}")

    except KeyboardInterrupt:
        conn.close()
        consumer.close()