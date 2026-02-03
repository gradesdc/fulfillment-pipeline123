import time
import json
import os
import random
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# 작성하신 데이터 생성 로직이 들어있는 파일에서 함수를 가져옵니다.
# 만약 한 파일에 합치려면 아래에 build_order_json 함수 등을 위치시키면 됩니다.
from src.producer.data_factory import build_order_json, FULL_PRODUCT_CATALOG, STATUS_OPTS

# ---------------------------------------------------------
# ⚙️ 카프카 접속 및 토픽 설정
# ---------------------------------------------------------
BOOTSTRAP_SERVERS = os.getenv('BOOTSTRAP_SERVERS', 'localhost:9092')
TOPIC_NAME = 'event'

def create_producer():
    """카프카 브로커 연결 시도 (연결될 때까지 재시도)"""
    producer = None
    print(f"📡 카프카 브로커 연결 시도 중... ({BOOTSTRAP_SERVERS})")
    
    while not producer:
        try:
            producer = KafkaProducer(
                bootstrap_servers=[BOOTSTRAP_SERVERS],
                # JSON 직렬화 & 한글 깨짐 방지
                value_serializer=lambda x: json.dumps(x, ensure_ascii=False).encode('utf-8'),
                # 대량 전송 안정성을 위한 설정
                acks=1,
                retries=5
            )
            print("✅ 카프카 연결 성공!")
        except NoBrokersAvailable:
            print("⏳ 브로커를 찾을 수 없습니다. 3초 후 재시도...")
            time.sleep(3)
    return producer

# ---------------------------------------------------------
# 🚀 메인 실행부
# ---------------------------------------------------------
if __name__ == "__main__":
    producer = create_producer()
    print(f"🚀 [프로듀서] '{TOPIC_NAME}' 토픽으로 복합 시나리오 데이터 전송 시작...\n")

    try:
        while True:
            dice = random.random()
            batch = []
            scenario_name = ""

            # -------------------------------------------------------
            # 🎲 시나리오 선택 로직 (작성하신 코드 반영)
            # -------------------------------------------------------
            
            # 1️⃣ 시나리오: 다수 유저의 인기 상품 폭주 (10%)
            if dice < 0.10:
                hot_prod = random.choice(FULL_PRODUCT_CATALOG)
                burst_size = random.randint(10, 20)
                scenario_name = f"🔥 [BURST] {hot_prod['name']} ({burst_size}건)"
                for _ in range(burst_size):
                    batch.append((build_order_json(prod=hot_prod, status_idx=0), 0.02))

            # 2️⃣ 시나리오: 특정 유저의 어뷰징/도배 (5%)
            elif dice < 0.15:
                abuser_id = f"ABUSER_{random.randint(10, 99)}"
                abuse_prod = random.choice(FULL_PRODUCT_CATALOG)
                scenario_name = f"🚨 [ABUSE] {abuser_id} 연사"
                for _ in range(6):
                    batch.append((build_order_json(c_id=abuser_id, prod=abuse_prod, status_idx=0), 0.04))

            # 3️⃣ 시나리오: 전체 상품 중 랜덤 재고 부족 유발 (10%)
            elif dice < 0.25:
                stock_target = random.choice(FULL_PRODUCT_CATALOG)
                scenario_name = f"📦 [STOCK_CHECK] {stock_target['id']}"
                batch.append((build_order_json(prod=stock_target, status_idx=0), 0))

            # 4️⃣ 시나리오: 주소 오염 데이터 (5%)
            elif dice < 0.30:
                bad_addr = random.choice(["???", "Unknown", "123", "Seoul"])
                scenario_name = f"🏠 [BAD_ADDR] {bad_addr}"
                batch.append((build_order_json(addr=bad_addr, status_idx=0), 0))

            # 5️⃣ 평시: 정상 주문 (70%)
            else:
                order = build_order_json()
                scenario_name = f"✅ [NORMAL] {order['current_status']}"
                batch.append((order, 0))

            # -------------------------------------------------------
            # 📦 Kafka 전송 및 로그 출력
            # -------------------------------------------------------
            if len(batch) > 1:
                print(f"{scenario_name} 시나리오 전송 시작...")

            for i, (msg, interval) in enumerate(batch):
                producer.send(TOPIC_NAME, value=msg)
                
                # 로그 출력 (단건 주문일 때만 상세 출력, 버스트는 요약)
                if len(batch) == 1:
                    print(f"{scenario_name} | {msg['customer_id']} | {msg['product_name']}")
                
                # 버스트 모드일 때의 미세 간격 조절 (0.02초 등)
                if interval > 0:
                    time.sleep(interval)

            producer.flush() # 배송 완료 보장
            
            if len(batch) > 1:
                print(f"   └─ {len(batch)}건 전송 완료.")

            # 시나리오 사이의 기본 대기 시간 (0.5초 ~ 1.5초)
            time.sleep(random.uniform(0.5, 1.5))

    except KeyboardInterrupt:
        print("\n🛑 프로듀서를 종료합니다.")
        if producer:
            producer.close()