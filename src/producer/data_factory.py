import time
import random
import uuid
import json
from datetime import datetime
from faker import Faker

fake = Faker('ko_KR')

# ---------------------------------------------------------
# 1. 상태 리스트 (이미지 속 7개 상태 완벽 반영)
# ---------------------------------------------------------
STATUS_OPTS = [
    {"stage": "ORDER",       "status": "PAID",              "event": "PAYMENT_CONFIRMED"}, # 결제완료
    {"stage": "FULFILLMENT", "status": "PICKING",           "event": "PICKING_STARTED"},   # 피킹중
    {"stage": "FULFILLMENT", "status": "PACKED",            "event": "PACKING_COMPLETED"}, # 포장완료
    {"stage": "SHIPMENT",    "status": "SHIPPED",           "event": "SHIPMENT_STARTED"},  # 출고/발송완료
    {"stage": "SHIPMENT",    "status": "DELIVERED",         "event": "DELIVERY_COMPLETED"},# 배송완료
    {"stage": "ORDER",       "status": "CANCELED",          "event": "ORDER_CANCELED"},    # 취소
    {"stage": "ORDER",       "status": "HOLD",              "event": "ORDER_HOLD"},        # 보류
]

# ---------------------------------------------------------
# 2. 50종 전체 상품 카탈로그 (ELEC, CLOTH, FOOD, BOOK, TEST)
# ---------------------------------------------------------
FULL_PRODUCT_CATALOG = [
    # 1. 전자제품
    {"id": "ELEC-001", "name": "맥북 프로 16인치 M3"}, {"id": "ELEC-002", "name": "갤럭시북4 울트라"},
    {"id": "ELEC-003", "name": "아이패드 에어 6세대"}, {"id": "ELEC-004", "name": "소니 노이즈캔슬링 헤드폰 XM5"},
    {"id": "ELEC-005", "name": "LG 울트라기어 32인치 모니터"}, {"id": "ELEC-006", "name": "로지텍 MX Master 3S 마우스"},
    {"id": "ELEC-007", "name": "기계식 키보드 (적축)"}, {"id": "ELEC-008", "name": "C타입 고속 충전기 65W"},
    {"id": "ELEC-009", "name": "HDMI 2.1 케이블"}, {"id": "ELEC-010", "name": "스마트폰 짐벌 안정기"},
    # 2. 의류/패션
    {"id": "CLOTH-001", "name": "남성용 기본 무지 티셔츠 (L)"}, {"id": "CLOTH-002", "name": "남성용 기본 무지 티셔츠 (XL)"},
    {"id": "CLOTH-003", "name": "여성용 슬림핏 청바지 (27)"}, {"id": "CLOTH-004", "name": "여성용 슬림핏 청바지 (28)"},
    {"id": "CLOTH-005", "name": "유니섹스 후드 집업 (Grey)"}, {"id": "CLOTH-006", "name": "스포츠 러닝 양말 3팩"},
    {"id": "CLOTH-007", "name": "방수 윈드브레이커 자켓"}, {"id": "CLOTH-008", "name": "캔버스 에코백 (Ivory)"},
    {"id": "CLOTH-009", "name": "베이스볼 캡 모자 (Black)"}, {"id": "CLOTH-010", "name": "겨울용 스마트폰 터치 장갑"},
    # 3. 식품/생필품
    {"id": "FOOD-001", "name": "제주 삼다수 2L x 6개입"}, {"id": "FOOD-002", "name": "신라면 멀티팩 (5개입)"},
    {"id": "FOOD-003", "name": "햇반 210g x 12개입"}, {"id": "FOOD-004", "name": "서울우유 1L"},
    {"id": "FOOD-005", "name": "유기농 바나나 1송이"}, {"id": "FOOD-006", "name": "냉동 닭가슴살 1kg"},
    {"id": "FOOD-007", "name": "맥심 모카골드 믹스커피 100T"}, {"id": "FOOD-008", "name": "3겹 데코 롤휴지 30롤"},
    {"id": "FOOD-009", "name": "물티슈 100매 캡형"}, {"id": "FOOD-010", "name": "KF94 마스크 대형 50매"},
    # 4. 도서/취미
    {"id": "BOOK-001", "name": "데이터 엔지니어링 교과서"}, {"id": "BOOK-002", "name": "파이썬으로 시작하는 데이터 분석"},
    {"id": "BOOK-003", "name": "SQL 레벨업 가이드"}, {"id": "BOOK-004", "name": "해리포터 전집 세트"},
    {"id": "BOOK-005", "name": "닌텐도 스위치 OLED 게임기"}, {"id": "BOOK-006", "name": "젤다의 전설 게임 타이틀"},
    {"id": "BOOK-007", "name": "건담 프라모델 (MG 등급)"}, {"id": "BOOK-008", "name": "전문가용 48색 색연필"},
    {"id": "BOOK-009", "name": "요가 매트 (10mm)"}, {"id": "BOOK-010", "name": "캠핑용 접이식 의자"},
    # 5. 테스트용
    {"id": "TEST-001", "name": "한정판 스니커즈 (품절임박)"}, {"id": "TEST-002", "name": "인기 아이돌 앨범 (재고부족)"},
    {"id": "TEST-003", "name": "단종된 레거시 상품"}, {"id": "TEST-004", "name": "이벤트 경품 (선착순)"},
    {"id": "TEST-005", "name": "창고 깊숙한 곳 악성재고"}, {"id": "TEST-006", "name": "시스템 오류 유발 상품 A"},
    {"id": "TEST-007", "name": "시스템 오류 유발 상품 B"}, {"id": "TEST-008", "name": "배송 지연 예상 상품"},
    {"id": "TEST-009", "name": "합포장 테스트용 상품 A"}, {"id": "TEST-010", "name": "합포장 테스트용 상품 B"}
]

def build_order_json(c_id=None, prod=None, addr=None, status_idx=0):
    """단일 주문 데이터 조립"""
    product = prod if prod else random.choice(FULL_PRODUCT_CATALOG)
    customer_id = c_id if c_id else f"{fake.user_name()}{random.randint(100, 999)}"
    address = addr if addr else fake.address()
    cfg = STATUS_OPTS[status_idx]
    
    return {
        "order_id": f"ORD-{uuid.uuid4()}",
        "product_id": product["id"],
        "product_name": product["name"],
        "current_stage": cfg["stage"],
        "current_status": cfg["status"],
        "last_event_type": cfg["event"],
        "last_occurred_at": datetime.now().isoformat(),
        "customer_id": customer_id,
        "address": address,
        "updated_at": datetime.now().isoformat()
    }

# ---------------------------------------------------------
# 🚀 메인 스트림 실행
# ---------------------------------------------------------
if __name__ == "__main__":
    print("📡 [Producer] 전체 상품 반영 및 복합 시나리오 가동 중...")
    
    try:
        while True:
            dice = random.random()
            
            # 1️⃣ 시나리오: 다수 유저의 인기 상품 폭주 (10%)
            if dice < 0.10:
                hot_prod = random.choice(FULL_PRODUCT_CATALOG)
                burst_size = random.randint(10, 20)
                print(f"🔥 [BURST] '{hot_prod['name']}'에 주문 폭탄! ({burst_size}건)")
                for _ in range(burst_size):
                    data = build_order_json(prod=hot_prod, status_idx=0)
                    print(json.dumps(data, ensure_ascii=False))
                    time.sleep(0.02) # 0.02초 간격 연사

            # 2️⃣ 시나리오: 특정 유저의 어뷰징/도배 (5%)
            elif dice < 0.15:
                abuser_id = f"ABUSER_{random.randint(10, 99)}"
                abuse_prod = random.choice(FULL_PRODUCT_CATALOG)
                print(f"🚨 [ABUSE] 유저 {abuser_id}가 '{abuse_prod['name']}' 연사 시도!")
                for _ in range(7):
                    data = build_order_json(c_id=abuser_id, prod=abuse_prod, status_idx=0)
                    print(json.dumps(data, ensure_ascii=False))
                    time.sleep(0.04)

            # 3️⃣ 시나리오: 전체 상품 중 랜덤 재고 부족 유발 (10%)
            elif dice < 0.25:
                stock_target = random.choice(FULL_PRODUCT_CATALOG)
                data = build_order_json(prod=stock_target, status_idx=0)
                print(f"📦 [STOCK_CHECK] 재고 확인용 주문: {data['product_id']}")
                print(json.dumps(data, ensure_ascii=False))

            # 4️⃣ 시나리오: 주소 오염 데이터 (5%)
            elif dice < 0.30:
                bad_addr = random.choice(["???", "Unknown", "123", "Seoul"])
                data = build_order_json(addr=bad_addr, status_idx=0)
                print(f"🏠 [BAD_ADDR] 오염 데이터: {bad_addr}")
                print(json.dumps(data, ensure_ascii=False))

            # 5️⃣ 평시: 정상 주문 (70%)
            else:
                data = build_order_json()
                print(f"✅ [NORMAL] {data['current_status']}: {data['customer_id']}")

            time.sleep(random.uniform(0.5, 1.5))
            
    except KeyboardInterrupt:
        print("\n🛑 생성 종료")