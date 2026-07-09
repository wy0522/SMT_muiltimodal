import os
import time
import json
import base64
from kafka import KafkaProducer

# ----------------- 설정 -----------------
KAFKA_BROKER = '100.70.106.105:9092'  # 또는 실제 Tailscale IP
KAFKA_TOPIC = 'class'       # 엣지 샘플 전송용 별도 토픽
BASE_DIR = os.path.join(os.path.dirname(__file__), 'kafkadata', 'test')

def encode_file_to_base64(filepath):
    """파일을 읽어 Base64 문자열로 반환합니다."""
    if not os.path.exists(filepath):
        return None
    try:
        with open(filepath, 'rb') as f:
            return base64.b64encode(f.read()).decode('utf-8')
    except Exception as e:
        print(f"파일 인코딩 오류 ({filepath}): {e}")
        return None

def read_json_file(filepath):
    """JSON 파일을 읽어 파이썬 딕셔너리로 반환합니다."""
    if not os.path.exists(filepath):
        return None
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"JSON 읽기 오류 ({filepath}): {e}")
        return None

def group_files_by_basename(directory):
    """
    디렉토리 내의 파일들을 기본 이름(확장자 제외)으로 그룹핑합니다.
    반환 예시: {'PR_DEF...01576': ['.jpg', '.json', '.xlsx'], ...}
    """
    groups = {}
    if not os.path.exists(directory):
        return groups
        
    for filename in os.listdir(directory):
        if not os.path.isfile(os.path.join(directory, filename)):
            continue
            
        basename, ext = os.path.splitext(filename)
        if basename not in groups:
            groups[basename] = {}
        groups[basename][ext.lower()] = os.path.join(directory, filename)
        
    return groups

def start_producer():
    print(f"Kafka 브로커({KAFKA_BROKER}) 접속 준비 중...")
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            # JSON 형태로 자동 직렬화 (Base64가 포함되므로 크기가 커질 수 있음)
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            max_request_size=5242880,  # 5MB로 요청 크기 제한 증가 (이미지+엑셀 대비)
            request_timeout_ms=10000,
            max_block_ms=10000
        )
        print("Kafka 브로커 연결 성공!")
    except Exception as e:
        print(f"Kafka 연결 실패: {e}")
        return

    # pr, sd 폴더를 각각 확인
    categories = ['pr', 'sd']
    
    for category in categories:
        cat_dir = os.path.join(BASE_DIR, category)
        print(f"\n[{category}] 디렉토리 데이터 스캔 중... ({cat_dir})")
        
        # 1. 파일들을 그룹핑
        file_groups = group_files_by_basename(cat_dir)
        
        # 2. 각 그룹(세트)마다 순차적으로 전송
        for basename, files in file_groups.items():
            print(f"\n데이터 세트 준비 중: {basename}")
            
            # JSON, JPG, XLSX 경로 추출
            json_path = files.get('.json')
            jpg_path = files.get('.jpg')
            xlsx_path = files.get('.xlsx')
            
            # 메타데이터 및 파일 바이너리 추출
            metadata = read_json_file(json_path) if json_path else {}
            image_b64 = encode_file_to_base64(jpg_path) if jpg_path else ""
            excel_b64 = encode_file_to_base64(xlsx_path) if xlsx_path else ""
            
            # Kafka로 보낼 거대한 페이로드 구성
            payload = {
                "category": category,
                "base_name": basename,
                # 타임스탬프 등 추가 정보 추출 (예: '20250902-180241' 부분)
                "timestamp": basename.split('_')[-2] if '_' in basename else str(time.time()),
                "metadata": metadata,
                "image_base64": image_b64,
                "excel_base64": excel_b64
            }
            
            # 3. 데이터 전송
            try:
                print(f"토픽 '{KAFKA_TOPIC}'으로 메시지 전송 시도...")
                future = producer.send(KAFKA_TOPIC, value=payload)
                # 전송 대기 (동기식)
                record_metadata = future.get(timeout=10)
                print(f"전송 성공! (파티션: {record_metadata.partition}, 오프셋: {record_metadata.offset})")
            except Exception as e:
                print(f"전송 실패: {e}")
            
            # 5초 대기 후 다음 세트 전송
            print("5초 대기 중...")
            time.sleep(5)
            
    # 전체 완료
    producer.flush()
    producer.close()
    print("\n모든 폴더 스캔 및 데이터 전송이 완료되었습니다.")

if __name__ == "__main__":
    start_producer()
