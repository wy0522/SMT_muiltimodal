import os
import time
import json
import base64
import random
from itertools import cycle
from kafka import KafkaProducer

# ----------------- 설정 -----------------
KAFKA_BROKER = '100.70.106.105:9092'
KAFKA_TOPIC = 'edge_data_topic_Goo'
BASE_DIR = os.path.join(os.path.dirname(__file__), 'kafkadata', 'testReal')
SEND_INTERVAL = 5

def encode_file_to_base64(filepath):
    if not filepath or not os.path.exists(filepath):
        return None
    try:
        with open(filepath, 'rb') as f:
            return base64.b64encode(f.read()).decode('utf-8')
    except Exception as e:
        print(f"파일 인코딩 오류 ({filepath}): {e}")
        return None

def read_json_file(filepath):
    if not filepath or not os.path.exists(filepath):
        return None
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"JSON 읽기 오류 ({filepath}): {e}")
        return None

def group_files_by_basename(directory):
    groups = {}
    if not os.path.exists(directory):
        return groups
        
    for filename in os.listdir(directory):
        filepath = os.path.join(directory, filename)
        if not os.path.isfile(filepath):
            continue
            
        basename, ext = os.path.splitext(filename)
        if basename not in groups:
            groups[basename] = {}
        groups[basename][ext.lower()] = filepath
        
    return groups

def send_data(producer, category, basename, files):
    json_path = files.get('.json')
    jpg_path = files.get('.jpg')
    xlsx_path = files.get('.xlsx')
    
    metadata = read_json_file(json_path) if json_path else {}
    image_b64 = encode_file_to_base64(jpg_path) if jpg_path else ""
    excel_b64 = encode_file_to_base64(xlsx_path) if xlsx_path else ""
    
    payload = {
        "category": category,
        "base_name": basename,
        "timestamp": basename.split('_')[-2] if '_' in basename else str(time.time()),
        "metadata": metadata,
        "image_base64": image_b64,
        "excel_base64": excel_b64
    }
    
    try:
        print(f"[{category}] 토픽 '{KAFKA_TOPIC}'으로 데이터 전송 시도... ({basename})")
        future = producer.send(KAFKA_TOPIC, value=payload)
        record_metadata = future.get(timeout=10)
        print(f"전송 성공! (파티션: {record_metadata.partition}, 오프셋: {record_metadata.offset})")
    except Exception as e:
        print(f"전송 실패: {e}")

def start_producer():
    print(f"Kafka 브로커({KAFKA_BROKER}) 접속 준비 중...")
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            max_request_size=5242880,
            request_timeout_ms=10000,
            max_block_ms=10000
        )
        print("Kafka 브로커 연결 성공!")
    except Exception as e:
        print(f"Kafka 연결 실패: {e}")
        return

    pr_groups = group_files_by_basename(os.path.join(BASE_DIR, 'pr'))
    sd_groups = group_files_by_basename(os.path.join(BASE_DIR, 'sd'))
    
    pr_keys = list(pr_groups.keys())
    sd_keys = list(sd_groups.keys())

    pr_nor_cycle = cycle(sorted([k for k in pr_keys if "NOR" in k]))
    pr_def_cycle = cycle(sorted([k for k in pr_keys if "DEF" in k]))
    
    sd_nor_cycle = cycle(sorted([k for k in sd_keys if "NOR" in k]))
    sd_def_cycle = cycle(sorted([k for k in sd_keys if "DEF" in k]))

    print("\n데이터 9:1 확률 교차 전송을 시작합니다. (중단하려면 Ctrl+C 입력)")

    try:
        while True:
            # 1. pr 폴더 전송
            if random.random() < 0.9:
                pr_basename = next(pr_nor_cycle)
                print("[분기] 90% 확률 (NOR) 선택됨")
            else:
                pr_basename = next(pr_def_cycle)
                print("[분기] 10% 확률 (DEF) 선택됨")
                
            send_data(producer, 'pr', pr_basename, pr_groups[pr_basename])
            print(f"{SEND_INTERVAL}초 대기 중...\n")
            time.sleep(SEND_INTERVAL)
            
            # 2. sd 폴더 전송
            if random.random() < 0.9:
                sd_basename = next(sd_nor_cycle)
                print("[분기] 90% 확률 (NOR) 선택됨")
            else:
                sd_basename = next(sd_def_cycle)
                print("[분기] 10% 확률 (DEF) 선택됨")
                
            send_data(producer, 'sd', sd_basename, sd_groups[sd_basename])
            print(f"{SEND_INTERVAL}초 대기 중...\n")
            time.sleep(SEND_INTERVAL)

    except KeyboardInterrupt:
        print("\n사용자에 의해 루프가 중단되었습니다.")
    finally:
        producer.flush()
        producer.close()
        print("Kafka Producer가 안전하게 종료되었습니다.")

if __name__ == "__main__":
    start_producer()