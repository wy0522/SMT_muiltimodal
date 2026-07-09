import json
from kafka import KafkaConsumer

# Consumer가 실행될 컴퓨터(외부망 서버) 기준의 Kafka 브로커 주소
# 해당 서버 내부에서 실행한다면 'localhost:9092' 또는 해당 서버의 내부망/Tailscale IP를 사용합니다.
KAFKA_BROKER = '100.70.106.105:9092'  # 또는 '100.70.106.105:9092'
KAFKA_TOPIC = 'test_topic'       # 수신할 토픽 이름 (Producer와 동일해야 함)
GROUP_ID = 'test-consumer-group' # Consumer 그룹 ID

def start_consumer():
    """Kafka 토픽을 구독하고 메시지를 지속적으로 수신합니다."""
    print(f"🔄 Kafka 브로커({KAFKA_BROKER})의 '{KAFKA_TOPIC}' 토픽 수신 대기 중...")

    try:
        # Consumer 생성
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=[KAFKA_BROKER],
            group_id=GROUP_ID,
            # 메시지 값이 JSON 형태인 경우, 이를 다시 Python 딕셔너리로 역직렬화
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            # 처음부터 메시지를 읽어오려면 'earliest', 켜진 시점부터 읽으려면 'latest'
            auto_offset_reset='earliest',
            enable_auto_commit=True
        )

        print("✅ Consumer 연결 완료! 메시지를 기다립니다. (종료하려면 Ctrl+C)")

        # 무한 루프를 돌며 브로커로부터 메시지를 수신
        for message in consumer:
            # message는 ConsumerRecord 객체로, value 속성에 전송된 데이터가 담겨 있습니다.
            payload = message.value
            
            print("\n" + "="*50)
            print("📥 [새로운 메시지 수신]")
            print(f" - 토픽: {message.topic} (파티션: {message.partition}, 오프셋: {message.offset})")
            
            # 테스트 메시지 처리
            if payload.get("type") == "ping":
                print(f" - 내용: {payload.get('message')}")
                print(" - 상태: 통신 테스트 성공! 🎉")
            # 향후 실제 파일 데이터 전송 시 처리
            elif "filename" in payload:
                print(f" - 수신된 파일명: {payload.get('filename')}")
                print(f" - 파일 내용 (일부): {payload.get('content')[:100]}...")
            else:
                print(f" - 데이터: {payload}")
                
            print("="*50 + "\n")

    except KeyboardInterrupt:
        print("\n🛑 사용자에 의해 Consumer가 종료되었습니다.")
    except Exception as e:
        print(f"❌ Consumer 실행 중 오류 발생: {e}")
    finally:
        # 안전한 종료를 위해 Consumer를 닫아줍니다.
        if 'consumer' in locals():
            consumer.close()

if __name__ == "__main__":
    start_consumer()
