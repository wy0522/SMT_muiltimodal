import time
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import UnknownTopicOrPartitionError

# 삭제할 Kafka 브로커 및 토픽 설정
KAFKA_BROKER = '100.70.106.105:9092'
TARGET_TOPIC = 'edge_data_topic_Goo'

def recreate_topic():
    print(f"Kafka 브로커({KAFKA_BROKER})에 접속 중...")
    
    try:
        # Admin Client 생성
        admin_client = KafkaAdminClient(
            bootstrap_servers=[KAFKA_BROKER],
            request_timeout_ms=10000
        )
        
        # 1. 기존 토픽 삭제 (데이터 초기화)
        print(f"기존 '{TARGET_TOPIC}' 토픽을 삭제합니다... (기존 데이터 모두 날아감)")
        try:
            admin_client.delete_topics([TARGET_TOPIC])
            print("토픽 삭제 완료!")
            
            # 카프카가 내부적으로 토픽을 완전히 지울 때까지 약간의 시간이 필요합니다.
            print("토픽이 완전히 삭제될 때까지 5초 대기...")
            time.sleep(5)
        except UnknownTopicOrPartitionError:
            print("삭제하려는 토픽이 이미 존재하지 않습니다. (삭제 통과)")
        except Exception as e:
            print(f"토픽 삭제 중 오류: {e}")
            
        # 2. 깨끗한 상태로 토픽 다시 생성
        print(f"'{TARGET_TOPIC}' 토픽을 새롭게 생성합니다...")
        try:
            # 파티션 1개, 복제본 1개인 기본 토픽 생성
            new_topic = NewTopic(name=TARGET_TOPIC, num_partitions=1, replication_factor=1)
            admin_client.create_topics(new_topics=[new_topic])
            print("깨끗한 토픽이 새로 생성되었습니다! 데이터 초기화 완료.")
        except Exception as e:
            print(f"토픽 생성 중 오류: {e}")

    except Exception as e:
        print(f"Admin Client 연결 실패: {e}")
    finally:
        if 'admin_client' in locals():
            admin_client.close()

if __name__ == "__main__":
    recreate_topic()
