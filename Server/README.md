# AI 실시간 불량 판별 시스템 배포 가이드

이 프로젝트는 역할을 두 가지로 분리하여 서로 다른 컴퓨터에서 실행할 수 있도록 구성되어 있습니다.

1. **DB / Kafka 서버**: 센서 데이터를 수집하고 데이터베이스를 호스팅하는 메인 서버
2. **AI 추론 서버**: Kafka에서 데이터를 실시간으로 가져와 정상/불량을 판별하는 엔진 서버

---

## 1. 메인 서버 (DB 및 Kafka) 실행 방법

현재 이 폴더가 있는 "메인 컴퓨터"에서 수행하는 작업입니다.

1. 터미널을 열고 `DBserver` 폴더로 이동합니다.
   ```powershell
   cd DBserver
   ```
2. 도커를 통해 Kafka Broker와 MongoDB를 백그라운드로 실행합니다.
   ```powershell
   docker-compose -f kafka-compose.yml up -d
   ```
3. 실행 확인
   ```powershell
   docker ps
   ```
   *정상적으로 실행되었다면 `ai_kafka_broker`, `ai_zookeeper`, `ai_pipeline_db` 컨테이너가 켜져 있어야 합니다.*

---

## 2. AI 추론 서버 (다른 컴퓨터) 실행 방법

**추론 엔진 전용** 컴퓨터에서 수행하는 작업입니다.

### 사전 준비 (파일 복사)
메인 컴퓨터에 있는 `docker_inference` 폴더 전체를 복사하여 추론용 컴퓨터에 붙여넣기 합니다.

### 설정 수정
1. 복사해 온 `docker_inference` 폴더 안에 있는 `docker-compose.yml` 파일을 엽니다.
2. `KAFKA_BROKER` 부분의 IP 주소를 **메인 서버(DB 및 Kafka가 켜져있는 컴퓨터)의 IP**로 수정하고 저장합니다.
   ```yaml
   # 예시 (DB서버 IP가 100.70.106.105인 경우)
   environment:
     - KAFKA_BROKER=100.70.106.105:9092
   ```

### 실행
1. 터미널을 열고 `docker_inference` 폴더로 이동합니다.
2. 아래 명령어를 실행하여 도커를 빌드하고 켭니다.
   ```powershell
   docker-compose up -d --build
   ```
3. **실시간 판별 결과 확인**
   프로듀서(엣지 기기)가 데이터를 전송하기 시작하면, 터미널에서 다음 명령어를 통해 실시간 추론 결과를 확인할 수 있습니다.
   ```powershell
   docker logs -f ai_inference_engine
   ```
