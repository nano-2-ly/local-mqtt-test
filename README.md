# MQTT 서버

Python으로 구현된 MQTT 브로커 서버입니다. IoT 디바이스 간의 메시지 교환을 위한 경량 메시징 프로토콜을 지원합니다.

## 기능

- ✅ MQTT 3.1.1 프로토콜 지원
- ✅ 클라이언트 연결 관리
- ✅ 토픽 기반 메시지 발행/구독
- ✅ QoS 0 지원 (최소 한 번 전달)
- ✅ 다중 클라이언트 동시 연결
- ✅ 실시간 로깅
- ✅ PING/PONG 연결 유지

## 설치

1. 필요한 패키지 설치:
```bash
pip install -r requirements.txt
```

## 사용법

### 1. MQTT 서버 시작

```bash
python mqtt_server.py
```

서버는 기본적으로 `0.0.0.0:1883`에서 실행됩니다.

### 2. 클라이언트 테스트

#### 단일 클라이언트 테스트
```bash
python mqtt_client_test.py
```

#### 다중 클라이언트 테스트 (고급)
```bash
python multi_client_test.py
```
- 시나리오 1: 2개 발행자 + 3개 구독자
- 시나리오 2: 채팅방 시뮬레이션 (5개 참여자)
- 시나리오 3: 센서 데이터 수집 시뮬레이션

#### 다중 클라이언트 테스트 (간단)
```bash
python simple_multi_test.py
```
- 기본 테스트: 3개 구독자 + 2개 발행자
- 채팅 테스트: 5개 참여자
- 센서 테스트: 3개 센서 + 1개 모니터

## 파일 구조

```
server/
├── mqtt_server.py          # MQTT 서버 메인 파일
├── mqtt_client_test.py     # 단일 클라이언트 테스트 파일
├── multi_client_test.py    # 고급 다중 클라이언트 테스트
├── simple_multi_test.py    # 간단한 다중 클라이언트 테스트
├── requirements.txt         # 필요한 패키지 목록
├── README.md              # 사용법 설명
└── mqtt_server.log        # 서버 로그 파일 (자동 생성)
```

## 주요 클래스

### MQTTServer
- 서버 소켓 관리
- 클라이언트 연결 처리
- 토픽 구독 관리
- 메시지 라우팅

### MQTTClient
- 개별 클라이언트 연결 관리
- MQTT 패킷 처리
- 메시지 송수신

## 지원하는 MQTT 패킷

- **CONNECT**: 클라이언트 연결
- **CONNACK**: 연결 응답
- **PUBLISH**: 메시지 발행
- **SUBSCRIBE**: 토픽 구독
- **SUBACK**: 구독 응답
- **UNSUBSCRIBE**: 구독 해제
- **UNSUBACK**: 구독 해제 응답
- **PINGREQ**: 연결 유지 요청
- **PINGRESP**: 연결 유지 응답
- **DISCONNECT**: 연결 해제

## 로그

서버는 다음 정보를 로그로 기록합니다:
- 클라이언트 연결/해제
- 토픽 구독/해제
- 메시지 발행/수신
- 오류 및 예외 상황

로그 파일: `mqtt_server.log`

## 예제 사용 시나리오

### IoT 센서 데이터 수집
```
토픽: sensor/temperature
메시지: {"temperature": 25.5, "unit": "celsius"}

토픽: sensor/humidity  
메시지: {"humidity": 60.2, "unit": "percent"}
```

### 디바이스 상태 모니터링
```
토픽: device/status
메시지: {"status": "online", "timestamp": 1234567890}
```

## 주의사항

- 현재 QoS 0만 지원합니다
- 인증 및 암호화는 구현되지 않았습니다
- 프로덕션 환경에서는 추가적인 보안 조치가 필요합니다

## 라이선스

이 프로젝트는 교육 및 개발 목적으로 제작되었습니다. 