import paho.mqtt.client as mqtt
import json
import time
import threading
import random
from datetime import datetime

class MultiMQTTClient:
    def __init__(self, client_id, broker_host='localhost', broker_port=1883):
        self.client_id = client_id
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.client = mqtt.Client(client_id=client_id)
        self.messages_received = []
        self.is_connected = False
        
        # 콜백 함수 설정
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.on_message = self.on_message
        self.client.on_subscribe = self.on_subscribe
        self.client.on_publish = self.on_publish
        
    def on_connect(self, client, userdata, flags, rc):
        """연결 콜백"""
        if rc == 0:
            self.is_connected = True
            print(f"[{self.client_id}] 서버에 연결되었습니다.")
        else:
            print(f"[{self.client_id}] 연결 실패. 코드: {rc}")
    
    def on_disconnect(self, client, userdata, rc):
        """연결 해제 콜백"""
        self.is_connected = False
        print(f"[{self.client_id}] 연결 해제되었습니다.")
    
    def on_message(self, client, userdata, msg):
        """메시지 수신 콜백"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        message_info = {
            'timestamp': timestamp,
            'topic': msg.topic,
            'payload': msg.payload.decode(),
            'client_id': self.client_id
        }
        self.messages_received.append(message_info)
        print(f"[{self.client_id}] 메시지 수신 - 토픽: {msg.topic}, 페이로드: {msg.payload.decode()}")
    
    def on_subscribe(self, client, userdata, mid, granted_qos):
        """구독 콜백"""
        print(f"[{self.client_id}] 구독 완료. QoS: {granted_qos}")
    
    def on_publish(self, client, userdata, mid):
        """발행 콜백"""
        print(f"[{self.client_id}] 메시지 발행 완료. 메시지 ID: {mid}")
    
    def connect(self):
        """서버에 연결"""
        try:
            self.client.connect(self.broker_host, self.broker_port, 60)
            self.client.loop_start()
            return True
        except Exception as e:
            print(f"[{self.client_id}] 연결 실패: {e}")
            return False
    
    def disconnect(self):
        """서버에서 연결 해제"""
        self.client.loop_stop()
        self.client.disconnect()
    
    def subscribe(self, topic, qos=0):
        """토픽 구독"""
        result = self.client.subscribe(topic, qos)
        print(f"[{self.client_id}] 토픽 {topic} 구독 요청")
        return result
    
    def publish(self, topic, message, qos=0):
        """메시지 발행"""
        result = self.client.publish(topic, message, qos)
        print(f"[{self.client_id}] 토픽 {topic}에 메시지 발행: {message}")
        return result
    
    def get_message_count(self):
        """수신한 메시지 개수 반환"""
        return len(self.messages_received)
    
    def print_messages(self):
        """수신한 메시지들 출력"""
        print(f"\n[{self.client_id}] 수신한 메시지들 ({len(self.messages_received)}개):")
        for msg in self.messages_received:
            print(f"  [{msg['timestamp']}] {msg['topic']}: {msg['payload']}")

def publisher_client(client_id, topics, duration=30):
    """발행자 클라이언트"""
    client = MultiMQTTClient(client_id)
    
    if client.connect():
        print(f"[{client_id}] 발행자 시작")
        
        start_time = time.time()
        message_count = 0
        
        while time.time() - start_time < duration:
            topic = random.choice(topics)
            message = {
                "client_id": client_id,
                "message_id": message_count,
                "timestamp": datetime.now().isoformat(),
                "data": f"메시지 {message_count} from {client_id}"
            }
            
            client.publish(topic, json.dumps(message, ensure_ascii=False))
            message_count += 1
            time.sleep(random.uniform(1, 3))  # 1-3초 랜덤 대기
        
        print(f"[{client_id}] 발행자 종료 - 총 {message_count}개 메시지 발행")
        client.disconnect()

def subscriber_client(client_id, topics, duration=30):
    """구독자 클라이언트"""
    client = MultiMQTTClient(client_id)
    
    if client.connect():
        print(f"[{client_id}] 구독자 시작")
        
        # 토픽 구독
        for topic in topics:
            client.subscribe(topic)
        
        # 지정된 시간 동안 메시지 수신 대기
        time.sleep(duration)
        
        print(f"[{client_id}] 구독자 종료 - 총 {client.get_message_count()}개 메시지 수신")
        client.print_messages()
        client.disconnect()

def test_scenario_1():
    """시나리오 1: 2개 발행자 + 3개 구독자"""
    print("=== 시나리오 1: 2개 발행자 + 3개 구독자 ===")
    
    topics = ["sensor/temperature", "sensor/humidity", "device/status"]
    
    # 구독자들 시작
    subscribers = []
    for i in range(3):
        client_id = f"subscriber_{i+1}"
        thread = threading.Thread(
            target=subscriber_client,
            args=(client_id, topics, 25)
        )
        thread.daemon = True
        subscribers.append(thread)
        thread.start()
    
    time.sleep(2)  # 구독자들이 준비될 때까지 대기
    
    # 발행자들 시작
    publishers = []
    for i in range(2):
        client_id = f"publisher_{i+1}"
        thread = threading.Thread(
            target=publisher_client,
            args=(client_id, topics, 20)
        )
        thread.daemon = True
        publishers.append(thread)
        thread.start()
    
    # 모든 스레드 완료 대기
    for thread in publishers + subscribers:
        thread.join()
    
    print("=== 시나리오 1 완료 ===")

def test_scenario_2():
    """시나리오 2: 채팅방 시뮬레이션"""
    print("=== 시나리오 2: 채팅방 시뮬레이션 ===")
    
    chat_topic = "chat/room1"
    
    # 채팅 참여자들 (모두 구독자이자 발행자)
    participants = []
    for i in range(5):
        client_id = f"user_{i+1}"
        thread = threading.Thread(
            target=chat_participant,
            args=(client_id, chat_topic, 30)
        )
        thread.daemon = True
        participants.append(thread)
        thread.start()
    
    # 모든 스레드 완료 대기
    for thread in participants:
        thread.join()
    
    print("=== 시나리오 2 완료 ===")

def chat_participant(client_id, topic, duration):
    """채팅 참여자 (구독자 + 발행자)"""
    client = MultiMQTTClient(client_id)
    
    if client.connect():
        print(f"[{client_id}] 채팅방 입장")
        
        # 채팅방 구독
        client.subscribe(topic)
        
        start_time = time.time()
        message_count = 0
        
        while time.time() - start_time < duration:
            # 랜덤하게 메시지 발행
            if random.random() < 0.3:  # 30% 확률로 메시지 발행
                message = {
                    "user": client_id,
                    "message": f"안녕하세요! 메시지 {message_count}입니다.",
                    "timestamp": datetime.now().isoformat()
                }
                client.publish(topic, json.dumps(message, ensure_ascii=False))
                message_count += 1
            
            time.sleep(random.uniform(2, 5))
        
        print(f"[{client_id}] 채팅방 퇴장 - 총 {message_count}개 메시지 발행, {client.get_message_count()}개 메시지 수신")
        client.print_messages()
        client.disconnect()

def test_scenario_3():
    """시나리오 3: 센서 데이터 수집 시뮬레이션"""
    print("=== 시나리오 3: 센서 데이터 수집 시뮬레이션 ===")
    
    sensor_topics = ["sensor/temperature", "sensor/humidity", "sensor/pressure"]
    controller_topics = ["controller/command", "controller/status"]
    
    # 센서들 (발행자)
    sensors = []
    for i in range(3):
        client_id = f"sensor_{i+1}"
        topic = sensor_topics[i]
        thread = threading.Thread(
            target=sensor_simulator,
            args=(client_id, topic, 25)
        )
        thread.daemon = True
        sensors.append(thread)
        thread.start()
    
    # 컨트롤러 (구독자 + 발행자)
    controller_thread = threading.Thread(
        target=controller_simulator,
        args=("controller_1", sensor_topics, controller_topics, 25)
    )
    controller_thread.daemon = True
    controller_thread.start()
    
    # 모든 스레드 완료 대기
    for thread in sensors + [controller_thread]:
        thread.join()
    
    print("=== 시나리오 3 완료 ===")

def sensor_simulator(client_id, topic, duration):
    """센서 시뮬레이터"""
    client = MultiMQTTClient(client_id)
    
    if client.connect():
        print(f"[{client_id}] 센서 시작 - 토픽: {topic}")
        
        start_time = time.time()
        message_count = 0
        
        while time.time() - start_time < duration:
            # 센서 데이터 생성
            if "temperature" in topic:
                value = random.uniform(20, 30)
                unit = "celsius"
            elif "humidity" in topic:
                value = random.uniform(40, 80)
                unit = "percent"
            else:  # pressure
                value = random.uniform(1000, 1020)
                unit = "hPa"
            
            message = {
                "sensor_id": client_id,
                "value": round(value, 2),
                "unit": unit,
                "timestamp": datetime.now().isoformat()
            }
            
            client.publish(topic, json.dumps(message, ensure_ascii=False))
            message_count += 1
            time.sleep(random.uniform(1, 2))
        
        print(f"[{client_id}] 센서 종료 - 총 {message_count}개 데이터 발행")
        client.disconnect()

def controller_simulator(client_id, sensor_topics, controller_topics, duration):
    """컨트롤러 시뮬레이터"""
    client = MultiMQTTClient(client_id)
    
    if client.connect():
        print(f"[{client_id}] 컨트롤러 시작")
        
        # 센서 데이터 구독
        for topic in sensor_topics:
            client.subscribe(topic)
        
        # 컨트롤러 명령 구독
        client.subscribe(controller_topics[0])
        
        start_time = time.time()
        command_count = 0
        
        while time.time() - start_time < duration:
            # 주기적으로 상태 보고
            if int(time.time() - start_time) % 10 == 0:
                status_message = {
                    "controller_id": client_id,
                    "status": "online",
                    "sensors_connected": len(sensor_topics),
                    "timestamp": datetime.now().isoformat()
                }
                client.publish(controller_topics[1], json.dumps(status_message, ensure_ascii=False))
            
            # 랜덤하게 명령 발행
            if random.random() < 0.1:  # 10% 확률로 명령 발행
                command_message = {
                    "command": random.choice(["start", "stop", "reset"]),
                    "target": random.choice(sensor_topics),
                    "timestamp": datetime.now().isoformat()
                }
                client.publish(controller_topics[0], json.dumps(command_message, ensure_ascii=False))
                command_count += 1
            
            time.sleep(1)
        
        print(f"[{client_id}] 컨트롤러 종료 - 총 {command_count}개 명령 발행, {client.get_message_count()}개 메시지 수신")
        client.print_messages()
        client.disconnect()

def main():
    """메인 함수"""
    print("=== 다중 MQTT 클라이언트 테스트 ===")
    print("1. 기본 테스트 (2개 발행자 + 3개 구독자)")
    print("2. 채팅방 시뮬레이션 (5개 참여자)")
    print("3. 센서 데이터 수집 시뮬레이션")
    print("4. 모든 테스트 실행")
    
    choice = input("선택하세요 (1-4): ")
    
    if choice == "1":
        test_scenario_1()
    elif choice == "2":
        test_scenario_2()
    elif choice == "3":
        test_scenario_3()
    elif choice == "4":
        print("\n" + "="*50)
        test_scenario_1()
        print("\n" + "="*50)
        test_scenario_2()
        print("\n" + "="*50)
        test_scenario_3()
    else:
        print("잘못된 선택입니다.")

if __name__ == "__main__":
    main() 