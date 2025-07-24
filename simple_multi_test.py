import paho.mqtt.client as mqtt
import threading
import time
import json
from datetime import datetime

def create_client(client_id):
    """MQTT 클라이언트 생성"""
    client = mqtt.Client(client_id=client_id)
    
    def on_connect(client, userdata, flags, rc):
        print(f"[{client_id}] 연결됨 (코드: {rc})")
    
    def on_message(client, userdata, msg):
        print(f"[{client_id}] 메시지 수신: {msg.topic} -> {msg.payload.decode()}")
    
    def on_disconnect(client, userdata, rc):
        print(f"[{client_id}] 연결 해제됨")
    
    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect
    
    return client

def publisher_task(client_id, topic, message_count=10):
    """발행자 태스크"""
    client = create_client(client_id)
    
    try:
        client.connect("localhost", 1883, 60)
        client.loop_start()
        
        print(f"[{client_id}] 발행자 시작 - 토픽: {topic}")
        
        for i in range(message_count):
            message = {
                "from": client_id,
                "message_id": i + 1,
                "timestamp": datetime.now().isoformat(),
                "data": f"메시지 {i + 1} from {client_id}"
            }
            
            client.publish(topic, json.dumps(message, ensure_ascii=False))
            print(f"[{client_id}] 메시지 발행: {i + 1}/{message_count}")
            time.sleep(1)
        
        print(f"[{client_id}] 발행자 완료")
        
    except Exception as e:
        print(f"[{client_id}] 오류: {e}")
    finally:
        client.loop_stop()
        client.disconnect()

def subscriber_task(client_id, topics, duration=15):
    """구독자 태스크"""
    client = create_client(client_id)
    
    try:
        client.connect("localhost", 1883, 60)
        
        # 토픽 구독
        for topic in topics:
            client.subscribe(topic)
            print(f"[{client_id}] 토픽 구독: {topic}")
        
        client.loop_start()
        print(f"[{client_id}] 구독자 시작 - {duration}초 동안 대기")
        
        time.sleep(duration)
        
        print(f"[{client_id}] 구독자 완료")
        
    except Exception as e:
        print(f"[{client_id}] 오류: {e}")
    finally:
        client.loop_stop()
        client.disconnect()

def test_basic():
    """기본 테스트: 3개 구독자 + 2개 발행자"""
    print("=== 기본 다중 클라이언트 테스트 ===")
    
    topics = ["test/topic1", "test/topic2"]
    
    # 구독자들 시작
    subscribers = []
    for i in range(3):
        client_id = f"sub_{i+1}"
        thread = threading.Thread(
            target=subscriber_task,
            args=(client_id, topics, 12)
        )
        subscribers.append(thread)
        thread.start()
    
    time.sleep(2)  # 구독자들이 준비될 때까지 대기
    
    # 발행자들 시작
    publishers = []
    for i in range(2):
        client_id = f"pub_{i+1}"
        topic = topics[i % len(topics)]
        thread = threading.Thread(
            target=publisher_task,
            args=(client_id, topic, 8)
        )
        publishers.append(thread)
        thread.start()
    
    # 모든 스레드 완료 대기
    for thread in subscribers + publishers:
        thread.join()
    
    print("=== 테스트 완료 ===")

def test_chat():
    """채팅 테스트: 5개 참여자"""
    print("=== 채팅 테스트 ===")
    
    chat_topic = "chat/room1"
    
    def chat_participant(client_id, duration=10):
        client = create_client(client_id)
        
        try:
            client.connect("localhost", 1883, 60)
            client.subscribe(chat_topic)
            client.loop_start()
            
            print(f"[{client_id}] 채팅방 입장")
            
            for i in range(5):
                message = {
                    "user": client_id,
                    "message": f"안녕하세요! {client_id}입니다. ({i+1}/5)",
                    "timestamp": datetime.now().isoformat()
                }
                client.publish(chat_topic, json.dumps(message, ensure_ascii=False))
                time.sleep(2)
            
            print(f"[{client_id}] 채팅방 퇴장")
            
        except Exception as e:
            print(f"[{client_id}] 오류: {e}")
        finally:
            client.loop_stop()
            client.disconnect()
    
    # 5개 참여자 시작
    participants = []
    for i in range(5):
        client_id = f"user_{i+1}"
        thread = threading.Thread(
            target=chat_participant,
            args=(client_id, 12)
        )
        participants.append(thread)
        thread.start()
    
    # 모든 스레드 완료 대기
    for thread in participants:
        thread.join()
    
    print("=== 채팅 테스트 완료 ===")

def test_sensors():
    """센서 테스트: 3개 센서 + 1개 모니터"""
    print("=== 센서 테스트 ===")
    
    sensor_topics = ["sensor/temp", "sensor/humidity", "sensor/pressure"]
    
    def sensor_simulator(client_id, topic, duration=10):
        client = create_client(client_id)
        
        try:
            client.connect("localhost", 1883, 60)
            client.loop_start()
            
            print(f"[{client_id}] 센서 시작: {topic}")
            
            for i in range(duration):
                # 센서 데이터 생성
                if "temp" in topic:
                    value = 20 + (i % 10)  # 20-29도
                    unit = "°C"
                elif "humidity" in topic:
                    value = 50 + (i % 20)  # 50-69%
                    unit = "%"
                else:  # pressure
                    value = 1000 + (i % 20)  # 1000-1019 hPa
                    unit = "hPa"
                
                message = {
                    "sensor": client_id,
                    "value": value,
                    "unit": unit,
                    "timestamp": datetime.now().isoformat()
                }
                
                client.publish(topic, json.dumps(message, ensure_ascii=False))
                time.sleep(1)
            
            print(f"[{client_id}] 센서 종료")
            
        except Exception as e:
            print(f"[{client_id}] 오류: {e}")
        finally:
            client.loop_stop()
            client.disconnect()
    
    def monitor_simulator(client_id, topics, duration=10):
        client = create_client(client_id)
        
        try:
            client.connect("localhost", 1883, 60)
            
            for topic in topics:
                client.subscribe(topic)
            
            client.loop_start()
            print(f"[{client_id}] 모니터 시작")
            
            time.sleep(duration)
            
            print(f"[{client_id}] 모니터 종료")
            
        except Exception as e:
            print(f"[{client_id}] 오류: {e}")
        finally:
            client.loop_stop()
            client.disconnect()
    
    # 센서들 시작
    sensors = []
    for i in range(3):
        client_id = f"sensor_{i+1}"
        topic = sensor_topics[i]
        thread = threading.Thread(
            target=sensor_simulator,
            args=(client_id, topic, 10)
        )
        sensors.append(thread)
        thread.start()
    
    time.sleep(1)  # 센서들이 시작될 때까지 대기
    
    # 모니터 시작
    monitor_thread = threading.Thread(
        target=monitor_simulator,
        args=("monitor_1", sensor_topics, 12)
    )
    monitor_thread.start()
    
    # 모든 스레드 완료 대기
    for thread in sensors + [monitor_thread]:
        thread.join()
    
    print("=== 센서 테스트 완료 ===")

def main():
    """메인 함수"""
    print("=== 간단한 다중 MQTT 클라이언트 테스트 ===")
    print("1. 기본 테스트 (3구독자 + 2발행자)")
    print("2. 채팅 테스트 (5참여자)")
    print("3. 센서 테스트 (3센서 + 1모니터)")
    print("4. 모든 테스트 실행")
    
    choice = input("선택하세요 (1-4): ")
    
    if choice == "1":
        test_basic()
    elif choice == "2":
        test_chat()
    elif choice == "3":
        test_sensors()
    elif choice == "4":
        print("\n" + "="*50)
        test_basic()
        print("\n" + "="*50)
        test_chat()
        print("\n" + "="*50)
        test_sensors()
    else:
        print("잘못된 선택입니다.")

if __name__ == "__main__":
    main() 