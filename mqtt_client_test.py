import paho.mqtt.client as mqtt
import json
import time
import threading

class MQTTClientTest:
    def __init__(self, client_id, broker_host='localhost', broker_port=1885):
        self.client_id = client_id
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.client = mqtt.Client(client_id=client_id)
        
        # 연결 상태 추적
        self.connected = False
        self.connection_event = threading.Event()
        
        # 콜백 함수 설정
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.on_message = self.on_message
        self.client.on_subscribe = self.on_subscribe
        self.client.on_publish = self.on_publish
        
    def on_connect(self, client, userdata, flags, rc):
        """연결 콜백"""
        if rc == 0:
            print(f"클라이언트 {self.client_id}가 서버에 연결되었습니다.")
            self.connected = True
            self.connection_event.set()
        else:
            print(f"연결 실패. 코드: {rc}")
            self.connected = False
            self.connection_event.set()
    
    def on_disconnect(self, client, userdata, rc):
        """연결 해제 콜백"""
        print(f"클라이언트 {self.client_id}가 연결 해제되었습니다.")
        self.connected = False
    
    def on_message(self, client, userdata, msg):
        """메시지 수신 콜백"""
        print(f"메시지 수신 - 토픽: {msg.topic}, 페이로드: {msg.payload.decode()}")
    
    def on_subscribe(self, client, userdata, mid, granted_qos):
        """구독 콜백"""
        print(f"구독 완료. QoS: {granted_qos}")
    
    def on_publish(self, client, userdata, mid):
        """발행 콜백"""
        print(f"메시지 발행 완료. 메시지 ID: {mid}")
    
    def connect(self):
        """서버에 연결"""
        try:
            print(f"클라이언트 {self.client_id} 연결 시도 중...")
            self.client.connect(self.broker_host, self.broker_port, 60)
            self.client.loop_start()
            
            # 연결 완료 대기 (최대 15초)
            if self.connection_event.wait(timeout=15):
                if self.connected:
                    print(f"클라이언트 {self.client_id} 연결 성공!")
                    return True
                else:
                    print(f"클라이언트 {self.client_id} 연결 실패")
                    return False
            else:
                print("연결 시간 초과")
                return False
        except Exception as e:
            print(f"연결 실패: {e}")
            return False
    
    def disconnect(self):
        """서버에서 연결 해제"""
        self.client.loop_stop()
        self.client.disconnect()
    
    def subscribe(self, topic, qos=0):
        """토픽 구독"""
        result = self.client.subscribe(topic, qos)
        print(f"토픽 {topic} 구독 요청")
        return result
    
    def unsubscribe(self, topic):
        """토픽 구독 해제"""
        result = self.client.unsubscribe(topic)
        print(f"토픽 {topic} 구독 해제 요청")
        return result
    
    def publish(self, topic, message, qos=0):
        """메시지 발행"""
        result = self.client.publish(topic, message, qos)
        print(f"토픽 {topic}에 메시지 발행: {message}")
        return result

def publisher_test():
    """발행자 테스트"""
    publisher = MQTTClientTest("publisher_test")
    
    if publisher.connect():
        print("발행자 테스트 시작...")
        
        # 메시지 발행
        topics = ["sensor/temperature", "sensor/humidity", "device/status"]
        messages = [
            '{"temperature": 25.5, "unit": "celsius"}',
            '{"humidity": 60.2, "unit": "percent"}',
            '{"status": "online", "timestamp": "' + str(time.time()) + '"}'
        ]
        
        for i in range(10):
            topic = topics[i % len(topics)]
            message = messages[i % len(messages)]
            publisher.publish(topic, message)
            time.sleep(2)
        
        publisher.disconnect()

def subscriber_test():
    """구독자 테스트"""
    subscriber = MQTTClientTest("subscriber_test")
    
    print("구독자 테스트 시작...")
    
    if subscriber.connect():
        print("구독자 연결 성공!")
        print("연결 상태:", subscriber.connected)
        
        # 토픽 구독
        topics = ["sensor/#", "device/#"]
        for topic in topics:
            subscriber.subscribe(topic)
        
        print("구독 완료. 30초간 메시지 수신 대기 중...")
        print("메시지를 받으려면 다른 터미널에서 발행자 테스트를 실행하세요.")
        
        # 30초간 메시지 수신 대기
        time.sleep(30)
        
        subscriber.disconnect()
        print("구독자 테스트 완료")
    else:
        print("구독자 연결에 실패했습니다.")
        print("MQTT 서버가 실행 중인지 확인하세요.")

def main():
    """메인 함수"""
    print("MQTT 클라이언트 테스트")
    print("1. 발행자 테스트")
    print("2. 구독자 테스트")
    print("3. 동시 테스트")
    
    choice = input("선택하세요 (1-3): ")
    
    if choice == "1":
        publisher_test()
    elif choice == "2":
        subscriber_test()
    elif choice == "3":
        # 발행자와 구독자를 동시에 실행
        subscriber_thread = threading.Thread(target=subscriber_test)
        subscriber_thread.daemon = True
        subscriber_thread.start()
        
        time.sleep(2)  # 구독자가 준비될 때까지 대기
        publisher_test()
    else:
        print("잘못된 선택입니다.")

if __name__ == "__main__":
    main() 