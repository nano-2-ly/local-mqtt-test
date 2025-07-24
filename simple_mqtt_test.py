import paho.mqtt.client as mqtt
import time
import threading

class SimpleMQTTTest:
    def __init__(self, client_id):
        self.client_id = client_id
        self.client = mqtt.Client(client_id=client_id)
        self.connected = False
        
        # 콜백 설정
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.on_message = self.on_message
        self.client.on_subscribe = self.on_subscribe
        
    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            print(f"클라이언트 {self.client_id} 연결 성공!")
            self.connected = True
        else:
            print(f"연결 실패. 코드: {rc}")
            self.connected = False
    
    def on_disconnect(self, client, userdata, rc):
        print(f"클라이언트 {self.client_id} 연결 해제")
        self.connected = False
    
    def on_message(self, client, userdata, msg):
        print(f"메시지 수신 - 토픽: {msg.topic}, 내용: {msg.payload.decode()}")
    
    def on_subscribe(self, client, userdata, mid, granted_qos):
        print(f"구독 완료. QoS: {granted_qos}")
    
    def connect(self):
        try:
            print(f"클라이언트 {self.client_id} 연결 시도...")
            self.client.connect('localhost', 1883, 60)
            self.client.loop_start()
            
            # 연결 완료 대기
            timeout = 10
            while not self.connected and timeout > 0:
                time.sleep(0.1)
                timeout -= 0.1
            
            return self.connected
        except Exception as e:
            print(f"연결 실패: {e}")
            return False
    
    def disconnect(self):
        self.client.loop_stop()
        self.client.disconnect()
    
    def subscribe(self, topic):
        if self.connected:
            self.client.subscribe(topic)
            print(f"토픽 {topic} 구독 요청")
        else:
            print("연결되지 않음")
    
    def publish(self, topic, message):
        if self.connected:
            self.client.publish(topic, message)
            print(f"토픽 {topic}에 메시지 발행: {message}")
        else:
            print("연결되지 않음")

def subscriber_test():
    """구독자 테스트"""
    subscriber = SimpleMQTTTest("subscriber_test")
    
    if subscriber.connect():
        print("구독자 테스트 시작...")
        
        # 토픽 구독
        topics = ["sensor/#", "device/#"]
        for topic in topics:
            subscriber.subscribe(topic)
        
        print("구독 완료. 30초간 메시지 수신 대기 중...")
        time.sleep(30)
        
        subscriber.disconnect()
    else:
        print("구독자 연결에 실패했습니다.")

def publisher_test():
    """발행자 테스트"""
    publisher = SimpleMQTTTest("publisher_test")
    
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
    else:
        print("발행자 연결에 실패했습니다.")

def main():
    print("간단한 MQTT 테스트")
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