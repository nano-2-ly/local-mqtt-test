import paho.mqtt.client as mqtt
import json
import time
import threading

class RemoteMQTTClient:
    def __init__(self, client_id, broker_host='192.168.0.76', broker_port=1883):
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
            print(f"클라이언트 {self.client_id}가 서버 {self.broker_host}:{self.broker_port}에 연결되었습니다.")
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
            print(f"서버 주소: {self.broker_host}:{self.broker_port}")
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
        if self.connected:
            result = self.client.subscribe(topic, qos)
            print(f"토픽 {topic} 구독 요청")
            return result
        else:
            print("연결되지 않음")
            return None
    
    def unsubscribe(self, topic):
        """토픽 구독 해제"""
        if self.connected:
            result = self.client.unsubscribe(topic)
            print(f"토픽 {topic} 구독 해제 요청")
            return result
        else:
            print("연결되지 않음")
            return None
    
    def publish(self, topic, message, qos=0):
        """메시지 발행"""
        if self.connected:
            result = self.client.publish(topic, message, qos)
            print(f"토픽 {topic}에 메시지 발행: {message}")
            return result
        else:
            print("연결되지 않음")
            return None

def remote_subscriber_test():
    """원격 구독자 테스트"""
    # 서버 IP 주소를 입력받거나 기본값 사용
    server_ip = input("서버 IP 주소를 입력하세요 (기본값: 192.168.0.76): ").strip()
    if not server_ip:
        server_ip = "192.168.0.76"
    
    subscriber = RemoteMQTTClient("remote_subscriber", server_ip, 1883)
    
    print("원격 구독자 테스트 시작...")
    
    if subscriber.connect():
        print("원격 구독자 연결 성공!")
        print("연결 상태:", subscriber.connected)
        
        # 토픽 구독
        topics = ["sensor/#", "device/#"]
        for topic in topics:
            subscriber.subscribe(topic)
        
        print("구독 완료. 30초간 메시지 수신 대기 중...")
        print("메시지를 받으려면 다른 기기에서 발행자 테스트를 실행하세요.")
        
        # 30초간 메시지 수신 대기
        time.sleep(30)
        
        subscriber.disconnect()
        print("원격 구독자 테스트 완료")
    else:
        print("원격 구독자 연결에 실패했습니다.")
        print("서버 IP 주소와 포트를 확인하세요.")

def remote_publisher_test():
    """원격 발행자 테스트"""
    # 서버 IP 주소를 입력받거나 기본값 사용
    server_ip = input("서버 IP 주소를 입력하세요 (기본값: 192.168.0.76): ").strip()
    if not server_ip:
        server_ip = "192.168.0.76"
    
    publisher = RemoteMQTTClient("remote_publisher", server_ip, 1883)
    
    print("원격 발행자 테스트 시작...")
    
    if publisher.connect():
        print("원격 발행자 연결 성공!")
        
        # 메시지 발행
        topics = ["sensor/temperature", "sensor/humidity", "device/status"]
        messages = [
            '{"temperature": 25.5, "unit": "celsius", "source": "remote"}',
            '{"humidity": 60.2, "unit": "percent", "source": "remote"}',
            '{"status": "online", "timestamp": "' + str(time.time()) + '", "source": "remote"}'
        ]
        
        for i in range(10):
            topic = topics[i % len(topics)]
            message = messages[i % len(messages)]
            publisher.publish(topic, message)
            time.sleep(2)
        
        publisher.disconnect()
        print("원격 발행자 테스트 완료")
    else:
        print("원격 발행자 연결에 실패했습니다.")
        print("서버 IP 주소와 포트를 확인하세요.")

def main():
    """메인 함수"""
    print("원격 MQTT 클라이언트 테스트")
    print("=" * 40)
    print("1. 원격 구독자 테스트")
    print("2. 원격 발행자 테스트")
    print("3. 동시 테스트")
    
    choice = input("선택하세요 (1-3): ")
    
    if choice == "1":
        remote_subscriber_test()
    elif choice == "2":
        remote_publisher_test()
    elif choice == "3":
        # 발행자와 구독자를 동시에 실행
        subscriber_thread = threading.Thread(target=remote_subscriber_test)
        subscriber_thread.daemon = True
        subscriber_thread.start()
        
        time.sleep(2)  # 구독자가 준비될 때까지 대기
        remote_publisher_test()
    else:
        print("잘못된 선택입니다.")

if __name__ == "__main__":
    main() 