import asyncio
import logging
import json
from datetime import datetime
from typing import Dict, Set, Optional
import socket
import threading
import time

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('mqtt_server.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class MQTTServer:
    def __init__(self, host='0.0.0.0', port=1883):
        self.host = host
        self.port = port
        self.clients: Dict[str, 'MQTTClient'] = {}
        self.subscriptions: Dict[str, Set[str]] = {}
        self.server_socket = None
        self.running = False
        
    def start(self):
        """MQTT 서버 시작"""
        try:
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server_socket.bind((self.host, self.port))
            self.server_socket.listen(5)
            self.running = True
            
            logger.info(f"MQTT 서버가 {self.host}:{self.port}에서 시작되었습니다.")
            
            while self.running:
                try:
                    client_socket, address = self.server_socket.accept()
                    logger.info(f"새로운 클라이언트 연결: {address}")
                    
                    # 새 클라이언트 스레드 시작
                    client_thread = threading.Thread(
                        target=self.handle_client,
                        args=(client_socket, address)
                    )
                    client_thread.daemon = True
                    client_thread.start()
                    
                except Exception as e:
                    if self.running:
                        logger.error(f"클라이언트 연결 처리 중 오류: {e}")
                        
        except Exception as e:
            logger.error(f"서버 시작 실패: {e}")
        finally:
            self.stop()
    
    def stop(self):
        """MQTT 서버 중지"""
        self.running = False
        if self.server_socket:
            self.server_socket.close()
        
        # 모든 클라이언트 연결 종료
        for client_id, client in list(self.clients.items()):
            client.disconnect()
        
        logger.info("MQTT 서버가 중지되었습니다.")
    
    def handle_client(self, client_socket, address):
        """클라이언트 연결 처리"""
        client = MQTTClient(client_socket, address, self)
        try:
            client.handle_connection()
        except Exception as e:
            logger.error(f"클라이언트 {address} 처리 중 오류: {e}")
        finally:
            client.disconnect()
    
    def add_client(self, client_id: str, client: 'MQTTClient'):
        """클라이언트 추가"""
        self.clients[client_id] = client
        logger.info(f"클라이언트 추가됨: {client_id}")
    
    def remove_client(self, client_id: str):
        """클라이언트 제거"""
        if client_id in self.clients:
            del self.clients[client_id]
            logger.info(f"클라이언트 제거됨: {client_id}")
    
    def subscribe(self, client_id: str, topic: str):
        """클라이언트 구독"""
        if topic not in self.subscriptions:
            self.subscriptions[topic] = set()
        self.subscriptions[topic].add(client_id)
        logger.info(f"클라이언트 {client_id}가 토픽 {topic}에 구독했습니다.")
    
    def unsubscribe(self, client_id: str, topic: str):
        """클라이언트 구독 해제"""
        if topic in self.subscriptions and client_id in self.subscriptions[topic]:
            self.subscriptions[topic].remove(client_id)
            if not self.subscriptions[topic]:
                del self.subscriptions[topic]
            logger.info(f"클라이언트 {client_id}가 토픽 {topic}에서 구독 해제했습니다.")
    
    def publish(self, topic: str, message: str, qos: int = 0):
        """메시지 발행"""
        if topic in self.subscriptions:
            for client_id in self.subscriptions[topic]:
                if client_id in self.clients:
                    self.clients[client_id].send_message(topic, message, qos)
            logger.info(f"토픽 {topic}에 메시지 발행: {message[:50]}...")
        else:
            logger.info(f"토픽 {topic}에 구독자가 없습니다.")

class MQTTClient:
    def __init__(self, socket, address, server):
        self.socket = socket
        self.address = address
        self.server = server
        self.client_id = None
        self.connected = False
        self.subscriptions = set()
        
    def handle_connection(self):
        """클라이언트 연결 처리"""
        try:
            # CONNECT 패킷 처리
            if not self.handle_connect():
                return
            
            self.connected = True
            logger.info(f"클라이언트 {self.client_id} 연결됨")
            
            while self.connected:
                try:
                    # 패킷 헤더 읽기
                    packet_type = self.read_packet_header()
                    if packet_type is None:
                        break
                    
                    # 패킷 타입에 따른 처리
                    if packet_type == 1:  # CONNECT
                        self.handle_connect()
                    elif packet_type == 3:  # PUBLISH
                        self.handle_publish()
                    elif packet_type == 8:  # SUBSCRIBE
                        self.handle_subscribe()
                    elif packet_type == 10:  # UNSUBSCRIBE
                        self.handle_unsubscribe()
                    elif packet_type == 12:  # PINGREQ
                        self.handle_pingreq()
                    elif packet_type == 14:  # DISCONNECT
                        self.handle_disconnect()
                        break
                    else:
                        logger.warning(f"지원하지 않는 패킷 타입: {packet_type}")
                        
                except Exception as e:
                    logger.error(f"패킷 처리 중 오류: {e}")
                    break
                    
        except Exception as e:
            logger.error(f"클라이언트 연결 처리 중 오류: {e}")
        finally:
            self.disconnect()
    
    def read_packet_header(self):
        """패킷 헤더 읽기"""
        try:
            # 첫 번째 바이트 읽기
            first_byte = self.socket.recv(1)
            if not first_byte:
                return None
            
            packet_type = (first_byte[0] >> 4) & 0x0F
            
            # 나머지 길이 읽기
            remaining_length = self.read_remaining_length()
            if remaining_length is None:
                return None
            
            return packet_type
            
        except Exception as e:
            logger.error(f"패킷 헤더 읽기 오류: {e}")
            return None
    
    def read_remaining_length(self):
        """나머지 길이 읽기"""
        try:
            multiplier = 1
            value = 0
            
            while True:
                byte = self.socket.recv(1)
                if not byte:
                    return None
                
                byte_val = byte[0]
                value += (byte_val & 127) * multiplier
                multiplier *= 128
                
                if (byte_val & 128) == 0:
                    break
            
            return value
            
        except Exception as e:
            logger.error(f"나머지 길이 읽기 오류: {e}")
            return None
    
    def handle_connect(self):
        """CONNECT 패킷 처리"""
        try:
            # 프로토콜 이름 길이 읽기
            protocol_name_length_bytes = self.socket.recv(2)
            if len(protocol_name_length_bytes) < 2:
                logger.error("프로토콜 이름 길이를 읽을 수 없습니다.")
                return False
            protocol_name_length = int.from_bytes(protocol_name_length_bytes, 'big')
            
            # 프로토콜 이름 읽기
            protocol_name_bytes = self.socket.recv(protocol_name_length)
            if len(protocol_name_bytes) < protocol_name_length:
                logger.error("프로토콜 이름을 읽을 수 없습니다.")
                return False
            protocol_name = protocol_name_bytes.decode('utf-8')
            
            # 프로토콜 레벨 읽기
            protocol_level_bytes = self.socket.recv(1)
            if len(protocol_level_bytes) < 1:
                logger.error("프로토콜 레벨을 읽을 수 없습니다.")
                return False
            protocol_level = protocol_level_bytes[0]
            
            # 연결 플래그 읽기
            connect_flags_bytes = self.socket.recv(1)
            if len(connect_flags_bytes) < 1:
                logger.error("연결 플래그를 읽을 수 없습니다.")
                return False
            connect_flags = connect_flags_bytes[0]
            
            # Keep Alive 읽기
            keep_alive_bytes = self.socket.recv(2)
            if len(keep_alive_bytes) < 2:
                logger.error("Keep Alive를 읽을 수 없습니다.")
                return False
            keep_alive = int.from_bytes(keep_alive_bytes, 'big')
            
            # 클라이언트 ID 길이 읽기
            client_id_length_bytes = self.socket.recv(2)
            if len(client_id_length_bytes) < 2:
                logger.error("클라이언트 ID 길이를 읽을 수 없습니다.")
                return False
            client_id_length = int.from_bytes(client_id_length_bytes, 'big')
            
            # 클라이언트 ID 읽기
            client_id_bytes = self.socket.recv(client_id_length)
            if len(client_id_bytes) < client_id_length:
                logger.error("클라이언트 ID를 읽을 수 없습니다.")
                return False
            self.client_id = client_id_bytes.decode('utf-8')
            
            logger.info(f"CONNECT: 프로토콜={protocol_name}, 레벨={protocol_level}, 클라이언트ID={self.client_id}")
            
            # 서버에 클라이언트 추가
            self.server.add_client(self.client_id, self)
            
            # CONNACK 응답 전송
            self.send_connack()
            
            return True
            
        except Exception as e:
            logger.error(f"CONNECT 패킷 처리 오류: {e}")
            return False
    
    def handle_publish(self):
        """PUBLISH 패킷 처리"""
        try:
            # 토픽 이름 길이 읽기
            topic_length = int.from_bytes(self.socket.recv(2), 'big')
            topic = self.socket.recv(topic_length).decode('utf-8')
            
            # 메시지 ID 읽기 (QoS > 0인 경우)
            message_id = None
            if hasattr(self, 'qos') and self.qos > 0:
                message_id = int.from_bytes(self.socket.recv(2), 'big')
            
            # 페이로드 읽기
            payload = self.socket.recv(1024).decode('utf-8')
            
            logger.info(f"PUBLISH: 토픽={topic}, 메시지={payload}")
            
            # 구독자들에게 메시지 전달
            self.server.publish(topic, payload)
            
        except Exception as e:
            logger.error(f"PUBLISH 패킷 처리 오류: {e}")
    
    def handle_subscribe(self):
        """SUBSCRIBE 패킷 처리"""
        try:
            # 메시지 ID 읽기
            message_id = int.from_bytes(self.socket.recv(2), 'big')
            
            # 토픽 필터 길이 읽기
            topic_filter_length = int.from_bytes(self.socket.recv(2), 'big')
            topic_filter = self.socket.recv(topic_filter_length).decode('utf-8')
            
            # QoS 읽기
            qos = self.socket.recv(1)[0]
            
            # 구독 처리
            self.server.subscribe(self.client_id, topic_filter)
            self.subscriptions.add(topic_filter)
            
            # SUBACK 응답 전송
            self.send_suback(message_id, qos)
            
            logger.info(f"SUBSCRIBE: 토픽={topic_filter}, QoS={qos}")
            
        except Exception as e:
            logger.error(f"SUBSCRIBE 패킷 처리 오류: {e}")
    
    def handle_unsubscribe(self):
        """UNSUBSCRIBE 패킷 처리"""
        try:
            # 메시지 ID 읽기
            message_id = int.from_bytes(self.socket.recv(2), 'big')
            
            # 토픽 필터 길이 읽기
            topic_filter_length = int.from_bytes(self.socket.recv(2), 'big')
            topic_filter = self.socket.recv(topic_filter_length).decode('utf-8')
            
            # 구독 해제 처리
            self.server.unsubscribe(self.client_id, topic_filter)
            self.subscriptions.discard(topic_filter)
            
            # UNSUBACK 응답 전송
            self.send_unsuback(message_id)
            
            logger.info(f"UNSUBSCRIBE: 토픽={topic_filter}")
            
        except Exception as e:
            logger.error(f"UNSUBSCRIBE 패킷 처리 오류: {e}")
    
    def handle_pingreq(self):
        """PINGREQ 패킷 처리"""
        try:
            # PINGRESP 응답 전송
            self.send_pingresp()
            logger.debug(f"PINGREQ 처리됨: {self.client_id}")
            
        except Exception as e:
            logger.error(f"PINGREQ 패킷 처리 오류: {e}")
    
    def handle_disconnect(self):
        """DISCONNECT 패킷 처리"""
        logger.info(f"클라이언트 {self.client_id} 연결 해제 요청")
        self.disconnect()
    
    def send_connack(self):
        """CONNACK 응답 전송"""
        try:
            # 고정 헤더
            packet_type = 2  # CONNACK
            remaining_length = 2
            
            # 가변 헤더
            connect_acknowledge_flags = 0
            connect_return_code = 0  # 연결 수락
            
            packet = bytes([(packet_type << 4) | remaining_length])
            packet += bytes([connect_acknowledge_flags, connect_return_code])
            
            self.socket.send(packet)
            
        except Exception as e:
            logger.error(f"CONNACK 전송 오류: {e}")
    
    def send_suback(self, message_id: int, qos: int):
        """SUBACK 응답 전송"""
        try:
            # 고정 헤더
            packet_type = 9  # SUBACK
            remaining_length = 3
            
            # 가변 헤더
            packet = bytes([(packet_type << 4) | remaining_length])
            packet += message_id.to_bytes(2, 'big')
            packet += bytes([qos])
            
            self.socket.send(packet)
            
        except Exception as e:
            logger.error(f"SUBACK 전송 오류: {e}")
    
    def send_unsuback(self, message_id: int):
        """UNSUBACK 응답 전송"""
        try:
            # 고정 헤더
            packet_type = 11  # UNSUBACK
            remaining_length = 2
            
            # 가변 헤더
            packet = bytes([(packet_type << 4) | remaining_length])
            packet += message_id.to_bytes(2, 'big')
            
            self.socket.send(packet)
            
        except Exception as e:
            logger.error(f"UNSUBACK 전송 오류: {e}")
    
    def send_pingresp(self):
        """PINGRESP 응답 전송"""
        try:
            # 고정 헤더
            packet_type = 13  # PINGRESP
            remaining_length = 0
            
            packet = bytes([(packet_type << 4) | remaining_length])
            
            self.socket.send(packet)
            
        except Exception as e:
            logger.error(f"PINGRESP 전송 오류: {e}")
    
    def send_message(self, topic: str, message: str, qos: int = 0):
        """메시지 전송"""
        try:
            # 고정 헤더
            packet_type = 3  # PUBLISH
            dup = 0
            qos_bits = qos
            retain = 0
            
            first_byte = (packet_type << 4) | (dup << 3) | (qos_bits << 1) | retain
            
            # 가변 헤더
            topic_bytes = topic.encode('utf-8')
            topic_length = len(topic_bytes)
            
            # 페이로드
            payload_bytes = message.encode('utf-8')
            
            # 나머지 길이 계산
            remaining_length = 2 + topic_length + len(payload_bytes)
            if qos > 0:
                remaining_length += 2  # 메시지 ID
            
            # 패킷 구성
            packet = bytes([first_byte])
            packet += self.encode_remaining_length(remaining_length)
            packet += topic_length.to_bytes(2, 'big')
            packet += topic_bytes
            packet += payload_bytes
            
            self.socket.send(packet)
            
        except Exception as e:
            logger.error(f"메시지 전송 오류: {e}")
    
    def encode_remaining_length(self, length: int):
        """나머지 길이 인코딩"""
        encoded = bytearray()
        
        while length > 0:
            byte = length % 128
            length //= 128
            
            if length > 0:
                byte |= 128
            
            encoded.append(byte)
        
        return bytes(encoded)
    
    def disconnect(self):
        """클라이언트 연결 해제"""
        if self.connected:
            self.connected = False
            if self.client_id:
                self.server.remove_client(self.client_id)
            if self.socket:
                self.socket.close()
            logger.info(f"클라이언트 {self.client_id} 연결 해제됨")

def main():
    """메인 함수"""
    print("MQTT 서버를 시작합니다...")
    print("종료하려면 Ctrl+C를 누르세요.")
    
    server = MQTTServer()
    
    try:
        server.start()
    except KeyboardInterrupt:
        print("\n서버를 종료합니다...")
        server.stop()

if __name__ == "__main__":
    main()
