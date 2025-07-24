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
        logging.FileHandler('mqtt_server_1885.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class MQTTServer:
    def __init__(self, host='0.0.0.0', port=1885):
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
        logger.info(f"구독: {client_id} -> {topic}")
    
    def unsubscribe(self, client_id: str, topic: str):
        """클라이언트 구독 해제"""
        if topic in self.subscriptions and client_id in self.subscriptions[topic]:
            self.subscriptions[topic].remove(client_id)
            logger.info(f"구독 해제: {client_id} -> {topic}")
    
    def publish(self, topic: str, message: str, qos: int = 0):
        """메시지 발행"""
        if topic in self.subscriptions:
            for client_id in self.subscriptions[topic]:
                if client_id in self.clients:
                    self.clients[client_id].send_message(topic, message, qos)
            logger.info(f"메시지 발행: {topic} -> {message}")

class MQTTClient:
    def __init__(self, socket, address, server):
        self.socket = socket
        self.address = address
        self.server = server
        self.client_id = None
        self.subscriptions = set()
        self.connected = False
        
    def handle_connection(self):
        """클라이언트 연결 처리"""
        try:
            while self.server.running:
                # 패킷 헤더 읽기
                packet_type, remaining_length = self.read_packet_header()
                
                if packet_type == 1:  # CONNECT
                    if self.handle_connect():
                        self.connected = True
                        logger.info(f"클라이언트 {self.client_id} 연결 성공")
                    else:
                        logger.error(f"클라이언트 {self.address} 연결 실패")
                        break
                        
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
                    
        except Exception as e:
            logger.error(f"클라이언트 {self.address} 처리 중 오류: {e}")
        finally:
            if self.client_id:
                self.server.remove_client(self.client_id)
    
    def read_packet_header(self):
        """패킷 헤더 읽기"""
        try:
            # 첫 번째 바이트 읽기
            first_byte = self.socket.recv(1)
            if not first_byte:
                raise Exception("연결이 끊어졌습니다")
            
            packet_type = (first_byte[0] >> 4) & 0x0F
            
            # 나머지 길이 읽기
            remaining_length = self.read_remaining_length()
            
            return packet_type, remaining_length
            
        except Exception as e:
            logger.error(f"패킷 헤더 읽기 오류: {e}")
            raise
    
    def read_remaining_length(self):
        """나머지 길이 읽기"""
        multiplier = 1
        value = 0
        
        while True:
            byte = self.socket.recv(1)
            if not byte:
                raise Exception("연결이 끊어졌습니다")
            
            byte_val = byte[0]
            value += (byte_val & 0x7F) * multiplier
            
            if (byte_val & 0x80) == 0:
                break
                
            multiplier *= 128
            
            if multiplier > 128 * 128 * 128:
                raise Exception("나머지 길이가 너무 큽니다")
        
        return value
    
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
            topic_length_bytes = self.socket.recv(2)
            if len(topic_length_bytes) < 2:
                logger.error("토픽 길이를 읽을 수 없습니다.")
                return
            topic_length = int.from_bytes(topic_length_bytes, 'big')
            
            # 토픽 이름 읽기
            topic_bytes = self.socket.recv(topic_length)
            if len(topic_bytes) < topic_length:
                logger.error("토픽을 읽을 수 없습니다.")
                return
            topic = topic_bytes.decode('utf-8')
            
            # 메시지 ID 읽기 (QoS > 0인 경우)
            message_id = None
            if hasattr(self, 'qos') and self.qos > 0:
                message_id_bytes = self.socket.recv(2)
                if len(message_id_bytes) >= 2:
                    message_id = int.from_bytes(message_id_bytes, 'big')
            
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
            message_id_bytes = self.socket.recv(2)
            if len(message_id_bytes) < 2:
                logger.error("메시지 ID를 읽을 수 없습니다.")
                return
            message_id = int.from_bytes(message_id_bytes, 'big')
            
            # 토픽 필터 길이 읽기
            topic_filter_length_bytes = self.socket.recv(2)
            if len(topic_filter_length_bytes) < 2:
                logger.error("토픽 필터 길이를 읽을 수 없습니다.")
                return
            topic_filter_length = int.from_bytes(topic_filter_length_bytes, 'big')
            
            # 토픽 필터 읽기
            topic_filter_bytes = self.socket.recv(topic_filter_length)
            if len(topic_filter_bytes) < topic_filter_length:
                logger.error("토픽 필터를 읽을 수 없습니다.")
                return
            topic_filter = topic_filter_bytes.decode('utf-8')
            
            # QoS 읽기
            qos_bytes = self.socket.recv(1)
            if len(qos_bytes) < 1:
                logger.error("QoS를 읽을 수 없습니다.")
                return
            qos = qos_bytes[0]
            
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
            message_id_bytes = self.socket.recv(2)
            if len(message_id_bytes) < 2:
                logger.error("메시지 ID를 읽을 수 없습니다.")
                return
            message_id = int.from_bytes(message_id_bytes, 'big')
            
            # 토픽 필터 길이 읽기
            topic_filter_length_bytes = self.socket.recv(2)
            if len(topic_filter_length_bytes) < 2:
                logger.error("토픽 필터 길이를 읽을 수 없습니다.")
                return
            topic_filter_length = int.from_bytes(topic_filter_length_bytes, 'big')
            
            # 토픽 필터 읽기
            topic_filter_bytes = self.socket.recv(topic_filter_length)
            if len(topic_filter_bytes) < topic_filter_length:
                logger.error("토픽 필터를 읽을 수 없습니다.")
                return
            topic_filter = topic_filter_bytes.decode('utf-8')
            
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
            logger.info("PINGREQ 처리 완료")
        except Exception as e:
            logger.error(f"PINGREQ 패킷 처리 오류: {e}")
    
    def handle_disconnect(self):
        """DISCONNECT 패킷 처리"""
        logger.info(f"클라이언트 {self.client_id} 연결 해제")
        self.connected = False
    
    def send_connack(self):
        """CONNACK 응답 전송"""
        try:
            # CONNACK 패킷 구성
            packet = bytearray()
            packet.append(0x20)  # CONNACK 패킷 타입
            packet.append(0x02)  # 나머지 길이
            packet.append(0x00)  # 연결 플래그 (0 = 연결 수락)
            packet.append(0x00)  # 반환 코드 (0 = 연결 수락)
            
            self.socket.send(packet)
            logger.info("CONNACK 전송 완료")
            
        except Exception as e:
            logger.error(f"CONNACK 전송 오류: {e}")
    
    def send_suback(self, message_id: int, qos: int):
        """SUBACK 응답 전송"""
        try:
            # SUBACK 패킷 구성
            packet = bytearray()
            packet.append(0x90)  # SUBACK 패킷 타입
            packet.append(0x03)  # 나머지 길이
            packet.extend(message_id.to_bytes(2, 'big'))  # 메시지 ID
            packet.append(qos)   # QoS
            
            self.socket.send(packet)
            logger.info("SUBACK 전송 완료")
            
        except Exception as e:
            logger.error(f"SUBACK 전송 오류: {e}")
    
    def send_unsuback(self, message_id: int):
        """UNSUBACK 응답 전송"""
        try:
            # UNSUBACK 패킷 구성
            packet = bytearray()
            packet.append(0xB0)  # UNSUBACK 패킷 타입
            packet.append(0x02)  # 나머지 길이
            packet.extend(message_id.to_bytes(2, 'big'))  # 메시지 ID
            
            self.socket.send(packet)
            logger.info("UNSUBACK 전송 완료")
            
        except Exception as e:
            logger.error(f"UNSUBACK 전송 오류: {e}")
    
    def send_pingresp(self):
        """PINGRESP 응답 전송"""
        try:
            # PINGRESP 패킷 구성
            packet = bytearray()
            packet.append(0xD0)  # PINGRESP 패킷 타입
            packet.append(0x00)  # 나머지 길이
            
            self.socket.send(packet)
            logger.info("PINGRESP 전송 완료")
            
        except Exception as e:
            logger.error(f"PINGRESP 전송 오류: {e}")
    
    def send_message(self, topic: str, message: str, qos: int = 0):
        """메시지 전송"""
        try:
            # PUBLISH 패킷 구성
            packet = bytearray()
            packet.append(0x30)  # PUBLISH 패킷 타입
            
            # 토픽 길이와 토픽 추가
            topic_bytes = topic.encode('utf-8')
            packet.extend(len(topic_bytes).to_bytes(2, 'big'))
            packet.extend(topic_bytes)
            
            # 메시지 ID 추가 (QoS > 0인 경우)
            if qos > 0:
                message_id = 1  # 간단한 구현
                packet.extend(message_id.to_bytes(2, 'big'))
            
            # 페이로드 추가
            message_bytes = message.encode('utf-8')
            packet.extend(message_bytes)
            
            # 나머지 길이 계산 및 추가
            remaining_length = len(packet) - 1
            packet[1] = remaining_length
            
            self.socket.send(packet)
            logger.info(f"메시지 전송: {topic} -> {message}")
            
        except Exception as e:
            logger.error(f"메시지 전송 오류: {e}")
    
    def encode_remaining_length(self, length: int):
        """나머지 길이 인코딩"""
        encoded = bytearray()
        
        while length > 0:
            byte = length % 128
            length = length // 128
            
            if length > 0:
                byte |= 0x80
                
            encoded.append(byte)
            
        return encoded
    
    def disconnect(self):
        """클라이언트 연결 종료"""
        try:
            if self.socket:
                self.socket.close()
        except Exception as e:
            logger.error(f"클라이언트 연결 종료 오류: {e}")

def main():
    """메인 함수"""
    print("MQTT 서버를 시작합니다...")
    print("종료하려면 Ctrl+C를 누르세요.")
    
    server = MQTTServer(port=1885)
    try:
        server.start()
    except KeyboardInterrupt:
        print("\n서버를 종료합니다...")
        server.stop()

if __name__ == "__main__":
    main() 