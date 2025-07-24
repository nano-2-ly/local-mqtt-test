import socket
import threading
import time
import json

class SimpleMQTTBroker:
    def __init__(self, host='0.0.0.0', port=1883):
        self.host = host
        self.port = port
        self.clients = {}
        self.subscriptions = {}
        self.running = False
        
    def start(self):
        """MQTT 브로커 시작"""
        try:
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server_socket.bind((self.host, self.port))
            self.server_socket.listen(5)
            self.running = True
            
            print(f"MQTT 브로커가 {self.host}:{self.port}에서 시작되었습니다.")
            
            while self.running:
                try:
                    client_socket, address = self.server_socket.accept()
                    print(f"새로운 클라이언트 연결: {address}")
                    
                    # 새 클라이언트 스레드 시작
                    client_thread = threading.Thread(
                        target=self.handle_client,
                        args=(client_socket, address)
                    )
                    client_thread.daemon = True
                    client_thread.start()
                    
                except Exception as e:
                    if self.running:
                        print(f"클라이언트 연결 처리 중 오류: {e}")
                        
        except Exception as e:
            print(f"서버 시작 실패: {e}")
        finally:
            self.stop()
    
    def stop(self):
        """MQTT 브로커 중지"""
        self.running = False
        if hasattr(self, 'server_socket'):
            self.server_socket.close()
        print("MQTT 브로커가 중지되었습니다.")
    
    def handle_client(self, client_socket, address):
        """클라이언트 연결 처리"""
        try:
            # 간단한 연결 처리
            client_socket.send(b"CONNACK")
            
            while self.running:
                try:
                    data = client_socket.recv(1024)
                    if not data:
                        break
                    
                    # 간단한 메시지 처리
                    message = data.decode('utf-8', errors='ignore')
                    print(f"클라이언트 {address}로부터 메시지: {message}")
                    
                    # 모든 구독자에게 메시지 전송
                    self.broadcast_message(message)
                    
                except Exception as e:
                    print(f"클라이언트 {address} 메시지 처리 중 오류: {e}")
                    break
                    
        except Exception as e:
            print(f"클라이언트 {address} 처리 중 오류: {e}")
        finally:
            client_socket.close()
            print(f"클라이언트 {address} 연결 종료")
    
    def broadcast_message(self, message):
        """모든 구독자에게 메시지 전송"""
        # 간단한 구현: 모든 연결된 클라이언트에게 메시지 전송
        pass

def main():
    broker = SimpleMQTTBroker()
    try:
        broker.start()
    except KeyboardInterrupt:
        print("\n브로커 종료 중...")
        broker.stop()

if __name__ == "__main__":
    main() 