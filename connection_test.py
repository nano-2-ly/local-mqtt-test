import socket
import time

def test_mqtt_connection():
    """MQTT 서버 연결 테스트"""
    host = 'localhost'
    port = 1883
    
    print(f"MQTT 서버 {host}:{port} 연결 테스트 중...")
    
    try:
        # 소켓 생성
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        
        # 연결 시도
        result = sock.connect_ex((host, port))
        
        if result == 0:
            print("✓ MQTT 서버에 연결 성공!")
            
            # 간단한 MQTT CONNECT 패킷 전송
            connect_packet = b'\x10\x0c\x00\x04MQTT\x04\x02\x00\x3c\x00\x00'
            sock.send(connect_packet)
            
            # 응답 대기
            try:
                response = sock.recv(1024)
                if response:
                    print(f"✓ 서버 응답 받음: {response}")
                else:
                    print("⚠ 서버로부터 응답 없음")
            except socket.timeout:
                print("⚠ 서버 응답 시간 초과")
            
            sock.close()
            return True
        else:
            print(f"✗ MQTT 서버 연결 실패 (에러 코드: {result})")
            return False
            
    except Exception as e:
        print(f"✗ 연결 테스트 중 오류: {e}")
        return False

def test_port_availability():
    """포트 1883 사용 가능 여부 테스트"""
    host = 'localhost'
    port = 1883
    
    print(f"포트 {port} 사용 가능 여부 확인 중...")
    
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        result = sock.connect_ex((host, port))
        sock.close()
        
        if result == 0:
            print(f"✓ 포트 {port}가 열려있습니다.")
            return True
        else:
            print(f"✗ 포트 {port}가 닫혀있습니다.")
            return False
    except Exception as e:
        print(f"✗ 포트 테스트 중 오류: {e}")
        return False

if __name__ == "__main__":
    print("MQTT 연결 테스트")
    print("=" * 30)
    
    # 포트 사용 가능 여부 확인
    if test_port_availability():
        # MQTT 연결 테스트
        test_mqtt_connection()
    else:
        print("\nMQTT 서버가 실행되지 않고 있습니다.")
        print("다음 중 하나를 시도해보세요:")
        print("1. python mqtt_server.py 실행")
        print("2. 표준 MQTT 브로커 설치 (예: Mosquitto)")
        print("3. 다른 포트 사용") 