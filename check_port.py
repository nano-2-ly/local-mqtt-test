import socket

def check_port(host='localhost', port=1883):
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        result = sock.connect_ex((host, port))
        sock.close()
        
        if result == 0:
            print(f"포트 {port}가 열려있습니다.")
            return True
        else:
            print(f"포트 {port}가 닫혀있습니다.")
            return False
    except Exception as e:
        print(f"포트 확인 중 오류: {e}")
        return False

if __name__ == "__main__":
    check_port() 