import socket
import json
import threading
import time


def handle_client(conn, addr):
    # Handle client in a separate thread
    try:
        data = fetch_data_from_api()
        conn.sendall(json.dumps(data).encode())
    finally:
        conn.close()

def start_server():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('localhost', 12345))
        s.listen()
        while True:
            conn, addr = s.accept()
            client_thread = threading.Thread(target=handle_client, args=(conn, addr))
            client_thread.start()

def fetch_data_from_api():
    # Your code to fetch data from API
    data = {'example_key': 'example_value'}
    time.sleep(10)
    return data

start_server()
