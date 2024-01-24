# data_generator.py
import socket
import time
from datetime import datetime
import string
import json

def generate_data(batch_size=1000):
    code_list = list(string.ascii_lowercase)
    batch_data = []
    for i in range(200):
        for code in code_list:
            current_time = datetime.now().strftime("%Y%m%d %H:%M:%S.%f")
            code_dict = {code: {'거래시간': current_time, '거래량': str(i)}}
            batch_data.append(code_dict)
            if len(batch_data) >= batch_size:
                yield batch_data
                batch_data = []
    if batch_data:  # Don't forget to send the last batch
        yield batch_data

def send_data_to_server(data):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect(('localhost', 12345))
        s.sendall(json.dumps(data).encode('utf-8'))

if __name__ == '__main__':
    for data_batch in generate_data():
        send_data_to_server(data_batch)

