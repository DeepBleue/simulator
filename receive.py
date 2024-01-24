import socket
import json
import sqlite3

def receive_data():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect(('localhost', 12345))
        data = s.recv(1024)
    return json.loads(data)

def save_data_to_db(data):
    conn = sqlite3.connect('your_database.db')
    cursor = conn.cursor()
    # Assuming 'data_table' with columns 'key' and 'value'
    cursor.execute("INSERT INTO data_table (key, value) VALUES (?, ?)", (data['example_key'], data['example_value']))
    conn.commit()
    conn.close()

while True:
    data = receive_data()
    # save_data_to_db(data)
    print(data)
