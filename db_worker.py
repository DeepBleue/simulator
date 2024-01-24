# db_operation.py
import socket
import json
import mysql.connector
from multiprocessing import Process, Queue


def pro1(batch_data):
    conn = mysql.connector.connect(
        host='127.0.0.1',
        user='root',
        password='93150lbm!!',
        database='hello'
    )
    cursor = conn.cursor()
    for code_dict in batch_data:
        code = list(code_dict.keys())[0]
        table_sql = f"""
                CREATE TABLE IF NOT EXISTS `{code}` (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    거래시간 VARCHAR(255),
                    거래량 VARCHAR(255)
                );
            """
        cursor.execute(table_sql)

        trans_time = code_dict[code]['거래시간']
        volume = code_dict[code]['거래량']
        sql_query = f"INSERT INTO `{code}` (거래시간, 거래량) VALUES (%s, %s)"   
        cursor.execute(sql_query, (trans_time, volume))

    conn.commit()
    conn.close()

def worker_process(queue):
    while True:
        batch_data = queue.get()
        if batch_data is None:
            break
        pro1(batch_data)
        
def server_process():
    queue = Queue()
    num_workers = 8
    workers = [Process(target=worker_process, args=(queue,)) for _ in range(num_workers)]
    for w in workers:
        w.start()

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('localhost', 12345))
        s.listen()
        while True:
            conn, addr = s.accept()
            with conn:
                data_parts = []
                while True:
                    part = conn.recv(4096)  # Increased buffer size
                    if not part:
                        break
                    data_parts.append(part)
                data = b''.join(data_parts)
                if data:
                    batch_data = json.loads(data.decode('utf-8'))
                    queue.put(batch_data)

    # for _ in range(num_workers):
    #     queue.put(None)
    # for w in workers:
    #     w.join()

        
        
if __name__ == '__main__':
    server_process()