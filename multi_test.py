from multiprocessing import Process
import multiprocessing
import time 
import sqlite3
from datetime import datetime
from multiprocessing import Process, Queue
import string
import mysql.connector



def pro1(batch_data):
    conn = mysql.connector.connect(
        host='127.0.0.1',
        user='root',
        password='93150lbm!!',
        database='hello'
    )
    cursor = conn.cursor()
    for code_dict in batch_data:
        # print(code_dict)
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
        query_to_insert = ['거래시간','거래량']
        values_to_insert = [trans_time,volume]
        sql_query = f"INSERT INTO `{code}` (거래시간, 거래량) VALUES (%s, %s)"   
        cursor.execute(sql_query, values_to_insert)
    conn.commit()
    conn.close()
    
def worker_process(queue):


    while True:
        batch_data  = queue.get()  # Get data from the queue
        if batch_data  is None:
            break  # End loop if None is received
        print(batch_data)
        pro1(batch_data)


def continuous_data_generator(queue):
    batch_size = 1000
    code_list = list(string.ascii_lowercase)
    batch_data = []
    cnt = 0 
    for i in range(200):
        for code in code_list:
            current_time = datetime.now().strftime("%Y%m%d %H:%M:%S.%f")
            code_dict = {code: {'거래시간': current_time, '거래량': f'{str(i)}'}}
            batch_data.append(code_dict)
            
            cnt += 1
            if cnt % batch_size == 0 : 
                queue.put(batch_data)
                batch_data = []
    
    queue.put(batch_data)




if __name__ == '__main__':
    queue = Queue()
    num_workers = 8  # Adjust as needed

    # Start worker processes
    workers = [Process(target=worker_process, args=(queue,)) for _ in range(num_workers)]
    for w in workers:
        w.start()

    try:
        continuous_data_generator(queue)  # Continuously generate and queue data
    except KeyboardInterrupt:
        pass  # Handle Ctrl+C or other interruption

    # Send None to signal workers to terminate
    for _ in range(num_workers):
        queue.put(None)

    # Wait for all worker processes to finish
    for w in workers:
        w.join()


