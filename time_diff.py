import mysql.connector
from datetime import datetime
import mysql.connector
import pandas as pd
import warnings
from tqdm.notebook import tqdm
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
import time
import numpy as np 
import matplotlib.gridspec as gridspec
from matplotlib.widgets import Button
import matplotlib.dates as mdates

plt.rcParams['font.family'] = 'Malgun Gothic'
plt.rcParams['axes.unicode_minus'] = False


def get_table_names(host, user, password, database):
    try:
        # Establish a database connection
        conn = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )

        # Create a cursor object
        cursor = conn.cursor()

        # Execute a query to retrieve table names
        query = f"SELECT table_name FROM INFORMATION_SCHEMA.TABLES WHERE table_schema = '{database}'"
        cursor.execute(query)

        # Fetch all the rows
        tables = cursor.fetchall()

        # Print table names
        # for (table_name,) in tables:
        #     print(table_name)

    except mysql.connector.Error as error:
        print(f"Error: {error}")
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()
            return tables
        
        
def table_to_dataframe(host, user, password, database, table_name):
    try:
        # Establish a database connection
        conn = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )

        # Use backticks around the table name in the query
        query = f"SELECT * FROM `{table_name}`"

        # Use pandas to read the query into a DataFrame
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            df = pd.read_sql(query, conn)

        return df

    except mysql.connector.Error as error:
        print(f"Error: {error}")
        return None
    finally:
        if conn.is_connected():
            conn.close()
            

def show_amount(df):

    sns.histplot(df['거래대금'], kde=True)

    plt.title('Distribution of 거래대금')
    plt.xlabel('거래대금')
    plt.ylabel('Density')

    plt.show()
    

def num_to_won(num):
    kor_won_uk = num / 1e8
    return kor_won_uk




# databaseA = "A" + datetime.now().strftime("%Y%m%d") 
databaseA = 'a20240112'
password = '93150lbm!!'

tables = get_table_names('127.0.0.1', 'root', password, databaseA)

tables = [code for (code,) in tables]
print(tables)

formatter = mdates.DateFormatter('%H:%M:%S')
code = tables[0]

code = '452190'


df = table_to_dataframe('127.0.0.1', 'root', password, databaseA, code)
df['체결시간'] = pd.to_datetime(df['체결시간'])
df['현재가'] = pd.to_numeric(df['현재가'].str.replace('+','').str.replace('-',''), errors='coerce')
df['거래량'] = pd.to_numeric(df['거래량'], errors='coerce')
df['거래대금'] = df['현재가'] * df['거래량']
df = df.sort_values(by='체결시간', ignore_index=True)
df['time_diff'] = df['체결시간'].diff()
df['time_diff_seconds'] = df['time_diff'].dt.total_seconds()

df = df.dropna()


price_max = df['현재가'].max()
price_min = df['현재가'].min()



df['exp_minus'] = np.exp(-df['time_diff_seconds'])
df['signal'] = df['exp_minus'] * df['거래대금']
df['positive_signal'] = df['signal'].apply(lambda x: x if x > 0 else 0)
df['negative_signal'] = df['signal'].apply(lambda x: abs(x) if x < 0 else 0)
max_signal = abs(df['signal']).max()


df['positive_volume'] = df['거래량'].apply(lambda x: x if x > 0 else 0)
df['negative_volume'] = df['거래량'].apply(lambda x: abs(x) if x < 0 else 0)
step_size = 100
window_size = 500
max_y_value = 5
max_volume = abs(df['거래량']).max()

fig = plt.figure(figsize=(10, 8))
gs = gridspec.GridSpec(3, 1, height_ratios=[1, 1,0.1], hspace=0.5)

ax0 = plt.subplot(gs[0])
ax0.set_title("'현재가' Over Time")

ax1 = plt.subplot(gs[1])
ax2 = ax1.twinx()  # Create secondary y-axis for the bar plot
  # Set the label for the right y-axis
  
ax2.set_ylabel('Volume', color='red')
ax2.tick_params(axis='y', labelcolor='red')  # Set the tick parameters for the right y-axis


ax_button = plt.subplot(gs[2])
button = Button(ax_button, 'Start/Stop')

# Animation control variable
animation_running = True

def toggle_animation(event):
    global animation_running
    animation_running = not animation_running

button.on_clicked(toggle_animation)

for i in range(0,len(df) - window_size + 1,step_size):
    while not animation_running:
        plt.pause(0.1)  
    
    ax0.clear()
    ax1.clear()
    ax2.clear()

    
    window = df[i:i + window_size]
    current_price = window['현재가']
    time_diff_seconds = window['time_diff_seconds']
    positive_signal = window['positive_signal']
    negative_signal = window['negative_signal']
    transaction_time = window['체결시간']
    
    current_time = transaction_time[-1] + pd.Timedelta(minutes=1)
    
    future_price = df['현재가'][df['체결시간'][df['체결시간']< current_time].max()]
    
    
    # current_price = df['현재가'][i:i + window_size]
    # time_diff_seconds = df['time_diff_seconds'][i:i + window_size]
    # positive_signal = df['positive_signal'][i:i + window_size]
    # negative_signal = df['negative_signal'][i:i + window_size]
    # positive_volume = df['positive_volume'][i:i + window_size]
    # negative_volume = df['negative_volume'][i:i + window_size]
    


    ax0.plot(transaction_time,current_price, color='purple')
    ax0.xaxis.set_major_formatter(formatter) 
    ax0.set_ylim(price_min, price_max)
    
    ax1.plot(time_diff_seconds, color='green')
    ax1.set_title('Time Difference and Volume Over Time')
    ax1.set_xlabel('Index')
    ax1.set_ylabel('Time Difference (Seconds)', color='green')
    ax1.tick_params(axis='y', labelcolor='green')
    ax1.set_ylim(0, max_y_value)


    ax2.bar(range(i, i + window_size), positive_signal, color='red', alpha=0.6)
    ax2.bar(range(i, i + window_size), negative_signal, color='blue', alpha=0.6, bottom=positive_signal)
    # ax2.set_ylim(0,max_signal)
    
    plt.draw()
    plt.pause(0.0001)


        
