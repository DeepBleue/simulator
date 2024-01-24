import mysql.connector
from datetime import datetime
import mysql.connector
import pandas as pd
import matplotlib.pyplot as plt
import pandas as pd
import matplotlib.font_manager as fm
import mplcursors
import mplcursors

# Set Korean font



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
        df = pd.read_sql(query, conn)

        return df

    except mysql.connector.Error as error:
        print(f"Error: {error}")
        return None
    finally:
        if conn.is_connected():
            conn.close()





if __name__ == '__main__':
    plt.rcParams['font.family'] = 'Malgun Gothic' 


    databaseA = "A" + datetime.now().strftime("%Y%m%d") 
    # databaseA = "A20240111"
    password = '93150lbm!!'

    tables = get_table_names('127.0.0.1', 'root', password, databaseA)

    code = tables[0][0]
    print(code)

    # code = '021080'

    # Usage
    df = table_to_dataframe('127.0.0.1', 'root', password, databaseA, code)
    if df is not None:
        print(df)
        # Assuming df is your DataFrame
        df_sorted = df.sort_values(by='체결시간')
        print(df_sorted)
        
        
        
        # Separate positive and negative values of '거래량'
    df_sorted['거래량'] = pd.to_numeric(df_sorted['거래량'])
    df_sorted['positive_volume'] = df_sorted['거래량'].apply(lambda x: x if x > 0 else 0)
    df_sorted['negative_volume'] = df_sorted['거래량'].apply(lambda x: abs(x) if x < 0 else 0)

    df_sorted['체결시간'] = pd.to_datetime(df_sorted['체결시간'])
    df_sorted.set_index('체결시간', inplace=True)
    df_grouped = df_sorted.resample('T').sum()[['positive_volume', 'negative_volume']]
    # df_grouped = df_sorted.resample('T', on='체결시간').sum()[['positive_volume', 'negative_volume']]

    # Plotting
    plt.figure(figsize=(12,6))
    ax = df_grouped.plot(kind='bar', stacked=True, color=['red','blue'])

    # Adding cursor functionality
    cursor = mplcursors.cursor(ax, hover=True)

    @cursor.connect("add")
    def on_add(sel):
        idx = sel.target.index
        transaction_time = df_grouped.index[idx].strftime('%Y-%m-%d %H:%M:%S')
        positive_vol = df_grouped['positive_volume'].iloc[idx]
        negative_vol = df_grouped['negative_volume'].iloc[idx]
        sel.annotation.set(text=f'체결시간: {transaction_time}\n양(+) 거래량: {positive_vol}\n음(-) 거래량: {negative_vol}')
        sel.annotation.get_bbox_patch().set(fc="white", alpha=0.6)

    # Set x-ticks
    ticks = ax.get_xticks()
    ticklabels = [l.get_text() for l in ax.get_xticklabels()]
    ax.set_xticks(ticks[::5])  # Show every 5th tick
    ax.set_xticklabels(ticklabels[::5], rotation=45, ha="right")  # Adjust rotation and alignment as needed

    plt.title('거래량 by Minute')
    plt.xlabel('Minute')
    plt.ylabel('거래량')
    plt.tight_layout()
    plt.show()
            
        
        
        
        
        
        
        
        
    
    
    # # Convert '체결시간' to datetime
    # df_sorted['체결시간'] = pd.to_datetime(df_sorted['체결시간'])

    # # Convert '거래량' to numeric, removing any '+' or '-'
    # df_sorted['거래량'] = pd.to_numeric(df_sorted['거래량'].str.replace('+','').str.replace('-',''), errors='coerce')

    # # Group by minute and sum '거래량'
    # df_grouped = df_sorted.resample('T', on='체결시간').sum()

    # # Set Korean font
    # plt.rcParams['font.family'] = 'Malgun Gothic' 

    # # Assuming df_grouped is your DataFrame grouped by minute

    # # Plotting
    # plt.figure(figsize=(12,6))
    # ax = df_grouped['거래량'].plot(kind='bar')

    # # Adding cursor functionality
    # cursor = mplcursors.cursor(ax, hover=True)

    # @cursor.connect("add")
    # def on_add(sel):
    #     idx = sel.target.index
    #     transaction_time = df_grouped.index[idx].strftime('%Y-%m-%d %H:%M:%S')
    #     volume = df_grouped['거래량'].iloc[idx]
    #     sel.annotation.set(text=f'체결시간: {transaction_time}\n거래량: {volume}')
    #     sel.annotation.get_bbox_patch().set(fc="white", alpha=0.6)

    # # Set x-ticks
    # ticks = ax.get_xticks()
    # ticklabels = [l.get_text() for l in ax.get_xticklabels()]
    # ax.set_xticks(ticks[::5])  # Show every 5th tick
    # ax.set_xticklabels(ticklabels[::5], rotation=45, ha="right")  # Adjust rotation and alignment as needed

    # plt.title('거래량 by Minute')
    # plt.xlabel('Minute')
    # plt.ylabel('거래량')
    # plt.tight_layout()  # Adjust layout
    # plt.show()