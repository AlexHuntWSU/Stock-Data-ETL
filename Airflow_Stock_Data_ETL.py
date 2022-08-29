import pandas as pd
import requests
from datetime import datetime, timedelta
import json
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.operators.python import PythonOperator

mysql = MySqlHook(mysql_conn_id="MySQL") #Used MySQL because DB2 couldn't connect to Airflow'
conn = mysql.get_conn()
cursor = conn.cursor()

def extract_stock_data(**kwargs):
    symbols = ()
    cursor.execute("SELECT STOCK_SYMBOL FROM COMPANIES WHERE ACTIVE_IND = 'Y'")
    for row in cursor.fetchall():
        symbols = symbols + row[0]
    symbols = ','.join(map(str,symbols)) #Adds up all the active stock symbols into a string to be passed into the API request
    url = "http://api.marketstack.com/v1/eod/latest"
    headers = {
    "access_key": "*****************************", #Make sure API keys are hidden when uploading to public repositories
    "symbols": symbols
    }

    response = requests.request("GET", url, params=headers)

    status_code = response.status_code
    result = response.text
    return response 

def transform_stock_data(**kwargs):
    ti = kwargs['ti']
    response = ti.xcom_pull(task_id='extract_data') #Airflow cross-communication is necessary for passing data between tasks
    json = response.json()['data'] #Returns the nested data section from the API response
    data = pd.json_normalize(json)
    data = data.drop(columns=['adj_high','adj_low','adj_open','adj_volume','split_factor','exchange'])
    data['volume'] = data['volume'].astype(int)
    data['date'] = pd.to_datetime(data['date']).dt.date
    return data

def load_stock_data(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_id='transform_data')
    max_id_sql = "SELECT MAX(STOCK_PRICE_ID) FROM STOCK_PRICE" #Gets the last primary key value. Auto-increment would not work due to the manual csv upload
    cursor.execute(max_id_sql)
    max_id = cursor.fetchone()[0]
    
    cols='STOCK_PRICE_ID,STOCK_SYMBOL,STOCK_DATE,STOCK_OPEN,STOCK_CLOSE,STOCK_LOW,STOCK_HIGH, \
    STOCK_VOLUME,STOCK_DIVIDENDS'

    for i,row in data.iterrows():
        max_id = max_id + 1
        insert="{},'{}','{}',{},{},{},{},{},{}".format(max_id, row[7],row[8],row[0],row[3],
                                                   row[2],row[1],row[4],row[6])
        sql = "INSERT INTO STOCK_PRICE (" +cols+ ") VALUES("+insert+")"   
        cursor.execute(sql)
        conn.commit()

default_args = {
    'owner': 'Alex',
    'start_date': datetime(2022, 8, 25),
    'email': ['test@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='Stock_Data_ETL',
    schedule_interval='0 20 * * *', # 8:00PM everyday, well after the market close
    description='Loads daily stock data to database',
    default_args=default_args,
)

extract_data_task = PythonOperator(task_id='extract_data',
                                   python_callable = extract_stock_data,
                                   provide_context = True,
                                   dag=dag,
                                  )

transform_data_task = PythonOperator(task_id='transform_data',
                                     python_callable = transform_stock_data,
                                     provide_context = True,
                                     dag=dag,
                                    )

load_data_task = PythonOperator(task_id='load_data',
                                python_callable = load_stock_data,
                                provide_context = True,
                                dag=dag,
                               )

extract_data_task >> transform_data_task >> load_data_task

conn.close()