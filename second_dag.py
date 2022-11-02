import datetime as dt
from datetime import timedelta
import requests
from bs4 import BeautifulSoup
import psycopg2 as db

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator

dict_month = {'Januari':1,
              'Februari':2,
              'Maret':3,
              'April':4,
              'Mei':5,
              'Juni':6,
              'Juli':7,
              'Agustus':8,
              'September':9,
              'Oktober':10,
              'November':11,
              'Desember':12}
            
def scraping(ti): 
    data = requests.get('https://www.bmkg.go.id/')
    soup = BeautifulSoup(data.text, 'html.parser')
    dict_result = {}
    dict_result['Pukul'] = soup.find('span', {'class':'waktu'}).string.split(',')[1].split(' ')[1]
    dict_result['Tanggal'] = soup.find('span', {'class':'waktu'}).string.split(',')[0]
    dict_result['Tanggal'] = dict_result['Tanggal'].split(' ')[-1]+'-'+str(dict_month[dict_result['Tanggal'].split(' ')[-2]])+'-'+dict_result['Tanggal'].split(' ')[-3]
    count = 0
    for i in soup.find('ul', {'class':'list-unstyled'}).findChildren('li'):
        if count==1:
            dict_result["Magnitudo"] = float(i.text)
        elif count==2:
            dict_result["Kedalaman"] = int(i.text.split(" ")[0])
        elif count==3:
            dict_result["Koordinat"] = i.text
        elif count==4:
            dict_result["Lokasi"] = i.text
        count += 1  

    ti.xcom_push(key = 'time', value = dict_result['Tanggal']+' '+dict_result['Pukul'])
    ti.xcom_push(key = 'magnitudo', value = dict_result["Magnitudo"])
    ti.xcom_push(key = 'kedalaman', value = dict_result["Kedalaman"])
    ti.xcom_push(key = 'koordinat', value = dict_result["Koordinat"])
    ti.xcom_push(key = 'lokasi', value = dict_result["Lokasi"])

def get_result(ti):
    conn_string="dbname='postgres' host='localhost' user='postgres' password='postgres'"
    conn=db.connect(conn_string)
    cur=conn.cursor()
    result_scraping = ti.xcom_pull(task_ids='scraping', key='time')
    query2 = "select * from indonesian_earthquakes where datetime ="+ f"'{result_scraping}'"
    cur.execute(query2)
    if cur.fetchall():
        return 'old_data'
    else:
        return 'new_data'

def to_postgres(ti):
    conn_string="dbname='postgres' host='localhost' user='postgres' password='postgres'"
    conn=db.connect(conn_string)
    cur=conn.cursor()

    datetime = ti.xcom_pull(task_ids='scraping', key='time')
    magnitude = ti.xcom_pull(task_ids='scraping', key='magnitudo')
    depth = ti.xcom_pull(task_ids='scraping', key='kedalaman')
    coordinate = ti.xcom_pull(task_ids='scraping', key='koordinat')
    location = ti.xcom_pull(task_ids='scraping', key='lokasi')

    data_for_db=(datetime, magnitude, depth, coordinate, location)

    query = "INSERT INTO indonesian_earthquakes (datetime, magnitude, depth_km, coordinate_idn, location_idn) values(%s,%s,%s,%s,%s)"
    cur.execute(query,data_for_db)
    conn.commit() 


default_args = {
    'owner': 'Fadlil',
    'start_date': dt.datetime(2022, 10, 26),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5),
}


with DAG('ScrapingAutomationDAG',
         default_args=default_args,
         schedule_interval=timedelta(minutes=5),      # '0 * * * *',
         ) as dag:

    print_starting = BashOperator(task_id='starting',
                               bash_command='echo "I am reading the CSV now....."')
    
    scraping_bmkg = PythonOperator(task_id='scraping',
                                 python_callable=scraping)

    get_result_bmkg = BranchPythonOperator(task_id='get_result',
                                 python_callable=get_result)

    bash2 = BashOperator(task_id='new_data',
                               bash_command='echo "We get new data"')
    
    bash3 = BashOperator(task_id='old_data',
                               bash_command='echo "there is old data"')
    
    insert_to_postgres = PythonOperator(task_id='insert_to_postgres',
                                 python_callable=to_postgres)

print_starting >> scraping_bmkg >> get_result_bmkg
get_result_bmkg >> bash2 >> insert_to_postgres
get_result_bmkg >> bash3