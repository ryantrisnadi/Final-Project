from airflow.models import DAG
from airflow.operators.python import PythonOperator
#from airflow.providers.postgres.operators.postgres import PostgresOperator
# from airflow.utils.task_group import TaskGroup
from datetime import datetime
from sqlalchemy import create_engine #koneksi ke postgres
import pandas as pd
from datetime import datetime, timedelta
import pendulum
from elasticsearch import Elasticsearch
#url dataset = https://www.kaggle.com/datasets/sajkazmi/cleaned-pakistan-biggest-ecommerce-dataset

# from elasticsearch.helpers import bulk
database = "airflow_m3"
username = "airflow_m3"
password = "airflow_m3"
host = "postgres"

# Membuat URL koneksi PostgreSQL
postgres_url = f"postgresql+psycopg2://{username}:{password}@{host}/{database}"

def load_csv_to_postgres():
    '''
    fungsi ini adalah fungsi untuk mengload raw data csv ke database yang ada di postgres
    '''
    # Gunakan URL ini saat membuat koneksi SQLAlchemy
    engine = create_engine(postgres_url)
    # engine= create_engine("postgresql+psycopg2://airflow:airflow@postgres/airflow")
    conn = engine.connect()

    df = pd.read_csv('/opt/airflow/dags/Sales_Data.csv')
    #df.to_sql(nama_table_db, conn, index=False, if_exists='replace')
    df.to_sql('final_project', conn, 
              index=False, 
              if_exists='replace')  # M
    

def ambil_data():
    '''
    setelah saya mengirim data saya ke postgres, saya tarik kembali untuk melakukan data cleaning 
    '''

    # Gunakan URL ini saat membuat koneksi SQLAlchemy
    engine = create_engine(postgres_url)
    conn = engine.connect()

    df = pd.read_sql_query("select * from final_project", conn) #nama table sesuaikan sama nama table di postgres
    df.to_csv('/opt/airflow/dags/sales_data_new.csv', sep=',', index=False)
    


def preprocessing(): 
    ''' fungsi untuk membersihkan data
    dalam fungsi ini saya melakukan proses perubahan tipe data, mengubah nama kolom dari uppercase menjadi lowercase saya juga menghapus tabel yang tidak diperlukan
    '''
    # pembisihan data
    data = pd.read_csv("/opt/airflow/dags/sales_data_new.csv")

    # bersihkan data 
    data.drop_duplicates(inplace=True)
    data['Order Date'] = pd.to_datetime(data['Order Date'])
    del data['Unnamed: 0']
    data.columns = data.columns.str.lower()
    data.columns = data.columns.str.replace(' ', '_')

    data.to_csv('/opt/airflow/dags/sales_data_clean.csv', index=False)
    
def upload_to_elasticsearch():
    '''
    setelah saya melakukan processing, saya mengupload data yang sudah dibersihkan ke elastic search
    untuk dibuat visualisasinya di kibana
    '''
    es = Elasticsearch("http://elasticsearch:9200")
    df = pd.read_csv('/opt/airflow/dags/sales_data_clean.csv')
    
    for i, r in df.iterrows():
        doc = r.to_dict()  # Convert the row to a dictionary
        res = es.index(index="table_testing", id=i+1, body=doc)
        print(f"Response from Elasticsearch: {res}")
        

        
default_args = {
    'owner': 'Vincent', 
    'start_date': datetime(2024, 6, 25, 6, 30, tzinfo=pendulum.timezone('Asia/Jakarta')),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    "Finpro", #atur sesuai nama project kalian
    description='Final_project',
    schedule_interval='30 6 * * *', #atur schedule untuk menjalankan airflow pada 06:30.
    default_args=default_args, 
    catchup=False
) as dag:
    
    # Task : 1
    load_csv_task = PythonOperator(
        task_id='load_csv_to_postgres',
        python_callable=load_csv_to_postgres) #sesuaikan dengan nama fungsi yang kalian buat
    
    #task: 2
    ambil_data_pg = PythonOperator(
        task_id='ambil_data_postgres',
        python_callable=ambil_data) #
    

    # Task: 3
    '''  Fungsi ini ditujukan untuk menjalankan pembersihan data.'''
    edit_data = PythonOperator(
        task_id='edit_data',
        python_callable=preprocessing)

    # Task: 4
    upload_data = PythonOperator(
        task_id='upload_data_elastic',
        python_callable=upload_to_elasticsearch)

    #proses untuk menjalankan di airflow
    load_csv_task >> ambil_data_pg >> edit_data >> upload_data



