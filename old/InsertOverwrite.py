from airflow import DAG
import airflow.utils.dates
import logging
import re

from airflow.contrib.operators.bigquery_operator import BigQueryOperator, BigQueryCreateEmptyTableOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

from datetime import timedelta, datetime
#from module_variables import GlobalVariables 

#gvar = GlobalVariablesSet()

pj_bigquery = 'sa-bigdata-dev'
bq_location = 'asia-northeast3'
ds_demo     = 'hive_test'
tb_origin   = 'device_origin'
tb_profile  = 'device_profile'
#base_date   = Variable.get("demo.base_date")

default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('example_insertoverwrite'
        , default_args=default_args
        , description='Example Insert Overwrite'
        , schedule_interval=None #ga data 수집완료시점으로 매일 아침 7시(한국시간) '0 7 * * *' 일배치 수행
        , start_date=airflow.utils.dates.days_ago(2) #기준일을 지난 기간까지 포함할 경우 이전 날짜로 지정 start_date=datetime(2019,11,13) 
        , catchup=False
        )

def insert_overwrite(date):

    str_date = re.sub("-", '', date) 
    print ('str_date : %s' %str_date )
    
    obj = BigQueryOperator(
        task_id = 'insertOverwrite_{}'.format(date),
        write_disposition='WRITE_TRUNCATE', # WRITE_TRUNCATE, WRITE_APPEND, WRITE_EMPTY
        create_disposition = 'CREATE_IF_NEEDED',
        # priority="BATCH",
        allow_large_results=True,
        use_legacy_sql=False,
        location=bq_location,
        sql=""" 
            SELECT 
                CAST (cyymmdd AS DATE ) AS cyymmdd,
                un,
                rgn_cd,
                cnty_cd,
                tcom_cd,
                dvc_gp_id,
                dvc_modl_id ,
                fw_ver,
                cp_ver,
                hw_ver,
                os_ver
            FROM sa-bigdata-dev.hive_test.device_origin
            WHERE cyymmdd = "{}" """.format(date),
        destination_dataset_table= pj_bigquery+'.'+ ds_demo +'.' + tb_profile + '$' + str_date,
        # maximum_billing_tier=1, 
        #trigger_rule=TriggerRule.ALL_SUCCESS,
        retries=5,
        retry_delay=timedelta(seconds=5),
        dag=dag)
    return obj

dates = ['2020-05-01', '2020-05-02', '2020-05-03']

for date in dates:
    insert_overwrite(date)


