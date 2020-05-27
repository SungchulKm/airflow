#-*- coding:utf-8 -*-
import airflow.utils.dates
import logging
import os
from urllib.parse import quote_plus
from datetime import timedelta, datetime, date

from airflow import DAG
from airflow.models import Variable
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_to_mysql_operator import BigQueryToMySqlOperator
from airflow.contrib.operators.gcp_sql_operator import CloudSqlQueryOperator
from airflow.hooks.base_hook import BaseHook

#Set GCP Region Name from Composer environment variable
loc_ga = Variable.get("prod.locaion.ga")

#Set BQ Dataset and Table name from Composer environment variable
reportMartDSTable  = Variable.get("prod.martdataset") + '.' + Variable.get("prod.marttable")

#Get connection setting from Airflow connection
connection = BaseHook.get_connection('prod.con.cloudsql')

#Use connection.extra_Dejson.get() function for getting extra json values from Airflow connection
mysql_kwargs = dict(
    user        = quote_plus(connection.login),
    password    = quote_plus(connection.password),
    public_port = connection.port,
    public_ip   = quote_plus(connection.host),
    project_id  = quote_plus(connection.extra_dejson.get('project_id')),
    location    = quote_plus(connection.extra_dejson.get('location')),
    instance    = quote_plus(connection.extra_dejson.get('instance')),
    database    = quote_plus(connection.schema)
)

# Set MYSQL Connection variables on OS environment variable 
os.environ['AIRFLOW_CONN_PUBLIC_MYSQL_TCP'] = \
    "gcpcloudsql://{user}:{password}@{public_ip}:{public_port}/{database}?" \
    "database_type=mysql&" \
    "project_id={project_id}&" \
    "location={location}&" \
    "instance={instance}&" \
    "use_proxy= False&" \
    "use_ssl=False".format(**mysql_kwargs)


#Set Connection Name from AIRFLOW_CONN_PUBLIC_MYSQL_TCP environment variable
connection_name = "public_mysql_tcp"

#Get SQL Connection values from Airflow connection variables, Conn_ID : google_cloud_sql_default
cloudsql_connection = BaseHook.get_connection('prod.con.cloudsql')

default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('dmp_proc_report'
        , default_args=default_args
        # , on_success_callback=cleanup_xcom
        , description='Prebuilt Report Generation pipeline'
        , schedule_interval=None #ga data 수집완료시점으로 매일 아침 7시(한국시간) '0 7 * * *' 일배치 수행
        , start_date=airflow.utils.dates.days_ago(2) #기준일을 지난 기간까지 포함할 경우 이전 날짜로 지정 start_date=datetime(2019,11,13) 
        , catchup=False)

# Cloud SQL 의 DP_TB_PR_CRT_RPRT_INF Table 의 모든 건을 삭제
delete_mart_mysql = CloudSqlQueryOperator(
    task_id = "delete_mart_in_mysql",
        gcp_cloudsql_conn_id = connection_name, 
        sql = "DELETE FROM DKDMPDB.DP_TB_PR_CRT_RPRT_INF",
    retries=1,
    dag=dag)

# BigQuery 의 Report Mart Table 의 모든 건을 CloudSQL 의 Table 로 Load
load_bq_mart_to_mysql = BigQueryToMySqlOperator(
    task_id = "load_bqmart_to_mysql",
        dataset_table = reportMartDSTable,
        mysql_table = "DP_TB_PR_CRT_RPRT_INF",
        gcp_conn_id = "google_cloud_default",
        mysql_conn_id = cloudsql_connection,
        selected_fields = "SGM_NO,ITM_ID,PRP_NM,SUB_PRP_NM,POPART_V,OJ_CONS_RT,AM_CONS_RT",
        replace = True,
        batch_size = 50000,
        location = loc_ga,
    retries=2,
    dag=dag)

delete_lpimsmart_mysql >> load_bq_lpimsmart_to_mysql 

