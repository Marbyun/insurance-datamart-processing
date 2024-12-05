from datetime import timedelta, datetime
import logging
import pendulum
import pandas as pd
from sqlalchemy import create_engine
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

import queries.etl_assurance_process as etl

log = logging.getLogger(__name__)

local_tz = pendulum.timezone("Asia/Jakarta")
date_today = datetime.now()
env = Variable.get("my_credential", deserialize_json=True)
db_uri = env["db_uri"]

conn_postgre = 'local_postgresql'


def insert_csv_data():
    engine = create_engine(db_uri)
    csv_files_tables_map = {
        'dags/internal_data/DE_assumptions.csv' : 'landing_assumptions',
        'dags/internal_data/DE_claims.csv' : 'landing_claims',
        'dags/internal_data/DE_insurance_contracts.csv' : 'landing_insurance_contracts'
    }

    for csv_file, landing_table in csv_files_tables_map.items():
        df = pd.read_csv(csv_file)
        df.to_sql(name=landing_table, con=engine, if_exists='replace', index=False)
        print(f'Data from {csv_file} inserted into {landing_table} successfully.')

with DAG(
        dag_id='dag_etl_assurance_process',
        start_date=pendulum.datetime(2024, 12, 4, tz='Asia/Jakarta'),
        schedule_interval=None,
        catchup=False,
        max_active_runs=1,
) as dag:

    task_start = DummyOperator(
        task_id = 'start'
    )
    task_end = DummyOperator(
        task_id = 'end'
    )

    task_insert_csv_data_to_landing = PythonOperator(
        task_id = 'insert_csv_data_to_landing',
        python_callable=insert_csv_data,
    )

    task_delete_datamart = SQLExecuteQueryOperator(
        task_id = 'delete_datamart',
        conn_id =conn_postgre,
        sql = etl.DELETE_DATA
    )

    task_insert_datamart = SQLExecuteQueryOperator(
        task_id = 'insert_datamart',
        conn_id = conn_postgre,
        sql = etl.INSERT_DATAMART
    )

    task_start >> task_insert_csv_data_to_landing >> task_delete_datamart >> task_insert_datamart >>  task_end