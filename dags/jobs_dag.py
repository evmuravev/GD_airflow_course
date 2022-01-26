from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator, PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime
from uuid import uuid4

from custom.postgre_sql_count_rows_operator import PostgreSQLCountRowsOperator


def create_dag(dag_id, schedule, start_date):

    dag = DAG(dag_id,
              schedule_interval=schedule,
              start_date=start_date,
              catchup=False,
              default_args={'queue': 'jobs_queue'}
              )

    def _check_table_exist():
        pg_conn = PostgresHook('local_postgres')
        engine = pg_conn.get_sqlalchemy_engine()
        check_table_query = """
            SELECT EXISTS (
                SELECT FROM pg_tables
                WHERE  schemaname = 'public'
                AND    tablename  = 'test_table'
        );"""
        if engine.execute(check_table_query).fetchone()[0]: #return True if table exists
            return 'empty_task'
        else:
            return 'create_table'

    with dag:
        print_process_start = PythonOperator(
            task_id = "print_process_start",
            python_callable= lambda: print('process started') 
        )

        get_current_user = BashOperator(
            task_id="get_current_user",
            bash_command="whoami",
          )

        check_table_exist = BranchPythonOperator(
            task_id="check_table_exist",
            python_callable=_check_table_exist,
        )

        create_table = PostgresOperator(
            task_id = "create_table",
            postgres_conn_id='local_postgres',
            sql='''CREATE TABLE test_table(custom_id integer NOT NULL,
                user_name VARCHAR (50) NOT NULL, timestamp TIMESTAMP NOT NULL);'''
        )

        empty_task = DummyOperator(task_id = "empty_task")

        insert_row = PostgresOperator(
            task_id = "insert_row",
            trigger_rule="none_failed",
            postgres_conn_id='local_postgres',
            sql=f'''INSERT INTO test_table VALUES
                ({uuid4().int % 123456789},
                 '{{{{ti.xcom_pull(key='return_value', task_ids='get_current_user')}}}}', 
                 '{datetime.now()}')''',

        )

        query_table = PostgreSQLCountRowsOperator(
            task_id = "query_table",
            conn_id = "local_postgres",
            table_name = "test_table"
        )
        
        print_process_start >>  get_current_user >> check_table_exist 
        check_table_exist >> [create_table, empty_task] >> insert_row >> query_table

        return dag


config = {
    'dag_id_1': {'schedule_interval': None, "start_date": datetime(2021, 10, 1)},  
    'dag_id_2': {'schedule_interval': "@weekly", "start_date": datetime(2021, 10, 1)},  
    'dag_id_3':{'schedule_interval': "@monthly", "start_date": datetime(2021, 10, 1)}
   }

for dag_id, params in config.items():
    globals()[dag_id] = create_dag(dag_id,
                                  params['schedule_interval'],
                                  params['start_date'])