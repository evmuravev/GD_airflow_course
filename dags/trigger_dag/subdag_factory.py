from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.models import Variable

from pprint import pprint


def load_subdag(parent_dag_name, child_dag_name, args):
    dag_subdag = DAG(
        dag_id='{0}.{1}'.format(parent_dag_name, child_dag_name),
        default_args=args,
        schedule_interval=None,
    )
    with dag_subdag:
        run_subdag = ExternalTaskSensor(
            task_id="run_subdag",
            external_dag_id='dag_id_1',
            external_task_id=None,
            timeout=60*60*24-1,
            poke_interval=30,
            allowed_states=['success'],
            mode="reschedule",
        )
        def _print_result(**kwargs):
            result =  kwargs['ti'].xcom_pull(key='return_value', dag_id='dag_id_1', task_ids='query_table') 
            print(result)
            pprint(kwargs)

        print_result = PythonOperator(
            task_id="print_result",
            python_callable=_print_result,
        )

        remove_run_file = BashOperator(
            task_id='remove_run_file',
            bash_command=f"rm {Variable.get('name_path_variable')};",
        )

        create_finished_file = BashOperator(
            task_id='create_finished_file',
            bash_command="touch /opt/airflow/logs/finished_{{ ts_nodash }};",
        )

        run_subdag >> print_result >> remove_run_file >> create_finished_file

    return dag_subdag