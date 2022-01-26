from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.python import PythonSensor
from airflow.sensors.filesystem import FileSensor
from airflow.operators.subdag import SubDagOperator
from airflow.models import Variable
from pathlib import Path
from airflow.hooks.base_hook import BaseHook
import json
from custom.custom_file_sensors import CustomFileSensor
from subdag_factory import load_subdag
from slackapi.send_message import send_message


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2021, 10, 5)
}

dag = DAG(
    dag_id="trigger_dag",
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
)

with dag:

    wait_run_file = CustomFileSensor(
            task_id=f"wait_run_file",
            filepath=Variable.get('name_path_variable'),
            timeout=60*60*24-1,
            poke_interval=30,
    )

    triger_dag = TriggerDagRunOperator(
        task_id="triger_dag",
        trigger_dag_id = 'dag_id_1',
        execution_date='{{execution_date}}',
        wait_for_completion = True,
        poke_interval=30,
    )

    run_sub_dag = SubDagOperator(
        task_id="run_sub_dag",
        subdag=load_subdag(
            parent_dag_name="trigger_dag",
            child_dag_name="run_sub_dag",
            args=default_args
        ),
    )

    vc_conn = BaseHook.get_connection('slack_conn')
    token = Variable.get('slack_token', json.loads(vc_conn.get_extra())['token'])
    alert_slack = PythonOperator(
            task_id="alert_slack",
            python_callable=send_message,
            op_kwargs={
                "token":token,
                "message":"The dag: {{dag.dag_id}} ran at: {{execution_date}}"}

    )

    wait_run_file >> triger_dag  >> run_sub_dag >> alert_slack



