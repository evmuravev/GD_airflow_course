# Apache Airflow (Python Engineer to BigData Engineer) 

Code for the Apache Airflow (Python Engineer to Big Data Engineer) project. Each commit corresponds to a practical task in the course.
## Contents

This repository contains the following files:

```
├── dags                            <- Folder containing DAGs.
│   ├── custom                      <- Custom sensor and operator used in the DAGs.
│   │   ├── __init__.py
│   │   ├── custom_file_sensors.py.py            <- Smart FileSensor
│   │   └── postgre_sql_count_rows_operator.py   <- Operator that fetches count rows from table.
│   │   
│   ├── jobs_dag.py                 <- The main group of DAGs, one of which is triggered by trigger_dag.py
│   ├── task_group_example.py       <- the task group dag example 
│   └── trigger_dag.zip             <- Packaged DAG
│       ├── slackapi            
│       │   ├── __init__.py
│       │   └── send_message.py     <- Function to post message to slack
│       ├── subdag_factory.py       <- Function to create subdag
│       └── trigger_dag.py          <- DAG triggering another dag, subdag and sending an alert to slack 
│   
├── docker-compose.yml
└── README.md
```

## Usage

To get started with the project, start Airflow in docker using the following command:
```shell
    docker-compose up airflow-init
```
After initialization, run the containers with the command:
```shell
    docker-compose up -d
```
Wait for a few seconds and you should be able to access the dags at http://localhost:8080/.


After restarting the containers, an airflow variable containing the path to the file for the trigger will be available, but you can freely create and use the variable in hashicorp vault, which runs in dev mode, so it is cleared on restart.