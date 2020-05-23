#!/bin/env/python
import os
import airflow
from airflow import models
from airflow.contrib.operators.dataflow_operator import DataFlowJavaOperator

GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID', 'irn-67491-dev-1122254548')
GCS_TMP = os.environ.get('GCP_DATAFLOW_GCS_TMP', 'gs://renault-dll-67491-dll-dev/beam/temp/')
GCS_STAGING = os.environ.get('GCP_DATAFLOW_GCS_STAGING', 'gs://renault-dll-67491-dll-dev/beam/staging/')
GCS_OUTPUT = os.environ.get('GCP_DATAFLOW_GCS_OUTPUT', 'gs://renault-dll-67491-dll-dev/beam/result')
GCS_JAR = os.environ.get('GCP_DATAFLOW_JAR', 'gs://renault-dll-67491-dll-dev/beam/beam-examples-bundled-0.1.jar')

default_args = {
    "start_date": airflow.utils.dates.days_ago(1),
    'dataflow_default_options': {
        'project': GCP_PROJECT_ID,
        'tempLocation': GCS_TMP,
        'stagingLocation': GCS_STAGING,
    }
}

with models.DAG(
    "example_gcp_dataflow",
    default_args=default_args,
    schedule_interval=None,  # Override to match your needs
) as dag:

    # [START howto_operator_start_java_job]
    start_java_job = DataFlowJavaOperator(
        task_id="start-java-job",
        jar=GCS_JAR,
        job_name='{{task.task_id}}22222255sss{{ macros.uuid.uuid4() }}',
        options={'inuptFile': 'gs://renault-dll-67491-dll-dev/beam/input_csv.txt',
                'output':GCS_OUTPUT
            },
        job_class='co.enydata.tutorial.beam.example3.CsvToAvro'
    )
    # [END howto_operator_start_java_job]