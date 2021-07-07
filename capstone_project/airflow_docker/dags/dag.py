from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr_step import EmrStepSensor
from airflow.contrib.operators.emr_terminate_job_flow_operator import  EmrTerminateJobFlowOperator


bucket_name="sparkprojectcode"
bootstrap_script="install_requirements.sh"
spark_etl="/mnt1/CapstoneProject/main.py"
deploy_mode="client"


default_args = {
    'owner': 'CapstoneProject',
    'start_date': datetime(2021, 7, 3),
    'depends_on_past': False,
    'email': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

with DAG('Spark_S3_ETL',
          default_args=default_args,
          description='Load and transform data S3 using spark and airflow',
          schedule_interval='@once'
        ) as dag:

    start_operator = DummyOperator(task_id='Begin_execution')


    emr_create_config = {
        "Name": "Capstone EMR cluster",
        "ReleaseLabel": "emr-5.33.0",
        "LogUri": "s3://{{params.BUCKET_NAME}}/logs",
        "Applications": [{"Name": "Hadoop"}, {"Name": "Spark"}, {"Name": "JupyterHub"}],
        "Configurations": [
            {
                "Classification": "spark-env",
                "Configurations": [
                    {
                        "Classification": "export",
                        "Properties": {"PYSPARK_PYTHON": "/usr/bin/python3"},
                    }
                ],
            }
        ],
        "Instances": {
            "InstanceGroups": [
                {
                    "Name": "Master node",
                    "Market": "SPOT",
                    "InstanceRole": "MASTER",
                    "InstanceType": "m5.xlarge",
                    "InstanceCount": 1,
                },
                {
                    "Name": "Slave node",
                    "Market": "SPOT",
                    "InstanceRole": "CORE",
                    "InstanceType": "m5.xlarge",
                    "InstanceCount": 2,
                },
            ],
            "KeepJobFlowAliveWhenNoSteps": True,
            "TerminationProtected": False,
            "Ec2KeyName": "spark-cluster",
        },
        "BootstrapActions" : [
            {
                "Name": "Install python dependencies",
                "ScriptBootstrapAction" :
                    {
                        "Path": "s3://{{params.BUCKET_NAME}}/{{params.BOOTSTRAP_RUN}}"
                    },
            }
        ],
        "JobFlowRole": "EMR_EC2_DefaultRole",
        "ServiceRole": "EMR_DefaultRole",
        "VisibleToAllUsers": True
    }


    # Create an EMR cluster
    create_emr_cluster = EmrCreateJobFlowOperator(
        task_id="create_emr_cluster",
        job_flow_overrides=emr_create_config,
        aws_conn_id="aws_credentials",
        emr_conn_id="emr_default",
        params={
                "BUCKET_NAME": bucket_name,
                "BOOTSTRAP_RUN": bootstrap_script
                },
    )

    spark_step = [
        {
            "Name": "Spark Job Capstone",
            "ActionOnFailure": "CANCEL_AND_WAIT",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": [
                    "spark-submit",
                    "--deploy-mode",
                    "{{params.DEPLOY_MODE}}",
                    # "--packages",
                    # "{{params.PACKAGES}}",
                    # "--py-files", "/mnt1/CapstoneProject/",
                    "{{params.ETL_SCRIPT}}",
                ],
            },
        },
    ]


    run_spark_submit = EmrAddStepsOperator(
        task_id="spark_submit_step",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', "
                    "key='return_value') }}",
        aws_conn_id="aws_credentials",
        steps=spark_step,
        params={"DEPLOY_MODE": deploy_mode,
                # "PACKAGES": jars,
                "ETL_SCRIPT": spark_etl
                },
    )


    watch_step = EmrStepSensor(
    task_id='watch_step',
    job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', "
                "key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='spark_submit_step', "
            "key='return_value')[0] }}",
    aws_conn_id='aws_credentials',
    )


    terminate_cluster = EmrTerminateJobFlowOperator(
    task_id='terminate_cluster',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', "
                "key='return_value') }}",
    aws_conn_id='aws_credentials',
    )


    end_operator = DummyOperator(task_id='Stop_execution')


start_operator >> create_emr_cluster  >> run_spark_submit >> watch_step >> terminate_cluster >> end_operator
# start_operator >> create_emr_cluster  >> run_spark_submit >> step_checker >> end_operator


