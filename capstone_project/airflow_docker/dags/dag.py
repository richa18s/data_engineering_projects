from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr_step import EmrStepSensor
from airflow.contrib.operators.emr_terminate_job_flow_operator import  EmrTerminateJobFlowOperator
from airflow.operators.bash import BashOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable




default_args = {
    'owner': 'CapstoneProject',
    'start_date': datetime(2021, 7, 3),
    'depends_on_past': False,
    'email': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

#. DAG for Pipeline
with DAG('Spark_S3_ETL',
          default_args=default_args,
          description='Load and transform data S3 using spark and airflow',
          schedule_interval='@once'
        ) as dag:


    # . Airflow Variables to run the DAG
    bucket_name = Variable.get("bucket_name")
    bootstrap_script = Variable.get("bootstrap_script")
    spark_etl = Variable.get("spark_etl")
    spark_validate = Variable.get("spark_validate")
    deploy_mode = Variable.get("deploy_mode")
    ec2_key_name = Variable.get("key_name")
    project_home_directory = Variable.get("home_dir")
    project_temp_directory = Variable.get("tmp_dir")


    start_operator = DummyOperator(task_id='Begin_execution')


    #. Create project package in airflow home
    create_project_package = BashOperator(
        task_id='create_project_package',
        bash_command='tar -czf {}/{} -C {} {}'.format(
            project_temp_directory, 'CapstoneProject.tar.gz',
            project_home_directory,'CapstoneProject'),
    )


    #. Method to uploade to s3 from airflow home
    def copy_to_s3(filename, key, bucket_name):
        s3 = S3Hook('aws_credentials')
        s3.load_file(
            filename=filename,
            bucket_name=bucket_name,
            replace=True,
            key=key)



    #. Upload project package to s3
    upload_to_s3 = PythonOperator(
        dag=dag,
        task_id="upload_to_s3",
        python_callable=copy_to_s3,
        op_kwargs={"filename": '{}/{}'.format(project_temp_directory,'CapstoneProject.tar.gz'),
                   "key": 'CapstoneProject.tar.gz',
                   "bucket_name": bucket_name},
    )

    #. Upload Bootstrap task to s3
    upload_bootstrap_task_to_s3 = PythonOperator(
        dag=dag,
        task_id="upload_bootstrap_task_to_s3",
        python_callable=copy_to_s3,
        op_kwargs={"filename": '{}/{}'.format(project_home_directory,'install_requirements.sh'),
                   "key": 'install_requirements.sh',
                   "bucket_name": bucket_name},
    )


    #. Configuration EMR cluster
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
            "Ec2KeyName": "{{params.KEY_NAME}}",
        },
        "BootstrapActions" : [
            {
                "Name": "Install python dependencies",
                "ScriptBootstrapAction" :
                    {
                        "Args": ["{{params.BUCKET_NAME}}"],
                        "Path": "s3://{{params.BUCKET_NAME}}/{{params.BOOTSTRAP_RUN}}"
                    },
            }
        ],
        "JobFlowRole": "EMR_EC2_DefaultRole",
        "ServiceRole": "EMR_DefaultRole",
        "VisibleToAllUsers": True
    }


    #. Create EMR cluster using EMR configuration
    create_emr_cluster = EmrCreateJobFlowOperator(
        task_id="create_emr_cluster",
        job_flow_overrides=emr_create_config,
        aws_conn_id="aws_credentials",
        emr_conn_id="emr_default",
        params={
                "BUCKET_NAME": bucket_name,
                "BOOTSTRAP_RUN": bootstrap_script,
                "KEY_NAME": ec2_key_name
                },
    )


    #. Configuration to run spark-submit
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
                    "{{params.ETL_SCRIPT}}",
                ],
            },
        },
    ]


    #. Issue spark-submit to run ETL
    run_etl_spark = EmrAddStepsOperator(
        task_id="run_etl_spark",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', "
                    "key='return_value') }}",
        aws_conn_id="aws_credentials",
        steps=spark_step,
        params={"DEPLOY_MODE": deploy_mode,
                # "PACKAGES": jars,
                "ETL_SCRIPT": spark_etl
                },
    )


    #. Watch step to ensure it wait until create cluster and spark-submit-etl is complete
    watch_step_etl = EmrStepSensor(
    task_id='watch_step_etl',
    job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', "
                "key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='run_etl_spark', "
            "key='return_value')[0] }}",
    aws_conn_id='aws_credentials',
    )


    #. Run Validation Configuration
    validation_step = [
        {
            "Name": "Validate Job Capstone",
            "ActionOnFailure": "CANCEL_AND_WAIT",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": [
                    "spark-submit",
                    "--deploy-mode",
                    "{{params.DEPLOY_MODE}}",
                    "{{params.VALIDATION_SCRIPT}}",
                ],
            },
        },
    ]


    #. Validate the data uploaded in schema
    run_data_validation_spark = EmrAddStepsOperator(
        task_id="run_data_validation_spark",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', "
                    "key='return_value') }}",
        aws_conn_id="aws_credentials",
        steps=validation_step,
        params={"DEPLOY_MODE": deploy_mode,
                # "PACKAGES": jars,
                "VALIDATION_SCRIPT": spark_validate
                },
    )


    #. Wait for validation to be completed
    watch_step_validate = EmrStepSensor(
    task_id='watch_step_validate',
    job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', "
                "key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='run_data_validation_spark', "
            "key='return_value')[0] }}",
    aws_conn_id='aws_credentials',
    )


    #. Terminate the cluster when all steps are completed.
    terminate_cluster = EmrTerminateJobFlowOperator(
    task_id='terminate_cluster',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', "
                "key='return_value') }}",
    aws_conn_id='aws_credentials',
    )


    end_operator = DummyOperator(task_id='Stop_execution')





#. Arrangement / dependencies of tasks in pipeline
start_operator >> create_project_package >> upload_to_s3
start_operator >> upload_bootstrap_task_to_s3
upload_bootstrap_task_to_s3 >> create_emr_cluster
upload_to_s3 >> create_emr_cluster >> run_etl_spark
run_etl_spark >> watch_step_etl >> run_data_validation_spark
run_data_validation_spark >> watch_step_validate
watch_step_validate >> terminate_cluster >> end_operator
