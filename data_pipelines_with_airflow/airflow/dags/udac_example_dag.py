"""
    Author:
        Udacity, RichaS
    Date Created:
        06/18/2021
    Description:
         - Contains the DAG for apache airflow
         - Process the song and log data sets (in JSON) available in s3 bucket
           and load the data in staging tables using StageToRedshiftOperator
         - Populates fact and dimension tables in star schema from staging tables
           using LoadFactOperator and LoadDimensionOperator
         - Runs data quality checks to ensure each table has data after the
           pipeline is completed, this is performed using DataQualityOperator
"""


from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator,
                                CreateTableOperator)
from helpers import SqlQueries

#. Default arguments for dag
default_args = {
    'owner': 'udacity',
    'start_date': datetime(2021, 6, 18),
    'depends_on_past': False,
    'email': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}


#. Dag to create the pipepline to run in apache airflow
dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          catchup=False
        )


#. Start task to mark the beginning of pipeline
start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)


#. Task to create tables in redshift
create_tables_redshift = CreateTableOperator(
    task_id = 'create_tables_in_redshift',
    redshift_conn_id = 'redshift',
    dag = dag
)


#. Task to create staging_events staging table in redshift
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_events",
    s3_bucket="sparkifyschema",
    s3_key="log_data",
    file_format="s3://sparkifyschema/log_json_path.json",
    provide_context=True
)


#. Task to create staging_songs staging table in redshift
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_songs",
    s3_bucket="sparkifyschema",
    s3_key="song_data",
    file_format="auto",
    provide_context=True
)


#. Task to load the data into songplays table
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songplays",
    sql=SqlQueries.songplay_table_insert
)


#. Task to load the data into users dimension table
load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="users",
    sql=SqlQueries.user_table_insert
)


#. Task to load the data into songs dimension table
load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songs",
    sql=SqlQueries.song_table_insert
)


#. Task to load the data into artists dimension table
load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="artists",
    sql=SqlQueries.artist_table_insert
)


#. Task to load the data into time dimension table
load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="time",
    sql=SqlQueries.time_table_insert
)


#. Task to run the quality checks on tables 
#. in star schema after pipeline completion
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    table=["songplays", "users", "songs", "artists", "time"],
)


#. End Task to mark the end of pipeline
end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


#. Workflow for the Pipeline
start_operator >> create_tables_redshift
create_tables_redshift >> [stage_events_to_redshift, stage_songs_to_redshift]
[stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table]
[load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks
run_quality_checks >> end_operator