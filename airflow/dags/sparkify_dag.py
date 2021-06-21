from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (
    StageToRedshiftOperator, 
    LoadFactOperator,
    LoadDimensionOperator, 
    DataQualityOperator
)
from helpers import SqlQueries

default_args = {
    'owner': 'hedcler',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2019, 1, 12),
    'email_on_retry': False,
    'catchup': False
}

dag = DAG('sparkify_dag',
          default_args=default_args,
          description='Sparkify ETL (from S3 to Redshift) using Apache Airflow',
          schedule_interval='@hourly'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table="staging_events",
    conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    json_path="s3://udacity-dend/log_json_path.json",
    file_type="json"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table="staging_songs",
    conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="song_data/A/A/A",
    json_path="auto",
    file_type="json"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    table='songplays',
    conn_id="redshift",
    load_sql_stmt=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    table='users',
    conn_id="redshift",
    load_sql_stmt=SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    table='songs',
    conn_id="redshift",
    load_sql_stmt=SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    table='artists',
    conn_id="redshift",
    load_sql_stmt=SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    table='time',
    conn_id="redshift",
    load_sql_stmt=SqlQueries.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    tables=['songplays', 'users', 'songs', 'artists', 'time'],
    conn_id="redshift"
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

#
# Task order
#
start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator