from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries
from airflow.operators.postgres_operator import PostgresOperator

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

# Add default parameters
default_args = {
    'owner': 'udacity',
    'start_date': datetime(2021, 4, 1),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

# Get the airflow variables
s3_bucket = Variable.get('s3_bucket')
s3_song_data = Variable.get('s3_song_data')
s3_log_data = Variable.get('s3_log_data')
s3_log_data_format = Variable.get('s3_log_data_format')

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

# Create all tables
create_tables = PostgresOperator(
    task_id="create_tables",
    dag=dag,
    postgres_conn_id="redshift",
    sql='create_tables.sql'
)

# Load data from s3 to redshift
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table="staging_events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket=s3_bucket,
    s3_key=s3_log_data,
    data_format="JSON 's3://{}/{}'".format(s3_bucket, s3_log_data_format)
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table="staging_songs",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket=s3_bucket,
    s3_key=s3_song_data,
    data_format="JSON 'auto'"
)

# Load fact tables
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table='songplays',
    sql_query=SqlQueries.songplay_table_insert
)

# Load dimension tables
load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table='songplays',
    sql_query=SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table='songplays',
    sql_query=SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table='songplays',
    sql_query=SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table='songplays',
    sql_query=SqlQueries.time_table_insert
)

# Execute data quality check
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    table=["songplays", "artists", "songs", "users", "time"]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# Configure the task dependencies
start_operator>>create_tables

create_tables>>stage_events_to_redshift
create_tables>>stage_songs_to_redshift

stage_events_to_redshift>>load_songplays_table
stage_songs_to_redshift>>load_songplays_table

load_songplays_table>>load_song_dimension_table
load_songplays_table>>load_user_dimension_table
load_songplays_table>>load_artist_dimension_table
load_songplays_table>>load_time_dimension_table

load_song_dimension_table>>run_quality_checks
load_user_dimension_table>>run_quality_checks
load_artist_dimension_table>>run_quality_checks
load_time_dimension_table>>run_quality_checks

run_quality_checks>>end_operator












