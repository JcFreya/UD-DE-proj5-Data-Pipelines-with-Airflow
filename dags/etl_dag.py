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

# dag = DAG('udac_example_dag',
#           default_args=default_args,
#           description='Load and transform data in Redshift with Airflow',
#           schedule_interval='0 * * * *'
#         )

# Get the airflow variables
s3_bucket = Variable.get('s3_bucket')
s3_song_data = Variable.get('s3_song_data')
s3_log_data = Variable.get('s3_log_data')
s3_log_data_format = Variable.get('s3_log_data_format')

with DAG(dag_id='udac_example_dag', default_args=default_args, description='Load and transform data in Redshift with Airflow', schedule_interval='0 0 * * *') as dag:

    start_operator = DummyOperator(task_id='Begin_execution')

    # Create all tables
    create_tables = PostgresOperator(
        task_id="create_tables",
        postgres_conn_id="redshift",
        sql='create_tables.sql'
    )

    # Load data from s3 to redshift
    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        table="staging_events",
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        s3_bucket=s3_bucket,
        s3_key=s3_log_data,
        data_format="JSON 's3://{}/{}'".format(s3_bucket, s3_log_data_format)
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
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
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table='songplays',
        sql_query=SqlQueries.songplay_table_insert
    )

    # Load dimension tables
    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table='songplays',
        sql_query=SqlQueries.user_table_insert
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table='songplays',
        sql_query=SqlQueries.song_table_insert
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table='songplays',
        sql_query=SqlQueries.artist_table_insert
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table='songplays',
        sql_query=SqlQueries.time_table_insert
    )
    
    # set data quality checks queries
    dq_checks=[
        {'check_sql': "SELECT COUNT(*) FROM users WHERE userid is null", 'expected_result': 0},
        {'check_sql': "SELECT COUNT(*) FROM songs WHERE songid is null", 'expected_result': 0},
        {'check_sql': "SELECT COUNT(*) FROM songplays WHERE playid is null", 'expected_result': 0},
        {'check_sql': "SELECT COUNT(*) FROM artists WHERE artistid is null", 'expected_result': 0},
        {'check_sql': "SELECT COUNT(*) FROM time WHERE start_time is null", 'expected_result': 0}
    ]
    
    # Execute data quality check
    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id="redshift",
        tables=["songplays", "artists", "songs", "users", "time"],
        dq_checks=dq_checks
    )

    end_operator = DummyOperator(task_id='Stop_execution')

    # Configure the task dependencies
    start_operator>>create_tables

    create_tables>>[stage_events_to_redshift, stage_songs_to_redshift]

    [stage_events_to_redshift, stage_songs_to_redshift]>>load_songplays_table

    load_songplays_table>>[load_song_dimension_table, load_user_dimension_table, load_artist_dimension_table, load_time_dimension_table]

    [load_song_dimension_table, load_user_dimension_table, load_artist_dimension_table, load_time_dimension_table]>>run_quality_checks

    run_quality_checks>>end_operator












