from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

import logging

class DataQualityOperator(BaseOperator):
    """The operator to create is the data quality operator, which is used to run checks on the data itself."""
    
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define operators params (with defaults)
                 redshift_conn_id="",
                 tables=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params
        self.redshift_conn_id = redshift_conn_id
        self.table = tables

    def execute(self, context):
        logging.info('Start running data quality check')
        redshift_hook = PostgresHook(self.redshift_conn_id)
        for i in len(tables):
            # check if each table has records imported
            records = redshift_hoook.get_records(f"SELECT COUNT(*) FROM {self.table[i]}")
            if len(records)<1 or len(records[0])<1:
                raise ValueError(f"Data quality check failed. {self.table[i]} returned no results")
            # check if the row counts of table is correct
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f"Data quality check failed. {self.table[i]} contained 0 rows")
            logging.info(f"Data quality on table {self.table[i]} check passed with {records[0][0]} records")
        
