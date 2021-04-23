from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """
    The operator is to load the fact table by taking a SQL statement as input and target 
    database on which to run the query against
    """
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults)
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 sql_query="",
                 truncate_insert=True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id=redshift_conn_id,
        self.aws_credentials_id=aws_credentials_id,
        self.table=table,
        self.sql_query=sql_query,
        self.truncate_insert = truncate_insert

    def execute(self, context):
        self.log.info('Start loading dimension table')
        
        # get the connection
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        # start loading data
        self.log.info("Inserting the fact table")
        formatted_sql = 'INSERT INTO {} ({})'.format(self.table, self.sql_query)
        
        if self.truncate_insert:
            truncate_sql_query='DELETE FROM {}'.format(self.table)
            # Truncate table
            self.log.info("Clearing data from destination {} table".format(self.table))
            redshift.run(truncate_sql_query)
            
        redshift.run(formatted_sql)        
        
        
        
        
        
        
        
        
        
        
        
        
        
