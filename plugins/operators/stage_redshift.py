from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    """"The stage operator is to load any JSON formatted files from S3 to Amazon Redshift."""
    
    ui_color = '#358140'
    
    # sql to copy the data from s3
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION 'us-west-2'
        TIMEFORMAT AS 'epochmillisecs'
        TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
        IGNOREHEADER {}
        DELIMITER '{}' 
        {}
    """
    
    
    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults)
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 delimiter=",",
                 ignore_headers=1,
                 data_format = "JSON 'auto'",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.delimiter = delimiter
        self.ignore_headers = ignore_headers
        self.aws_credentials_id = aws_credentials_id
        self.data_format = data_format

    def execute(self, context):
        self.log.info('Start staging tables loading')
        
        # get the connection
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        # delete table if exists before loading
        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))
        
        # start loading data
        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.ignore_headers,
            self.delimiter,
            self.data_format
        )
        redshift.run(formatted_sql)        




