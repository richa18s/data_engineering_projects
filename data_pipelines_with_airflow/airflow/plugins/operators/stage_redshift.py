"""
    Author:
        Udacity, RichaS
    Date Created:
        06/18/2021
    Description:
         - Connects to AWS using creds and get the log_data and song_data from s3 bucket
         - Loads the data from s3 to staging tables in redshift star schema
"""

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    copy_sql =  """
                  COPY {}
                  FROM '{}'
                  ACCESS_KEY_ID '{}'
                  SECRET_ACCESS_KEY '{}'
                  FORMAT AS JSON '{}';
       			"""
    delete_sql = "TRUNCATE TABLE {};"
   
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 file_format="",
                 append=True,
                 *args, **kwargs):
        """ Constructor for the class object, also calls the constructor 
            of base class to set the necessary parameters"""

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.file_format = file_format
        self.append = append

    def execute(self, context):
        """ Connects to S3 bucked using creds from AWS hook and 
            populates staging_songs and staging_songs table in redshift
        """
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        s3_path = "s3://{}/{}".format(self.s3_bucket, self.s3_key)
        
        self.log.info("Copying data from S3 to {}".format(self.table))
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
                self.table,
                s3_path,
                credentials.access_key,
                credentials.secret_key,
                self.file_format)
               
        if not self.append:
            self.log.info("Append false, performing truncate and \
                           re-load on {}".format(self.table))
            delete = StageToRedshiftOperator.delete_sql.format(self.table)
            redshift.run(delete)
        
        redshift.run(formatted_sql)
        self.log.info("Done Loading..")
