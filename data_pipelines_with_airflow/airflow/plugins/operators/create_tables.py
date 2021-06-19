"""
    Author:
        Udacity, RichaS
    Date Created:
        06/18/2021
    Description:
         - Runs the DDLS to craete tables in star schema.
"""

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class CreateTableOperator(BaseOperator):
    ui_color = '#358140'
    
    @apply_defaults
    def __init__(self, 
                 redshift_conn_id = ""
                 , *args, 
                 **kwargs):
        """ Constructor for the class object, also calls the constructor 
            of base class to set the necessary parameters"""
        super(CreateTableOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        
    def execute(self, context):
        """ Runs the commands in sql file and create tables in redshift"""
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        self.log.info('Creating tables in redshift')   
        with open('/home/workspace/airflow/create_tables.sql','r') as file:
            sql_statement = file.read()
        redshift.run(sql_statement)
        self.log.info("Done Creating..")
