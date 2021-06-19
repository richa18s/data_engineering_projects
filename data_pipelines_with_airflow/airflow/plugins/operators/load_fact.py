"""
    Author:
        Udacity, RichaS
    Date Created:
        06/18/2021
    Description:
         - Connects to redshift and Load the data from 
           staging to fact table.
"""

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):
    ui_color = '#F98866'
    
    load_songplays_table = """
            	   INSERT INTO {}
            	   {};
                   COMMIT;
                   """
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql="",
                 *args, **kwargs):
        """ Constructor for the class object, also calls the constructor 
            of base class to set the necessary parameters"""

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql

    def execute(self, context):
        """ Connects to redshift and load the data from staging events and songs to fact table """
        redshift_hook = PostgresHook(self.redshift_conn_id)
        self.log.info("Loading data into {} ".format(self.table))
        sql_statement = LoadFactOperator.load_songplays_table.format(self.table, self.sql)
        redshift_hook.run(sql_statement)
        self.log.info("Done Loading..")

        
        
        
        