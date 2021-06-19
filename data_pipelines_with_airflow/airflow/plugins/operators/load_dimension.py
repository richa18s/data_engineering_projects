"""
    Author:
        Udacity, RichaS
    Date Created:
        06/18/2021
    Description:
         - Makes connection to redshift to load dimension tables
"""

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'
    
    load_dim_table = """ 
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

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql

        
    def execute(self, context):
        """ Connects to redshift to load the dimension tables """
        self.log.info('Loading the data into {}'.format(self.table))
        redshift_hook = PostgresHook(self.redshift_conn_id)
        sql_statement = LoadDimensionOperator.load_dim_table.format(self.table, 
                                   self.sql)
        redshift_hook.run(sql_statement)
        self.log.info("Done Loading..")
