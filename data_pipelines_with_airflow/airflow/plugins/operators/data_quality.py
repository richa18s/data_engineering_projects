"""
    Author:
        Udacity, RichaS
    Date Created:
        06/18/2021
    Description:
         - Runs data quality check and raise error if no 
           rows are populated in tables after pipeline completion
"""

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'
    
    data_check_sql = "SELECT COUNT(*) FROM {}"

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 *args, **kwargs):
        """ Constructor for the class object, also calls the constructor 
            of base class to set the necessary parameters"""

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table= table

    def execute(self, context):
        """
            Connects to postgres database and runs the quality checks
        """
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        for idx in self.table:
            records = redshift_hook.get_records(DataQualityOperator.data_check_sql.format(idx))


            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError("""Data quality check failed. 
                                    {} returned no results""".format(idx))


            num_records = records[0][0]
            if num_records < 1:
                raise ValueError("""Data quality check failed. 
                                    {} contained 0 rows""".format(idx))
            self.log.info("""Data quality on table "{}" check passed 
                            with {} records""".format(idx, num_records))                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     