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
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 dq_checks=list(),
                 *args, **kwargs):
        """ Constructor for the class object, also calls the constructor 
            of base class to set the necessary parameters"""

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.dq_checks = dq_checks

    def execute(self, context):
        """
            Connects to postgres database and runs the quality checks
        """
        redshift = PostgresHook(self.redshift_conn_id)
        
        for check in self.dq_checks:
            sql = check.get('check_sql')
            exp_result = check.get('expected_result')
            check_name = check.get('check_name')
            
            records = redshift.get_records(sql)[0]
            
            self.log.info("""Running Data quality check {} 
                             """.format(check_name))
            
            if records[0] != exp_result:
                    raise ValueError("""Data quality check failed. 
                                        Query returned {}, not the expected result {}
                                        """.format(records, exp_result))
            else:
                self.log.info("""Data quality check "{}" passed
                              """.format(check_name))                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     