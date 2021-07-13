import pyspark.sql.functions as F
from scripts import load_schema as l
from scripts import spark as s


class DataValidation:

    def __init__(self, spark, logger):
        """
        Initiate the dims and fact tables and logger instance
        :param spark:
        :param logger:
        """
        self.spark = spark
        self.schema = l.ReadSchema(self.spark)
        self.logger = logger
        self.visa_dim = self.schema.get_visa_dim()
        self.mode_dim = self.schema.get_mode_dim()
        self.country_dim = self.schema.get_country_dim()
        self.ports_dim = self.schema.get_ports_dim()
        self.airports_dim = self.schema.get_airports_dim()
        self.states_dim = self.schema.get_states_dim()
        self.fact_df = self.schema.get_immigration_fact()
        self.non_imm_dim = self.schema.get_non_imm_dim()


    def _integrity_check(self, fact_df, dim_df, fact_col, dim_col):
        """
        Generic method compare the keys between fact and dimension
        to ensure all the FK values in fact are also present in respective
        dim tables
        :param fact_df: fact table
        :param dim_df: dim table
        :param fact_col: fact column to be checked
        :param dim_col: dim column to be checked
        :return: result of check
        """
        res = fact_df.select(fact_col).subtract(dim_df.select(dim_col)).count()
        return res


    def _check_rows(self, df):
        """
        Check the number of records in a data frame
        :param df: data frame / table to be checked
        :return: boolean (True/False)
        """
        return df.count() != 0


    def check_data_integrity(self):
        """
        This method invokes _integrity_check for each dimension table
        :return: None
        """
        v_rows = self._integrity_check(self.fact_df, self.visa_dim, self.fact_df.visa_code, self.visa_dim.code)
        if v_rows  != 0:
            self.logger.error("""Data quality check failed for {} and {}. 
                                 Validation returned {}, not the expected result {}
                                  """.format(self.fact_df, self.visa_dim, v_rows, 0))
        else:
            self.logger.info("Data Validation Completed : visa_dim")


        m_rows = self._integrity_check(self.fact_df, self.mode_dim, self.fact_df.arr_mode_code, self.mode_dim.code)
        if v_rows  != 0:
            self.logger.error("""Data quality check failed for {} and {}. 
                                 Validation returned {}, not the expected result {}
                                  """.format(self.fact_df, self.mode_dim, m_rows, 0))
        else:
            self.logger.info("Data Validation Completed : mode_dim")


        c_rows = self._integrity_check(self.fact_df, self.country_dim, self.fact_df.country_of_origin, self.country_dim.code)
        if c_rows  != 0:
            self.logger.error("""Data quality check failed for {} and {}. 
                                 Validation returned {}, not the expected result {}
                                  """.format(self.fact_df, self.country_dim, c_rows, 0))
        else:
            self.logger.info("Data Validation Completed : country_dim")


        all_ports = (self.ports_dim.select(F.col("code"))
                         .unionAll(self.airports_dim.select(F.col("local_code").alias("code"))))
        p_rows = self._integrity_check(self.fact_df, all_ports, self.fact_df.arr_port, all_ports.code)
        if p_rows != 0:
            self.logger.error("""Data quality check failed for {} and {}. 
                                     Validation returned {}, not the expected result {}
                                      """.format(self.fact_df, all_ports, p_rows, 0))
        else:
            self.logger.info("Data Validation Completed : ports")


        s_rows = self._integrity_check(self.fact_df, self.states_dim, self.fact_df.arr_state_code, self.states_dim.state_code)
        if s_rows != 0:
            self.logger.error("""Data quality check failed for {} and {}. 
                                         Validation returned {}, not the expected result {}
                                          """.format(self.fact_df, self.states_dim, s_rows, 0))
        else:
            self.logger.info("Data Validation Completed : states")



    def check_nulls(self):
        """
        This method checks the nulls in fact and dimension tables, necessary to impose PK constraint.
        :return: None
        """

        if self.visa_dim.filter(self.visa_dim.code.isNull()).count() != 0:
            self.logger.error("""Validation Error: Data quality check failed for {} on {}. 
                                 validation returned null rows
                                 """.format(self.visa_dim, "code"))
        else:
            self.logger.info("Not Null Validation Passed : visa_dim")


        if self.mode_dim.filter(self.mode_dim.code.isNull()).count() != 0:
            self.logger.error("""Validation Error: Data quality check failed for {} on {}. 
                                 validation returned null rows
                                 """.format(self.mode_dim, "code"))
        else:
            self.logger.info("Not Null Validation Passed : mode_dim")



        if self.country_dim.filter(self.country_dim.code.isNull()).count() != 0:
            self.logger.error("""Validation Error: Validation Failed: Data quality check failed for {} on {}. 
                                 validation returned null rows
                                 """.format(self.country_dim, "code"))
        else:
            self.logger.info("Not Null Validation Passed : country_dim")


        if self.ports_dim.filter(self.ports_dim.code.isNull()).count() != 0:
            self.logger.error("""Validation Error: Data quality check failed for {} on {}. 
                                 validation returned null rows
                                 """.format(self.ports_dim, "code"))
        else:
            self.logger.info("Not Null Validation Passed : ports_dim")


        if self.airports_dim.filter(self.airports_dim.ident.isNull()).count() != 0:
            self.logger.error("""Validation Error: Data quality check failed for {} on {}. 
                                 validation returned null rows
                                 """.format(self.airports_dim, "code"))

        else:
            self.logger.info("Not Null Validation Passed : airports_dim")


        if self.states_dim.filter(self.states_dim.state_code.isNull()).count() != 0:
            self.logger.error("""Validation Error: Data quality check failed for {} on {}. 
                                 validation returned null rows
                                 """.format(self.states_dim, "state_code"))
        else:
            self.logger.info("Not Null Validation Passed : states_dim")


        if self.non_imm_dim.filter(self.non_imm_dim.cicid.isNull()).count() != 0:
            self.logger.error("""Validation Error: Data quality check failed for {} on {}.
                                 validation returned null rows
                                 """.format(self.airports_dim, "code"))
        else:
            self.logger.info("Not Null Validation Passed : non_imm_dim")


        if self.fact_df.filter(self.fact_df.adm_num.isNull()).count() != 0:
            self.logger.error("""Validation Error: Data quality check failed for {} on {}. 
                                 validation returned null rows
                                 """.format(self.fact_df, "adm_num"))
        else:
            self.logger.info("Not Null Validation Passed : adm_num")



    def check_rows(self):
        """
        This method checks if there are any rows in dimension tables.
        :return: None
        """

        if not self._check_rows(self.visa_dim):
            self.logger.error("Validation Error: Data Validation Failed for visa dim")
        else:
            self.logger.info("Check Rows Validation Passed : visa_dim")


        if not self._check_rows(self.mode_dim):
            self.logger.error("Validation Error: Data Validation Failed for mode dim")
        else:
            self.logger.info("Rows Count Validation Passed : mode_dim")


        if not self._check_rows(self.country_dim):
            self.logger.error("Validation Error: Data Validation Failed for country dim")
        else:
            self.logger.info("Rows Count Validation Passed : country_dim")


        if not self._check_rows(self.ports_dim):
            self.logger.error("Validation Error: Data Validation Failed for ports dim")
        else:
            self.logger.info("Rows Count Validation Passed : ports_dim")


        if not self._check_rows(self.airports_dim):
            self.logger.error("Validation Error: Data Validation Failed for airports dim")
        else:
            self.logger.info("Rows Count Validation Passed : airports_dim")


        if not self._check_rows(self.states_dim):
            self.logger.error("Validation Error: Data Validation Failed for states dim")
        else:
            self.logger.info("Rows Count Validation Passed : states_dim")


        if not self._check_rows(self.non_imm_dim):
            self.logger.error("Validation Error: Data Validation Failed for non_imm_dim dim")
        else:
            self.logger.info("Rows Count Validation Passed : non_imm_dim")



def main():
    """
    Driving method to invoke check_data_integrity, check_nulls, check_rows
    along with spark session and logger
    :return: None
    """
    spark, logger = s.CreateSession.create_spark_session('Validate Data')
    validate = DataValidation(spark, logger)
    validate.check_data_integrity()
    validate.check_nulls()
    validate.check_rows()

    
if __name__ == '__main__':
    main()