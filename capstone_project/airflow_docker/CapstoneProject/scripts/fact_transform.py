from pyspark.sql.types import IntegerType, StringType, LongType
import pyspark.sql.functions as F


class CleanAndTransformFact:

    @staticmethod
    def fact_immigration(immigration_data_df,
                         ports_dim,
                         airports_dim,
                         mode_dim,
                         country_dim,
                         visa_dim,
                         states_dim):
        """

        Performs following transformation on immigration data :
        - Transform arrival and departure date to mm/dd/yyyy format
        - Remove leading '0' from flight no
        - Rename the columns to more readable names
        - Assign UNKNOWN to nulls values if the value is not known
        - Where ever the mode of travel is not 'Air' assign 'Not Applicable' to airline Column
        - Cast col adm_num to long type and cicid, country_of_origin, arr_mode_code, visa_code to Integer
        - Get the distinct ports from ports dim and local code from airport dim and create an intermediate data frame
          that has union of list of codes from both airport and port dims
        - semi left join following dims to ensure the immigration fact table has valid data (foreign key dependency)
        - mode_dim on mode_code
        - country_dim on country code
        - visa_dim on visa code
        - intermediate ports dim (airports_dim local codes + port from ports_dim) on port code
        - states_dim on state_code

        :param immigration_data_df: intermediate immigration data frame
        :param ports_dim: ports dimension table
        :param airports_dim: airports dimension table
        :param mode_dim: mode dimension table
        :param country_dim: country dimension table
        :param visa_dim: visa dimension table
        :param states_dim: states dimension table
        :return: immigration fact table
        """

        remove_padding = F.udf(lambda x: x.lstrip('0') if x else '0', StringType())

        df_imm_stg = (
            immigration_data_df.select(immigration_data_df.columns)
                .withColumn("arr_date", F.col("arrdate").cast(IntegerType()))
                .withColumn("dep_date", F.col("depdate").cast(IntegerType()))
                .withColumn("sas_date", F.to_date(F.lit("01/01/1960"), "MM/dd/yyyy"))
                .withColumn("arr_date", F.expr("date_add(sas_date, arr_date)"))
                .withColumn("dep_date", F.expr("date_add(sas_date, dep_date)"))
                .withColumn("flt_no", remove_padding("fltno"))
        )


        df_imm_stg = (
            df_imm_stg
                .select(
                    F.col("admnum").cast(LongType()).alias("adm_num"),
                    F.col("cicid").cast(IntegerType()).alias("cic_id"),
                    F.col("i94cit").cast(IntegerType()).alias("country_of_origin"),
                    F.col("i94port").alias("arr_port"),
                    F.col("i94mode").cast(IntegerType()).alias("arr_mode_code"),
                    F.when(F.col("i94addr").isNull(), 'UNK').otherwise(F.col("i94addr")
                                       ).alias("arr_state_code"),
                    F.col("arr_date"),
                    F.when(F.col("dep_date").isNull(), F.to_date(F.lit("12/31/9999"), "MM/dd/yyyy")
                           ).otherwise(F.col("dep_date")).alias("dep_date"),
                    F.col("i94visa").cast(IntegerType()).alias("visa_code"),
                    F.col("visatype"),
                    F.when(F.col("visapost").isNull(), 'UNK').otherwise(F.col("visapost")).alias("visa_issuing_state"),
                    F.when((F.col("airline").isNull() & (F.col("i94mode").cast(IntegerType()) != 1)),
                         'Not Applicable').otherwise(F.col("airline")).alias("airline"),
                    F.col("flt_no"),
                    F.to_date(F.lit(F.col("dtadfile")), 'yyyymmdd').alias("date_added"),
                    F.year("arr_date").alias('year'),
                    F.month("arr_date").alias('month')
            ).distinct()
        )


        df_ports_stg = ports_dim.select("code")
        df_airports_stg = airports_dim.select("local_code")
        df_ports_validation = df_ports_stg.select('code').union(df_airports_stg.select('local_code')).distinct()
        df_ports_validation.count()

        immigration_fact = (
            df_imm_stg
                .join(mode_dim, df_imm_stg.arr_mode_code == mode_dim.code, how="leftsemi")
                .join(country_dim, df_imm_stg.country_of_origin == country_dim.code, how="leftsemi")
                .join(df_ports_validation, df_ports_validation.code == df_imm_stg.arr_port, how="leftsemi")
                .join(visa_dim, visa_dim.code == df_imm_stg.visa_code, how="leftsemi")
                .join(states_dim, states_dim.state_code == df_imm_stg.arr_state_code, how="leftsemi")
                .select(df_imm_stg.columns)
        )

        # immigration_fact.count()

        immigration_fact.show()
        return immigration_fact