from pyspark.sql.types import IntegerType, StringType, DoubleType
import pyspark.sql.functions as F
from pyspark.sql.functions import udf


class CleanAndTransformDim:

    @staticmethod
    def dim_visa(visa_df, immigration_data_df):
        """
        :param visa_df:
        :param immigration_data_df:
        :return:
        """
        visa_df_stg = (
            immigration_data_df
                .select(
                    F.col("i94visa").cast(IntegerType()).alias("code"),
                    F.col("visatype").alias("visa_type")
                ).distinct()
        )

        visa_dim = (
            visa_df
                .join(visa_df_stg, visa_df.code == visa_df_stg.code, how='left')
                .select(
                    visa_df.code.cast(IntegerType()).alias("code"),
                    visa_df_stg.visa_type.alias("type"),
                    visa_df.visa_desc.alias("desc")
                ).sort("code")
        )

        visa_dim.show()

        return visa_dim


    @staticmethod
    def dim_mode(mode_df):
        """
        :param mode_df:
        :return:
        """
        mode_dim = (
            mode_df
                .select(
                    F.col("code").cast(IntegerType()).alias("code"),
                    F.col("mode"))
        )

        mode_dim.show()

        return mode_dim


    @staticmethod
    def dim_country(country_df):
        """
        :param country_df:
        :return:
        """
        country_dim = (
            country_df
                .select(
                    F.col("code").cast(IntegerType()),
                    F.col("name")
            )
                .filter(~F.col("name").contains("No Country Code"))
                .filter(~F.col("name").contains("INVALID"))
        )
        country_dim.show()
        return country_dim


    @staticmethod
    def dim_ports(ports_df):
        """
        :param ports_df:
        :return:
        """
        ports_dim = (
            ports_df
                .filter(~F.col("ports").contains("No PORT Code"))
        )
        ports_dim.show()
        return ports_dim


    @staticmethod
    def dim_airports(airport_codes_df):
        """
        :param airport_codes_df:
        :return:
        """

        remove_padding = F.udf(lambda x: x.lstrip('0') if x else '0', StringType())

        airport_codes_stg = (
            airport_codes_df
                .select(airport_codes_df.columns)
                .withColumn("local_code", F.when(F.col("local_code").isNull(), 'UNKNOWN'
                                             ).otherwise(F.col("local_code")).alias("local_code"))
                .withColumn("local_code", remove_padding("local_code"))
                .withColumn("latitude", F.split("coordinates", ',')[0])
                .withColumn("longitude", F.split("coordinates", ',')[1])
        )


        airports_dim = (
            airport_codes_stg
                .select(
                    F.col("ident"), F.col("type"), F.col("name"),
                    F.col("elevation_ft"), F.col("iso_country"),
                    F.when(F.col("iso_region").isNull(), 'UNKNOWN'
                           ).otherwise(F.col("iso_region")).alias("iso_region"),
                    F.when(F.col("municipality").isNull(), 'UNKNOWN'
                           ).otherwise(F.col("municipality")).alias("municipality"),
                    F.when(F.col("gps_code").isNull(), 'UNKNOWN'
                           ).otherwise(F.col("gps_code")).alias("gps_code"),
                    F.col("iata_code"),
                    F.col("local_code"),
                    F.col("latitude").cast(DoubleType()),
                    F.col("longitude").cast(DoubleType())
                    ).distinct()
                .where(F.col("iata_code").isNotNull()  # & (col("iso_country") == "US")
            )
        )

        airports_dim.show()
        return airports_dim


    @staticmethod
    def dim_states(states_df, city_df):
        """
        :param states_df:
        :param city_df:
        :return:
        """
        city_df_stg = (
            city_df
                .select(
                    F.col("city"),
                    F.col("State Code").alias("state_code"),
                    F.col("Total Population").alias("total_city_pop")
            )
                .distinct()
        )

        states_df_stg = (
            states_df
                .select(
                    F.col("code"),
                    F.col("state")
            )
                .distinct()
        )

        states_dim = (
            states_df_stg
                .join(city_df_stg, states_df_stg.code == city_df_stg.state_code, how='left')
                .select(
                    states_df_stg.code.alias("state_code"),
                    states_df_stg.state.alias("state_name"),
                    city_df_stg.city,
                    city_df_stg.total_city_pop)
        )

        states_dim.show()
        return states_dim


    @staticmethod
    def dim_time(immigration_data_df):
        """
        :param immigration_data_df:
        :return:
        """
        time_df_a = (
            immigration_data_df
                .select(
                F.col("arrdate").alias("date"))
                .where(F.col("arrdate").isNotNull())
        )

        time_df_d = (
            immigration_data_df
                .select(
                    F.col("depdate").alias("date"))
                .where(F.col("depdate").isNotNull())
        )


        time_df_stg = time_df_a.union(time_df_d).distinct()
        time_df_stg = time_df_stg.select(F.col("date").cast(IntegerType()))
        time_df_stg = time_df_stg.withColumn("sas_date", F.to_date(F.lit("01/01/1960"), "MM/dd/yyyy"))
        time_df_stg = time_df_stg.withColumn("date", F.expr("date_add(sas_date, date)"))


        date_dim = (
            time_df_stg
                .select("date")
                .withColumn("year", F.year("date"))
                .withColumn("month", F.month("date"))
                .withColumn("day", F.dayofmonth("date"))
                .withColumn("datekey", F.date_format(F.col("date"), "yyyyMMdd"))
                .withColumn("quarter", F.quarter("date"))
                .withColumn("day_of_month", F.dayofmonth("date"))
                .withColumn("day_of_week", F.dayofweek("date"))
                .withColumn("week_of_year", F.weekofyear("date"))
                .sort("datekey")
        )

        date_dim.show()
        return date_dim


    @staticmethod
    def dim_non_imm(immigration_data_df):
        """
        :param immigration_data_df:
        :return:
        """
        non_imm_dim = (
            immigration_data_df
                .select(
                F.col("cicid").cast(IntegerType()),
                F.col("biryear").alias("birth_year").cast(IntegerType()),
                F.col("i94bir").cast(IntegerType()).alias("age_in_years"),
                F.col("gender"),
                F.when(F.col("occup").isNull(), 'UNK').otherwise(F.col("occup")).alias("occup"),
                F.col("i94res").alias("country_of_res").cast(IntegerType())
            )
        )

        non_imm_dim.show(5)

        return non_imm_dim