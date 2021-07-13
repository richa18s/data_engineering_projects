from pyspark.sql.types import IntegerType, StringType, DoubleType
import pyspark.sql.functions as F


class CleanAndTransformDim:

    @staticmethod
    def dim_visa(visa_df, immigration_data_df):
        """
        This method does the following transformations:
        - change the data type of visa code to integer
        - assign proper alias to columns
        - join with immigration event data to also get the visa type
        - create a dim table / df for visa
        :param visa_df: intermediate visa data frame
        :param immigration_data_df: intermediate immigration data frame
        :return: visa dimension
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
        This method does following transformations:
        - assign correct data type to code
        - assign meaningful aliases to columns
        :param mode_df: intermediate mode data frame
        :return: mode dimension
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
        This method does following transformations:
        - Select only valid codes to create this data set eliminate the invalid code.
        - Type cast code to have integer data type
        :param country_df: intermediate country data frame
        :return: country dimension
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
        Method transform the intermediate ports data frame to select only valid codes to create ports dim
        :param ports_df: intermediate ports data frame
        :return: ports dimension
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
        This method does following transformations on intermediate airports df:
        - Select records that have proper IATA Code
        - Split coordinates to separate columns latitude and longitude
        - Typecast the columns where ever needed
        - Remove any duplicate records
        - Removing any leading '0' from local codes
        - Replace nulls with proper code : 'UNKNOWN'
        - Assign correct alias where necessary
        :param airport_codes_df: intermediate airport data frame
        :return: airports dimension
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
        This method does following transformations on intermediate state and city df:
        - Get the state data from states df and city data from city df and combine those to create one dimension that has state_code,
          state_name, list of cities in states, their respective population
        - Assign correct alias where necessary
        - Remove duplicates from states and city df before joining these two data sets
        :param states_df: intermediate states df
        :param city_df: intermediate city df
        :return: states dimension
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
        This method does following transformations on immigration data to get data for date dimension
        - This dimension would be useful when any analysis needs to be performed w.r.t specific date, month, year or quarter
        - There are other ways to create time dim but for this specific use case get all the arrival and departure dates from i94 immigrantion data
        - Get a union of both the data sets, eliminate duplicates
        - Extract year, month, day, date key, quarter, day_of_month, day_of_week, week_of_year from the date and assign to appropiate columns
        :param immigration_data_df: intermediate immigration data frame
        :return: date dimension
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
        This method derives non immigrant information from immigration records
        - This data is retrieved from I94 immigration event data
        - Data that is relevant to immigrant like cicid, birth year, gender, occup, country of residence that is not going to change frequently can be put in separate dimension for analysis on immigrants related data
        - Retrieve the above columns, typecast and assign aliases wherever necessary
        - Replace nulls with code : UNKNOWN or UNK
        :param immigration_data_df: intermediate immigration df
        :return: non immigrant dimension
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