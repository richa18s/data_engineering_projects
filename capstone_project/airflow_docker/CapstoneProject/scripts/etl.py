# import spark as s
# import read_from_s3 as e
# import dim_transform as d
# import fact_transform as f
# import write_to_s3 as w


from scripts import spark as s
from scripts import read_from_s3 as e
from scripts import dim_transform as d
from scripts import fact_transform as f
from scripts import write_to_s3 as w



def main():
    """
    :return:
    """

    # Create Spark session
    # spark = s.CreateSession.create_spark_session('Immigration')
    spark = s.CreateSession.create_spark_session('ProcessImmigrationRecords')


    # Create object of ExtractData to read data from S3
    # ext = e.ExtractData(spark)
    ext = e.ExtractData(spark)


    # Initialize CleanAndTransformDim to process the data
    # which will be uploaded in S3, this class creates dimension dfs
    # dim = d.CleanAndTransformDim()
    dim = d.CleanAndTransformDim()


    # Initialize CleanAndTransformFact to process the data
    # which will be uploaded in S3, this class creates fact data frame
    # fact = f.CleanAndTransformFact()
    fact = f.CleanAndTransformFact()


    # Initialize the class to write data in S3
    # in parquet format
    load = w.CreateSchema()


    """ MODE DIM """
    # Load mode of travel information into
    # a dimension table in s3
    mode_data = ext.get_mode_info()
    mode_dim = dim.dim_mode(mode_data)
    load.load_mode_dim(mode_dim)


    """ COUNTRY DIM """
    # Load list of country name along with
    # codes into dimension table in s3
    country_data = ext.get_country_info()
    country_dim = dim.dim_country(country_data)
    load.load_country_dim(country_dim)


    """ PORTS DIM """
    # Load list of ports along with their
    # code and load into ports dim in s3
    ports_data = ext.get_ports_info()
    ports_dim = dim.dim_ports(ports_data)
    load.load_ports_dim(ports_dim)


    """ AIRPORTS DIM """
    # Clean and load the airport data into
    # airports dimension table in s3
    airports_data = ext.get_airport_info()
    airports_dim = dim.dim_airports(airports_data)
    load.load_airports_dim(airports_dim)


    """ STATES DIM """
    # Combine the city data along with states
    # create a dimension with details of city along
    # with state codes store in dim table in s3
    states_data = ext.get_states_info()
    city_data = ext.get_city_info()
    states_dim = dim.dim_states(states_data, city_data)
    load.load_states_dim(states_dim)


    """ DATE DIM """
    # Creates time dimension from distinct arrival and
    # departure dates provided with immigration data
    # store the dim in s3
    imm_data = ext.get_immigration_info()
    date_dim = dim.dim_time(imm_data)
    load.load_date_dim(date_dim)


    """ VISA DIM """
    # Combine the visa code with the visa types provided
    # with immigration data and create visa dimension
    # and store in s3
    visa_data = ext.get_visa_info()
    visa_dim = dim.dim_visa(visa_data, imm_data)
    load.load_visa_dim(visa_dim)


    """ NON IMMIGRANT DIM """
    # Create a separate dim table for attributes related to non immigrant
    # that are not event related and can be put into a separate
    # dimension table (SCDs)
    non_imm_data = dim.dim_non_imm(imm_data)
    load.load_non_imm_dim(non_imm_data)


    """ IMMIGRATION FACT """
    # Extract, transform and load the immigration information
    # into fact table, data is validated against required dim tables
    # to filter out the invalid records, save fact table in s3
    imm_fact = fact.fact_immigration(imm_data, ports_dim, airports_dim,
                                     mode_dim, country_dim, visa_dim,
                                     states_dim)
    load.load_immigration_fact(imm_fact)



if __name__ == '__main__':
    main()
