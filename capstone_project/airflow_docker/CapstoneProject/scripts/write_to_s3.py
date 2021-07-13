from scripts import config as c


class CreateSchema:

    def __init__(self):
        """
        Constructor of class
        """
        print("instance created!")


    def load_visa_dim(self, visa_dim):
        """
        Method to write visa dimension table to s3 in parquet format.
        :param visa_dim: visa dim data frame
        :return: None
        """
        visa_dim.write.parquet("{}visa_dim.parquet".format(c.ReadConfig.getconfig('S3','output_data')))


    def load_mode_dim(self, mode_dim):
        """
        Method to write mode dimension table to s3 in parquet format.
        :param mode_dim: mode dim data frame
        :return: None
        """
        mode_dim.write.parquet("{}mode_dim.parquet".format(c.ReadConfig.getconfig('S3','output_data')))


    def load_country_dim(self, country_dim):
        """
        Method to write country dimension table to s3 in parquet format.
        :param country_dim: country dim data frame
        :return: None
        """
        country_dim.write.parquet("{}country_dim.parquet".format(c.ReadConfig.getconfig('S3','output_data')))


    def load_ports_dim(self, ports_dim):
        """
        Method to write ports dimension table to s3 in parquet format.
        :param ports_dim: ports dim data frame
        :return: None
        """
        ports_dim.write.parquet("{}ports_dim.parquet".format(c.ReadConfig.getconfig('S3','output_data')))


    def load_airports_dim(self, airports_dim):
        """
        Method to write airports dimension table to s3 in parquet format.
        :param airports_dim: airports dim data frame
        :return: None
        """
        airports_dim.write.parquet("{}airports_dim.parquet".format(c.ReadConfig.getconfig('S3','output_data')))


    def load_states_dim(self, states_dim):
        """
        Method to write states dimension table to s3 in parquet format.
        :param states_dim: states dim data frame
        :return: None
        """
        states_dim.write.parquet("{}states_dim.parquet".format(c.ReadConfig.getconfig('S3','output_data')))


    def load_date_dim(self, date_dim):
        """
        Method to write date dimension table to s3 in parquet format.
        :param date_dim: date dim data frame
        :return: None
        """
        date_dim.write.partitionBy("year", "month")\
            .parquet("{}date_dim.parquet".format(c.ReadConfig.getconfig('S3','output_data')))


    def load_non_imm_dim(self, non_imm_dim):
        """
        Method to write non_imm dimension table to s3 in parquet format.
        :param non_imm_dim: non immigrant data frame
        :return: None
        """
        non_imm_dim.write.parquet("{}non_imm_dim.parquet".format(c.ReadConfig.getconfig('S3','output_data')))


    def load_immigration_fact(self, immigration_fact):
        """
        Method to write fact data dimension table to s3 in parquet format which is partitioned by year and month
        :param immigration_fact: fact data frame
        :return: None
        """
        immigration_fact.write.partitionBy("year", "month")\
            .parquet("{}immigration_fact.parquet".format(c.ReadConfig.getconfig('S3','output_data')))