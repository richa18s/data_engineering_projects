from scripts import config as c

# import config as c



class ReadSchema:

    def __init__(self, spark):
        """
        Class constructor to set the spark session
        :param spark: spark session
        """
        self.spark=spark


    def get_visa_dim(self):
        """
        Method to get visa dimension table from s3 schema
        :return: visa dimension
        """
        return self.spark.read.parquet(
            "{}{}".format(c.ReadConfig
                          .getconfig('S3','output_data'),
                          c.ReadConfig.getconfig('SCHEMA','visa_dim')))

    def get_mode_dim(self):
        """
        Method to get mode dimension table from s3 schema
        :return: mode dimension
        """
        return self.spark.read.parquet(
            "{}{}".format(c.ReadConfig
                          .getconfig('S3','output_data'),
                          c.ReadConfig.getconfig('SCHEMA','mode_dim')))

    def get_country_dim(self):
        """
        Method to get country dimension table from s3 schema
        :return: country dimension
        """
        return  self.spark.read.parquet(
            "{}{}".format(c.ReadConfig
                          .getconfig('S3','output_data'),
                          c.ReadConfig.getconfig('SCHEMA','country_dim')))

    def get_ports_dim(self):
        """
        Method to get ports dimension table from s3 schema
        :return: ports dimension
        """
        return self.spark.read.parquet(
            "{}{}".format(c.ReadConfig
                          .getconfig('S3','output_data'),
                          c.ReadConfig.getconfig('SCHEMA','ports_dim')))

    def get_airports_dim(self):
        """
        Method to get airports dimension table from s3 schema
        :return: airports dimension
        """
        return self.spark.read.parquet(
            "{}{}".format(c.ReadConfig
                          .getconfig('S3','output_data'),
                          c.ReadConfig.getconfig('SCHEMA','airports_dim')))

    def get_states_dim(self):
        """
        Method to get states dimension table from s3 schema
        :return: states dimension
        """
        return self.spark.read.parquet(
            "{}{}".format(c.ReadConfig
                          .getconfig('S3','output_data'),
                          c.ReadConfig.getconfig('SCHEMA','states_dim')))

    def get_non_imm_dim(self):
        """
        Method to fetch non immigrant dimension table from s3 schema
        :return: non_imm dimension
        """
        return self.spark.read.parquet(
            "{}{}".format(c.ReadConfig
                          .getconfig('S3','output_data'),
                          c.ReadConfig.getconfig('SCHEMA','non_imm_dim')))

    def get_immigration_fact(self):
        """
        Method to fetch immigration record dimension table from s3 schema
        :return: immigration fact
        """
        return self.spark.read.parquet(
            "{}{}".format(c.ReadConfig
                          .getconfig('S3','output_data'),
                          c.ReadConfig.getconfig('SCHEMA','immigration_fact')))