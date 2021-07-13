from scripts import config as c


class ExtractData:

    def __init__(self, spark):
        """
        Class constructor
        :param spark: spark session
        """
        self.spark = spark


    def _read_csv_file(self, path, delimiter=',', header=True):
        """
        Generic method to read CVS files
        :param path: s3 path where file resides
        :param delimiter: Delimiter used to separate columns in file
        :param header: if header exists or not (True/False)
        :return: intermediate data frame
        """
        print(path)
        return self.spark.read.option("delimiter", delimiter).csv(path, header=header)
    

    def get_immigration_info(self):
        """
        Method to read immigration data file from s3
        :return: intermediate fact data frame
        """
        return self.spark.read.load("{}{}".format(c.ReadConfig.getconfig('S3','input_data'),
                                                  c.ReadConfig.getconfig('SOURCE','immigration_data')), header=True)


    def get_visa_info(self):
        """
        Method to read visa csv file from s3
        :return: intermediate visa data frame
        """
        return self._read_csv_file("{}{}".format(c.ReadConfig.getconfig('S3','input_data'),
                                                 c.ReadConfig.getconfig('SOURCE','visa_data')),
                                   ',', True)


    def get_mode_info(self):
        """
        Method to read mode csv file from s3
        :return: intermediate mode data frame
        """
        return self._read_csv_file("{}{}".format(c.ReadConfig.getconfig('S3','input_data'),
                                                 c.ReadConfig.getconfig('SOURCE','mode_data')),
                                   ',', True)


    def get_country_info(self):
        """
        Method to read country csv file from s3
        :return: intermediate country data frame
        """
        return self._read_csv_file("{}{}".format(c.ReadConfig.getconfig('S3','input_data'),
                                                 c.ReadConfig.getconfig('SOURCE','country_data')),
                                   ',', True)


    def get_states_info(self):
        """
        Method to read states csv file from s3
        :return: intermediate states data frame
        """
        return self._read_csv_file("{}{}".format(c.ReadConfig.getconfig('S3','input_data'),
                                                 c.ReadConfig.getconfig('SOURCE','states_data')),
                                   ',', True)


    def get_ports_info(self):
        """
        Method to read ports csv file from s3
        :return: intermediate ports data frame
        """
        return self._read_csv_file("{}{}".format(c.ReadConfig.getconfig('S3','input_data'),
                                                 c.ReadConfig.getconfig('SOURCE','ports_data')),
                                   ';', True)


    def get_airport_info(self):
        """
        Method to read airports csv file from s3
        :return: intermediate airports data frame
        """
        return self._read_csv_file("{}{}".format(c.ReadConfig.getconfig('S3','input_data'),
                                                 c.ReadConfig.getconfig('SOURCE','airport_data')),
                                   ',', True)


    def get_city_info(self):
        """
        Method to read city demographics csv file from s3
        :return: intermediate city demographic data frame
        """
        return self._read_csv_file("{}{}".format(c.ReadConfig.getconfig('S3','input_data'),
                                                 c.ReadConfig.getconfig('SOURCE','city_data')),
                                   ';', True)

