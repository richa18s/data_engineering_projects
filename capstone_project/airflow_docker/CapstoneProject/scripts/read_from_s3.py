import os
import configparser
# from  config import ReadConfig

from scripts import config as c


class ExtractData:

    def __init__(self, spark):
        """
        :param spark:
        """
        self.spark = spark


    def _read_csv_file(self, path, delimiter=',', header=True):
        """
        :param path:
        :param delimiter:
        :param header:
        :return:
        """
        print(path)
        return self.spark.read.option("delimiter", delimiter).csv(path, header=header)
    

    def get_immigration_info(self):
        """
        :return:
        """
        return self.spark.read.load("{}{}".format(c.ReadConfig.getconfig('S3','input_data'),
                                                  c.ReadConfig.getconfig('SOURCE','immigration_data')), header=True)


    def get_visa_info(self):
        """
        :return:
        """
        return self._read_csv_file("{}{}".format(c.ReadConfig.getconfig('S3','input_data'),
                                                 c.ReadConfig.getconfig('SOURCE','visa_data')),
                                   ',', True)


    def get_mode_info(self):
        """
        :return:
        """
        return self._read_csv_file("{}{}".format(c.ReadConfig.getconfig('S3','input_data'),
                                                 c.ReadConfig.getconfig('SOURCE','mode_data')),
                                   ',', True)


    def get_country_info(self):
        """
        :return:
        """
        return self._read_csv_file("{}{}".format(c.ReadConfig.getconfig('S3','input_data'),
                                                 c.ReadConfig.getconfig('SOURCE','country_data')),
                                   ',', True)


    def get_states_info(self):
        """
        :return:
        """
        return self._read_csv_file("{}{}".format(c.ReadConfig.getconfig('S3','input_data'),
                                                 c.ReadConfig.getconfig('SOURCE','states_data')),
                                   ',', True)


    def get_ports_info(self):
        """
        :return:
        """
        return self._read_csv_file("{}{}".format(c.ReadConfig.getconfig('S3','input_data'),
                                                 c.ReadConfig.getconfig('SOURCE','ports_data')),
                                   ';', True)


    def get_airport_info(self):
        """
        :return:
        """
        return self._read_csv_file("{}{}".format(c.ReadConfig.getconfig('S3','input_data'),
                                                 c.ReadConfig.getconfig('SOURCE','airport_data')),
                                   ',', True)


    def get_city_info(self):
        """
        :return:
        """
        return self._read_csv_file("{}{}".format(c.ReadConfig.getconfig('S3','input_data'),
                                                 c.ReadConfig.getconfig('SOURCE','city_data')),
                                   ';', True)

