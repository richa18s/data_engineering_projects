import configparser
import os
# from  config import ReadConfig
from scripts import config as c


class CreateSchema:

    def __init__(self):
        print("instance created!")


    def load_visa_dim(self, visa_dim):
        """
        :param visa_dim:
        :return:
        """
        visa_dim.write.parquet("{}visa_dim.parquet".format(c.ReadConfig.getconfig('S3','output_data')))


    def load_mode_dim(self, mode_dim):
        """
        :param mode_dim:
        :return:
        """
        mode_dim.write.parquet("{}mode_dim.parquet".format(c.ReadConfig.getconfig('S3','output_data')))


    def load_country_dim(self, country_dim):
        """
        :param country_dim:
        :return:
        """
        country_dim.write.parquet("{}country_dim.parquet".format(c.ReadConfig.getconfig('S3','output_data')))


    def load_ports_dim(self, ports_dim):
        """
        :param ports_dim:
        :return:
        """
        ports_dim.write.parquet("{}ports_dim.parquet".format(c.ReadConfig.getconfig('S3','output_data')))


    def load_airports_dim(self, airports_dim):
        """
        :param airports_dim:
        :return:
        """
        airports_dim.write.parquet("{}airports_dim.parquet".format(c.ReadConfig.getconfig('S3','output_data')))


    def load_states_dim(self, states_dim):
        """
        :param states_dim:
        :return:
        """
        states_dim.write.parquet("{}states_dim.parquet".format(c.ReadConfig.getconfig('S3','output_data')))


    def load_date_dim(self, date_dim):
        """
        :param date_dim:
        :return:
        """
        date_dim.write.partitionBy("year", "month")\
            .parquet("{}date_dim.parquet".format(c.ReadConfig.getconfig('S3','output_data')))


    def load_non_imm_dim(self, non_imm_dim):
        """
        :param imm_dim:
        :return:
        """
        non_imm_dim.write.parquet("{}non_imm_dim.parquet".format(c.ReadConfig.getconfig('S3','output_data')))


    def load_immigration_fact(self, immigration_fact):
        """
        :param immigration_fact:
        :return:
        """
        immigration_fact.write.partitionBy("year", "month")\
            .parquet("{}immigration_fact.parquet".format(c.ReadConfig.getconfig('S3','output_data')))