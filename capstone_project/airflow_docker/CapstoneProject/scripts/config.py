import os
import configparser

class ReadConfig:

    @staticmethod
    def _setpath():
        """
        Set the relative path of config file
        :return: complete path
        """
        path = os.path.join(os.path.dirname(__file__),
                            '..', 'conf', 'config.cfg')
        return path


    @staticmethod
    def getconfig(param, val):
        """
        Get the path and read config file for section
        and return variable's value
        :param param: section to be read
        :param val: variable to be read
        :return: Value of variable
        """
        path = ReadConfig._setpath()
        config = configparser.ConfigParser()
        config.read(path)
        return config.get(param, val)
    
    
