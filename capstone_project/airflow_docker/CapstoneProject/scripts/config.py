import os
import configparser

class ReadConfig:

    @staticmethod
    def _setpath():
        path = os.path.join(os.path.dirname(__file__),
                            '..', 'conf', 'config.cfg')
        return path


    @staticmethod
    def getconfig(param, val):
        path = ReadConfig._setpath()
        print(path)
        config = configparser.ConfigParser()
        config.read(path)
        return config.get(param, val)
