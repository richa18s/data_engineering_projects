from scripts import config as c
from pyspark.sql import SparkSession


class CreateSession:

    @staticmethod
    def create_spark_session(app_name):
        """
        Method to create SparkSession, an entry point to Spark to write DataFrame
        Add the spark jars to the Spark configuration to make them available for use
        set aws access id and secret access key
        :param app_name: name of the application
        :return: logger, spark session
        """

        spark_builder = (
            SparkSession
                .builder
                .appName(app_name))

        spark_builder.config(
            'spark.jars.packages', c.ReadConfig.getconfig('DEPENDENCIES','jars'))
        spark_session = spark_builder.getOrCreate()
        spark_session._jsc.hadoopConfiguration().set(
            'fs.s3a.impl', 'org.apache.hadoop.fs.s3native.NativeS3FileSystem')
        spark_session._jsc.hadoopConfiguration().set(
            'fs.s3a.awsAccessKeyId', c.ReadConfig.getconfig('AWS','access_key_id'))
        spark_session._jsc.hadoopConfiguration().set(
            'fs.s3a.awsSecretAccessKey', c.ReadConfig.getconfig('AWS','secret_access_key'))
        spark_session._jsc.hadoopConfiguration().set(
            "fs.s3.buffer.dir", c.ReadConfig.getconfig('DEPENDENCIES','buffer_dir'))

        log4jLogger = spark_session._jvm.org.apache.log4j
        logger = log4jLogger.LogManager.getLogger(__name__)
        logger.setLevel(log4jLogger.Level.INFO)

        return spark_session, logger
