from scripts import config as c
from pyspark.sql import SparkSession


class CreateSession:

    @staticmethod
    def create_spark_session(app_name):

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

        return spark_session