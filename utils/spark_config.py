from pyspark.sql import SparkSession


def get_spark_session(app_name='h1_app'):

    spark_session = SparkSession.builder.appName(app_name).getOrCreate()

    return spark_session

