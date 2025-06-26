
from pyspark.sql import SparkSession

def create_session():
    """ Create and Return Spark Session object"""
    spark = (
        SparkSession.
        builder.
        master("local").
        appName('JobStageTaskExample').
        getOrCreate()
    )
    return spark
