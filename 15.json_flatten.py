""" Flatten JSON data """
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    explode,
    explode_outer
)

def read_json_file(spark, filepath):
    """ Read JSON data"""
    return (
        spark.
        read.
        format("json").
        option("inferschema", "true").
        option("multiline", "true").
        load(filepath)
    )

if __name__ == "__main__":
    spark = (
        SparkSession.
        builder.
        appName("read-json-file").
        getOrCreate()
    )

    FILEPATH = "./data/resturant_json_data.json"

    json_df = read_json_file(spark=spark, filepath = FILEPATH)

    exploded_json_df = json_df.select("*", explode(col("restaurants")).alias("exploded_restaurant")).drop(col("restaurants"))
    exploded_json_df.printSchema()

    exploded_json_df.select(
        col("*"),
        col("exploded_restaurant.restaurant.R.res_id").alias("rest_id"),
        col("exploded_restaurant.restaurant.name").alias("rest_name"),
        explode_outer(col("exploded_restaurant.restaurant.establishment_types")).alias("new_establishment_types")
    ).drop(col("exploded_restaurant")).show(20)

    input("Press ctrl + c to exit the process: ...")
