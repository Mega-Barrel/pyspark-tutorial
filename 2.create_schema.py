
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType
)
from _spark_session import create_session

def read_csv_file(spark, file_path, my_schema):
    """ Read and show top 10 result"""
    df = (
        spark.
        read.
        format("csv").
        option("header", "false").
        option("skipRows", 1).
        option("multiLine", "false").
        option("inferschema", "false").
        schema(my_schema).
        option("mode", "PERMISSIVE").
        load(file_path)
    )
    return df

if __name__ == "__main__":
    spark = create_session()
    file_path = './data/flight_data_2015_summary.csv'
    schema = StructType(
        [
            StructField("DEST_COUNTRY_NAME", StringType(), True),
            StructField("ORIGIN_COUNTRY_NAME", StringType(), True),
            StructField("count", IntegerType(), True),
        ]
    )

    df = read_csv_file(spark=spark, file_path=file_path, my_schema=schema)
    df.show(10)
    input('Enter ctrl + c to terminate: ')
