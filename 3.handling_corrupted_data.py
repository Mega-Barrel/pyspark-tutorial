
from _spark_session import create_session
from pyspark.sql.types import (
    StringType,
    IntegerType,
    StructField,
    StructType
)

def read_csv_file(spark, file_path, mode, emp_schema):
    """ Read DataFrame"""
    df = (
        spark.
        read.
        format("csv").
        option("header", "true").
        option("inferschema", "true").
        schema(emp_schema).
        option("badRecordsPath", "./data/bad_records").
        option("mode", mode).
        load(file_path)
    )
    return df

if __name__ == "__main__":
    spark = create_session()
    file_path = './data/employee_data.csv'

    emp_schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("salary", IntegerType(), True),
            StructField("address", StringType(), True),
            StructField("nominee", StringType(), True),
            StructField("_corrupt_record", StringType(), True)
        ]
    )

    df_permissive = read_csv_file(
        spark=spark,
        file_path=file_path,
        mode="PERMISSIVE",
        emp_schema=emp_schema
    )
    df_permissive.show(10)

    df_malformed = read_csv_file(
        spark=spark,
        file_path=file_path,
        mode="DROPMALFORMED",
        emp_schema=emp_schema
    )
    df_malformed.show(10)

    input('Enter ctrl + c to terminate: ')
