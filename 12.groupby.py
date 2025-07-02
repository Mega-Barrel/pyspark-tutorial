
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import (
    col,
    sum as pysum
)
from pyspark.sql.types import (
    StructField,
    StructType,
    IntegerType,
    StringType,
    DecimalType
)

def create_dataframe(spark_session, data, schema):
    """ Create DataFrame"""
    return spark_session.createDataFrame(data=data, schema=schema)

if __name__ == "__main__":

    spark = (
        SparkSession.
        builder.
        appName("tutorial").
        getOrCreate()
    )

    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("salary", IntegerType(), True),
            StructField("country", StringType(), True),
            StructField("department", StringType(), True)
        ]
    )

    data = [
        (1, 'manish', 50000, "India", "IT"),
        (2, 'vikash', 60000, "US", "sales"),
        (3, 'raushan', 70000, "India", "marketing"),
        (4, 'mukesh', 80000, "US", "IT"),
        (5, 'pritam', 90000, "India", "sales"),
        (6, 'nikita', 45000, "Japan", "marketing"),
        (7, 'ragini', 55000, "Japan", "marketing"),
        (8, 'rakesh', 100000, "India", "IT"),
        (9, 'aditya', 65000, "India", "IT"),
        (10, 'rahul', 50000, "US", "marketing")
    ]

    df = create_dataframe(spark_session=spark, data=data, schema=schema)

    df.show()
    print(f"Total records: {df.count()}")

    print("Question 1: Total Salary")
    total_salary = df.select(pysum(col("salary")).alias("Total Salary"))
    total_salary.show()

    print("Question 2: Total Salary Department Level")
    dept_salary = (
        df.
        groupBy(col("department")).agg(
            pysum(col("salary")).alias("Total Salary")
        )
    )
    dept_salary.show()

    input("Press ctrl + c to exit the process: ...")
