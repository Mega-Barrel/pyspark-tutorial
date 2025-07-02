
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType
)
from pyspark.sql.functions import (
    col,
    count,
    sum as pysum,
    avg as pyavg,
    min as pymin,
    max as pymax
)

def create_dataframe(spark_session, data, schema):
    """ Create DataFrame"""
    return spark_session.createDataFrame(data=data, schema=schema)

if __name__ == "__main__":

    spark = (
        SparkSession.
        builder.
        appName('tutorial').
        getOrCreate()
    )

    schema = StructType(
        [
            StructField('id', IntegerType(), True),
            StructField('name', StringType(), True),
            StructField('age', IntegerType(), True),
            StructField('salary', IntegerType(), True),
            StructField('country', StringType(), True),
            StructField('department', StringType(), True)
        ]
    )

    data = [
        (1, 'manish', 26, 20000, 'india', 'IT'),
        (2, 'rahul', None, 40000, 'germany', 'engineering'),
        (3, 'pawan', 12, 60000, 'india', 'sales'),
        (4, 'roshini', 44, None, 'uk', 'engineering'),
        (5, 'raushan', 35, 70000, 'india', 'sales'),
        (6, None, 29, 200000, 'uk', 'IT'),
        (7, 'adam', 37, 65000, 'us', 'IT'),
        (8, 'chris', 16, 40000, 'us', 'sales'),
        (None, None, None, None, None, None),
        (7, 'adam', 37, 65000, 'us', 'IT')
    ]

    df = create_dataframe(spark_session=spark, data=data, schema=schema)

    df.show()
    print(f'Total records: {df.count()}')
    print(f'Total records in name column: {df.select(col("name")).count()}')

    df.select(count(col('name')))
    df.select(count(col('name'))).show()

    df.select(
        pysum(col('salary')).alias('Total Salary'),
        pyavg(col('salary')).alias('Avg Salary'),
        pymin(col('salary')).alias('Min Salary'),
        pymax(col('salary')).alias('Max Salary')
    ).show()

    df.select(
        (pysum(col('salary')) / df.count()).alias('Avg Salary Calculated')
    ).show()

    input('Press ctrl + c to exit the process: ...')
