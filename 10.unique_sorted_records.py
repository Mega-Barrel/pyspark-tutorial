

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StringType,
    IntegerType,
    StructField,
    StructType
)
from pyspark.sql.functions import (
    col,
    when
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

    data = [
        (10, 'Anil', 50000, 18),
        (11, 'Vikas', 75000, 16),
        (12, 'Nisha', 40000, 18),
        (13, 'Nidhi', 60000, 17),
        (14, 'Priya', 80000, 18),
        (15, 'Mohit', 45000, 18),
        (16, 'Rajesh', 90000, 10),
        (17, 'Raman', 55000, 16),
        (18, 'Sam', 65000, 17),
        (15, 'Mohit', 45000, 18),
        (13, 'Nidhi', 60000, 17),
        (14, 'Priya', 90000, 18),
        (18, 'Sam', 65000, 17)
    ]

    leetcode_data = [
        (1, 'Will', None),
        (2, 'Jane', None),
        (3, 'Alex', 2),
        (4, 'Bill', None),
        (5, 'Zack', 1),
        (6, 'Mark', 2)
    ]

    schema = StructType(
        [
            StructField('id', IntegerType(), True),
            StructField('name', StringType(), True),
            StructField('salary', IntegerType(), True),
            StructField('manager_id', IntegerType(), True)
        ]
    )

    leetcode_schema = StructType(
        [
            StructField('id', IntegerType(), True),
            StructField('name', StringType(), True),
            StructField('referee_id', IntegerType(), True),
        ]
    )

    df = create_dataframe(
        spark_session = spark,
        data = data,
        schema= schema
    )

    leetcode_df = create_dataframe(
        spark_session = spark,
        data = leetcode_data,
        schema= leetcode_schema
    )

    # Distinct Records
    df.show()
    print(f'Total records in df: {df.count()}')
    print()

    df.distinct().show()
    print(f'Total Distinct records in df: {df.distinct().count()}')

    col_distinct = df.select(col('id'), col('name')).distinct()
    col_distinct.show()
    print(f'Total Distinct records in for id-name column: {col_distinct.count()}')

    # Drop Duplicate records
    print('Dropping duplicate entries...')
    dropped_df = df.drop_duplicates(['id', 'name', 'salary', 'manager_id'])
    print(f'Total records in df: {dropped_df.count()}')
    dropped_df.show()

    # Sort DataFrame
    df.sort(col('salary').asc()).show()
    df.sort(col('salary').desc()).show()
    df.sort(col('salary').desc(), col('name').asc()).show()

    # Leetcode Question
    leetcode_df.show()

    final_df = (
        leetcode_df.
        withColumn(
            'flag',
            when(col('referee_id').isNull(), 'N').
            when(col('referee_id') == 2, 'N').
            otherwise('Y')
        )
    )
    final_df.select(col('name')).filter(col('flag')=='Y').show()

    input("press ctrl + c to exit")
