
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StringType,
    IntegerType,
    StructField,
    StructType
)

def create_dataframe(spark_session, data, schema):
    """ Create DataFrame"""
    return spark_session.createDataFrame(data=data, schema=schema)

if __name__ == "__main__":
    spark = (
        SparkSession.
        builder.
        appName('JobStageTaskExample').
        getOrCreate()
    )

    data1 = [
        (10, 'Anil', 50000, 18),
        (11, 'Vikas', 75000, 16),
        (12, 'Nisha', 40000, 18),
        (13, 'Nidhi', 60000, 17),
        (14, 'Priya', 80000, 18),
        (15, 'Mohit', 45000, 18),
        (16, 'Rajesh', 90000, 10),
        (17, 'Raman', 55000, 16),
        (18, 'Sam', 65000, 17)
    ]
    data2 = [
        (15, 'Mohit', 45000, 18),
        (19, 'Sohan', 50000, 18),
        (20, 'Sima', 75000, 17)
    ]
    data3 = [
        (19, 50000, 18, 'Sohan'),
        (20, 75000, 17, 'Sima')
    ]
    schema = StructType(
        [
            StructField('id', IntegerType(), True),
            StructField('name', StringType(), True),
            StructField('salary', IntegerType(), True),
            StructField('managerid', IntegerType(), True)
        ]
    )
    schema3 = StructType(
        [
            StructField('id', IntegerType(), True),
            StructField('salary', IntegerType(), True),
            StructField('managerid', IntegerType(), True),
            StructField('name', StringType(), True)
        ]
    )

    df1 = create_dataframe(
        spark_session = spark,
        data = data1,
        schema= schema
    )
    df2 = create_dataframe(
        spark_session = spark,
        data = data2,
        schema= schema
    )
    df3 = create_dataframe(
        spark_session = spark,
        data = data3,
        schema= schema3
    )

    df1.show()
    df2.show()
    df3.show()

    print(f'Total records in df1: {df1.count()}')
    print(f'Total records in df2: {df2.count()}')
    print(f'Total records in df3: {df3.count()}')

    union_df = df1.union(df2)
    union_df.show()
    print(f'Total records in union_df: {union_df.count()}')

    union_all_df = df1.unionAll(df2)
    union_all_df.show()
    print(f'Total records in union_all_df: {union_all_df.count()}')

    union_wrong_col_df = df2.union(df3)
    union_wrong_col_df.show()
    print(f'Total records in union_wrong_col_df: {union_wrong_col_df.count()}')

    union_wrong_col_by_name_df = df2.unionByName(df3)
    union_wrong_col_by_name_df.show()
    print(f'Total records in union_wrong_col_by_name_df: {union_wrong_col_by_name_df.count()}')

    input("press ctrl + c to exit")
