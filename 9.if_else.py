
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StringType,
    IntegerType,
    StructField,
    StructType
)
from pyspark.sql.functions import (
    col,
    when,
    lit
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
    schema = StructType(
        [
            StructField('id', IntegerType(), True),
            StructField('name', StringType(), True),
            StructField('age', IntegerType(), True),
            StructField('salary', IntegerType(), True),
            StructField('country', StringType(), True),
            StructField('dept', StringType(), True)
        ]
    )

    df = create_dataframe(
        spark_session = spark,
        data = data,
        schema= schema
    )

    df.show()
    print(f'Total records in df1: {df.count()}')

    # Case when statement
    df_case = (
        df.withColumn(
            'is_adult',
            when(col('age') >= 18, 'Adult').
            when(col('age') < 18, 'Not Adult').
            otherwise('NA')
        )
    )
    df_case.show()

    df_handle_null_case = (
        df.
        withColumn(
            'is_adult',
            when(col('age').isNull(), lit(10)).
            otherwise(col('age'))
        ).
        withColumn(
            'is_adult',
            when(col('is_adult') >= 18, 'Adult').
            when(col('is_adult') < 18, 'Not Adult').
            otherwise('NA')
        )
    )
    df_handle_null_case.show()

    df_major_minor = (
        df.
        withColumn(
            'is_minor',
            when((col('age') > 0) & (col('age') < 18), 'minor').
            when((col('age') >= 18) & (col('age') < 30), 'mid').
            otherwise('major')
        )
    )
    df_major_minor.show()

    input("press ctrl + c to exit")
