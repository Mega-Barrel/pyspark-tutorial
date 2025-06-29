
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType
)


def create_dataframe(spark_session, df, schema):
    return spark_session.createDataFrame(data=df, schema=schema)

if __name__ == "__main__":
    spark = (
        SparkSession.
        builder.
        appName('JobStageTaskExample').
        getOrCreate()
    )

    data = [
        (1, 1),
        (2, 1),
        (3, 1),
        (4, 2),
        (5, 1),
        (6, 2),
        (7, 2)
    ]

    df_schema = StructType(
        [
            StructField('id', IntegerType(), False),
            StructField('num', IntegerType(), False)
        ]
    )

    df = create_dataframe(spark_session=spark, df=data, schema=df_schema)

    print(df.columns)
    print(df.printSchema())

    df.show()

    input("press ctrl + c to exit")
