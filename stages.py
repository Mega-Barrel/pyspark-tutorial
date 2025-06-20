""" Application -> Job -> Stage -> Task"""

from pyspark.sql import SparkSession

def main(spark_session):
    data = [
        ('xwq', 24),
        ('avc', 24),
        ('yui', 28),
        ('ops', 50),
        ('ps', 50)
    ]
    df = spark_session.createDataFrame(data, ['name', 'age'])
    df.count()
    df.filter("age > 25").show()
    print(df)
    print(df.explain())

if __name__ == "__main__":
    spark = (
        SparkSession.
        builder.
        appName('JobStageTaskExample').
        getOrCreate()
    )
    main(spark_session = spark)
    input("press ctrl + c to exit")
