
from pyspark.sql import SparkSession

def main(spark_session, file_path):
    no_header_df = (
        spark_session.
        read.
        format("csv").
        option("header", "false").
        option("inferschema", "false").
        option("mode", "FAILFAST").
        load(file_path)
    )

    print(no_header_df.show(50))
    print()

    with_header_df = (
        spark_session.
        read.
        format("csv").
        option("header", "true").
        option("inferschema", "true").
        option("mode", "FAILFAST").
        load(file_path)
    )

    print(with_header_df.show(50))
    print(with_header_df.printSchema())

if __name__ == "__main__":
    spark = (
        SparkSession.
        builder.
        appName('JobStageTaskExample').
        getOrCreate()
    )
    file_path = './data/flight_data_2014_summary.csv'
    main(spark_session = spark, file_path=file_path)
    input("press ctrl + c to exit")
