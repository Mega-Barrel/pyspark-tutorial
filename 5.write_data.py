
from pyspark.sql import SparkSession

def main(spark_session, file_path):
    df = (
        spark_session.
        read.
        format("csv").
        option("header", "true").
        option("inferschema", "true").
        option("mode", "PERMISSIVE").
        load(file_path)
    )
    return df

if __name__ == "__main__":
    spark = (
        SparkSession.
        builder.
        appName('JobStageTaskExample').
        getOrCreate()
    )
    file_path = './data/employee_data.csv'

    df = main(spark_session=spark, file_path=file_path)
    df.show()

    print(f"Writing data")

    (
        df.
        write.
        format("csv").
        option("header", "true").
        option("mode", "overwrite").
        option("path", './data/output_folder').
        partitionBy("address").
        save()
    )
    print("File saved!")

    input("press ctrl + c to exit")
