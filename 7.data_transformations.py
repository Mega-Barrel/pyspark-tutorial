
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    expr,
    lit
)

def read_dataframe(spark_session, file_path):
    """ Read DataFrame"""
    return (
        spark_session.
        read.
        format('csv').
        option('inferschema', 'true').
        option('header', 'true').
        load(file_path)
    )

if __name__ == "__main__":
    spark = (
        SparkSession.
        builder.
        appName('JobStageTaskExample').
        getOrCreate()
    )

    file_path = './data/employee_data.csv'

    df = read_dataframe(
        spark_session = spark,
        file_path = file_path
    )

    print(df.columns)
    print(df.printSchema())
    df.show()

    # Column Selection
    print(df.select('name').show())
    print(df.select(col('name'), col('salary')).show())
    print(df.select('id', col('name'), df['salary'], df.address).show())
    print(df.select("*").show())
    print(df.select(expr('id + 5 as employee_id')).show())
    print(df.select(expr('concat(name, address) as emp_name_address')).show())

    # Alias
    print(df.select(col('id').alias('employee_id'), 'name', 'age', 'salary').show())

    # filter
    print(df.filter(col('salary') > 80000).show())
    print(df.filter((col('salary') > 20000) & (col('age') < 18)).show())

    # Literal
    print(df.select(col('*'), lit(' NA').alias('last_name')).show())
    print(df.withColumn('surname', lit('NULL')).show())

    # Rename Column
    print(df.withColumnRenamed('id', 'employee_id').show())

    # Change Data Type
    change_dtype = (
        df.
        withColumn('id', col('id').cast('string')).
        withColumn('salary', col('salary').cast('long'))
    )
    print(change_dtype.printSchema())

    # Remove Column
    print(df.drop('name').show())
    print(df.drop(col('id')).show())

    input("press ctrl + c to exit")
