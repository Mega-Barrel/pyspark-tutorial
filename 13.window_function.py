

from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import (
    col,
    round as pyround,
    sum as pysum,
    avg as pyavg,
    row_number,
    dense_rank,
    rank,
    lag,
    first,
    last,
    unix_timestamp,
    from_unixtime,
    to_timestamp,
    expr
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

    product_schema = ["id", "name", "date", "sales"]

    emp_schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("date", StringType(), True),
            StructField("time", StringType(), True)
        ]
    )

    data = [
        (1, 'manish', 50000, "India", "IT"),
        (2, 'vikash', 60000, "US", "sales"),
        (3, 'raushan', 70000, "India", "marketing"),
        (4, 'mukesh', 50000, "US", "IT"),
        (5, 'pritam', 90000, "India", "sales"),
        (6, 'nikita', 45000, "Japan", "marketing"),
        (7, 'ragini', 55000, "Japan", "marketing"),
        (8, 'rakesh', 100000, "India", "IT"),
        (9, 'aditya', 65000, "India", "IT"),
        (10, 'rahul', 50000, "US", "marketing")
    ]

    product_data = [
        (1, "iphone", "01-01-2023", 1500000),
        (2, "samsung", "01-01-2023", 1100000),
        (3, "oneplus", "01-01-2023", 1100000),
        (1, "iphone", "01-02-2023", 1300000),
        (2, "samsung", "01-02-2023", 1120000),
        (3, "oneplus", "01-02-2023", 1120000),
        (1, "iphone", "01-03-2023", 1600000),
        (2, "samsung", "01-03-2023", 1080000),
        (3, "oneplus", "01-03-2023", 1160000),
        (1, "iphone", "01-04-2023", 1700000),
        (2, "samsung", "01-04-2023", 1800000),
        (3, "oneplus", "01-04-2023", 1170000),
        (1, "iphone", "01-05-2023", 1200000),
        (2, "samsung", "01-05-2023", 980000),
        (3, "oneplus", "01-05-2023", 1175000),
        (1, "iphone", "01-06-2023", 1100000),
        (2, "samsung", "01-06-2023", 1100000),
        (3, "oneplus", "01-06-2023", 1200000)
    ]

    emp_data = [
        (1, "manish", "11-07-2023", "10:20"),
        (1, "manish", "11-07-2023", "11:20"),
        (2, "rajesh", "11-07-2023", "11:20"),
        (1, "manish", "11-07-2023", "11:50"),
        (2, "rajesh", "11-07-2023", "13:20"),
        (1, "manish", "11-07-2023", "19:20"),
        (2, "rajesh", "11-07-2023", "17:20"),
        (1, "manish", "12-07-2023", "10:32"),
        (1, "manish", "12-07-2023", "12:20"),
        (3, "vikash", "12-07-2023", "09:12"),
        (1, "manish", "12-07-2023", "16:23"),
        (3, "vikash", "12-07-2023", "18:08")
    ]

    df = create_dataframe(spark_session=spark, data=data, schema=schema)
    product_df = create_dataframe(spark_session=spark, data=product_data, schema=product_schema)

    emp_df = create_dataframe(spark_session=spark, data=emp_data, schema=emp_schema)

    df.show()
    print(f"Total records in df: {df.count()}")
    print()

    product_df.show()
    product_df.printSchema()
    print(f"Total records in product_df: {product_df.count()}")

    row_num_rnk_dense_rnk_window = Window.partitionBy(col("department")).orderBy(col("salary").asc())
    row_num_rnk_dense_rnk_df = (
        df.withColumn(
            "row_number",
            row_number().over(row_num_rnk_dense_rnk_window)
        ).
        withColumn(
            "rank",
            rank().over(row_num_rnk_dense_rnk_window)
        ).
        withColumn(
            "dense_rank",
            dense_rank().over(row_num_rnk_dense_rnk_window)
        )
    )
    row_num_rnk_dense_rnk_df.show()


    print("Question 1: Calculate salary % for every employee across department")
    dept_partition = Window.partitionBy(col("department"))
    partition_df = (
        df.withColumn(
            "total_dept_salary",
            pysum(col("salary")).over(dept_partition)
        ).withColumn(
            "salary %",
            ((col("salary") * 100.00) / col("total_dept_salary")).cast(DecimalType(10, 2))
        )
    )
    partition_df.show()


    print("Question 2: Return Top 2 employee with highest salary department wise")
    top_2_window = Window.partitionBy(col("department")).orderBy(col("salary").desc())
    top_2_df = (
        df.
        withColumn(
            "top_2_rnk",
            dense_rank().over(window=top_2_window)
        ).filter(col("top_2_rnk").isin(1, 2))
    )
    top_2_df.show()


    print("Question 3: Last 6 months product sales %")
    product_window = Window.partitionBy(col("name"))
    product_lag_df = (
        product_df.
        withColumn(
            "total_sales",
            pysum(col("sales")).over(window=product_window)
        ).
        withColumn(
            "sales %",
            pyround((col("sales") / col("total_sales")) * 100.00, 2)
        ).orderBy(col("name").asc(), col("date").asc())
    )
    product_lag_df.show()


    print("Question 4: MoM sales % change product wise")
    product_window = Window.partitionBy(col("name")).orderBy(col("date").asc())
    product_lag_df = (
        product_df.
        withColumn(
            "prev_mnth_sales",
            lag(col("sales"), 1).over(window=product_window)
        ).
        withColumn(
            "sales_change",
            pyround(((col("sales") - col("prev_mnth_sales")) / col("prev_mnth_sales")) * 100.00, 2)
        ).
        filter(col("prev_mnth_sales").isNotNull())
    )
    product_lag_df.show()


    print("Question 5: Find difference in sales from first_month to latest_month product wise")
    product_window = Window.partitionBy(col("name")).orderBy(col("date").asc()).rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    product_lag_df = (
        product_df.
        withColumn(
            "first_mnth_sales",
            first(col("sales")).over(window=product_window)
        ).
        withColumn(
            "last_mnth_sales",
            last(col("sales")).over(window=product_window)
        ).
        withColumn(
            "sales_change",
            pyround(((col("last_mnth_sales") - col("first_mnth_sales")) / col("first_mnth_sales")) * 100.00, 2)
        )
    )
    product_lag_df.show()


    print("Question 6: Find employees who have not completed 8 hours in office")
    emp_df = (
        emp_df.
        withColumn(
            "timestamp",
            from_unixtime(
                unix_timestamp(
                    expr("CONCAT(date, ' ', time)"),
                    "dd-MM-yyyy HH:mm"
                )
            )
        )
    )
    emp_ofc_window = Window.partitionBy(col("id"), col("date")).orderBy(col("timestamp").asc()).rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    emp_time_df = (
        emp_df.
        withColumn(
            "entry_time",
            first(col("timestamp")).over(window=emp_ofc_window)
        ).
        withColumn(
            "exit_time",
            last(col("timestamp")).over(window=emp_ofc_window)
        ).
        # convert to timestamp
        withColumn(
            "entry_time",
            to_timestamp(col("entry_time"), "yyyy-MM-dd HH:mm:ss")
        ).
        # convert to timestamp
        withColumn(
            "exit_time",
            to_timestamp(col("exit_time"), "yyyy-MM-dd HH:mm:ss")
        ).
        # final
        withColumn(
            "total_time",
            (col("exit_time") - col("entry_time")).cast("long")
        )
    ).filter((col("total_time") / 60 / 60) < 8)
    # show emp_time_df
    (
        emp_time_df.
        select(
            col("id"),
            col("date"),
            col("name")
        ).
        distinct().
        orderBy(
            col("date").
            asc()
        ).
        show()
    )


    print("Question 7: Last 3 months avg sales with current sales")
    rolling_window = (
        Window.
        partitionBy(col("name")).
        orderBy(col("date").asc()).
        rowsBetween(-2, 0)
    )
    default_window = (
        Window.
        partitionBy(col("name")).
        orderBy(col("date").asc())
    )
    cal_sales_df = (
        product_df.
        withColumn(
            "avg_sales",
            pyround(pyavg(col("sales")).over(rolling_window), 2)
        )
    )
    cal_sales_df = (
        cal_sales_df.
        withColumn(
            "row_number",
            row_number().over(default_window)
        )
    ).filter(col("row_number") > 2)
    cal_sales_df.show()


    input("Press ctrl + c to exit the process: ...")
