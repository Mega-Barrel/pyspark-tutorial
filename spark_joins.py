""" Joins"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def main(sparksession):
    customer_data = [
        (1, 'manish', 'patna', '30-05-2022'),
        (2, 'riya', 'delhi', '12-03-2023'),
        (3, 'arjun', 'mumbai', '05-07-2021'),
        (4, 'neha', 'bangalore', '19-11-2022'),
        (5, 'vivek', 'kolkata', '23-09-2020'),
        (6, 'sana', 'lucknow', '15-01-2024'),
        (7, 'rohit', 'jaipur', '02-06-2021'),
        (8, 'priya', 'chennai', '17-12-2023'),
        (9, 'anil', 'pune', '10-10-2022'),
        (10, 'kavya', 'hyderabad', '08-08-2020'),
    ]
    customer_schema = [
        'customer_id', 'customer_name',
        'customer_address', 'customer_date_of_joining'
    ]

    customer_df = sparksession.createDataFrame(data=customer_data, schema = customer_schema)
    customer_df.createOrReplaceTempView("customer_tbl")

    sales_data = [
        (101, 1, '2023-02-15', 3),
        (102, 2, '2023-03-21', 2),
        (103, 3, '2022-08-10', 4),
        (104, 4, '2023-01-05', 1),
        (105, 5, '2021-11-18', 2),
        (106, 6, '2024-03-10', 5),
        (107, 7, '2021-12-01', 1),
        (108, 8, '2024-01-20', 2),
        (109, 9, '2023-04-14', 3),
        (110, 10, '2020-09-22', 4),
    ]
    sales_schema = [
        'sales_id', 'customer_id',
        'sales_date', 'sales_quantity'
    ]

    sales_df = sparksession.createDataFrame(data=sales_data, schema=sales_schema)
    sales_df.createOrReplaceTempView("sales_tbl")

    # Sort Merge JOIN
    sort_merge_df = (
        customer_df.
        join(
            sales_df.hint("shuffle_merge"),
            customer_df['customer_id'] == sales_df['customer_id'],
            "inner"
        )
    )

    sort_merge_df.show()
    sort_merge_df.explain()

if __name__ == "__main__":
    spark = (
        SparkSession.
        builder.
        master("local[5]").
        appName('joins').
        getOrCreate()
    )
    spark.conf.set("spark.sql.shuffle.partitions", 3)
    main(sparksession = spark)
    input("press ctrl + c to exit")
