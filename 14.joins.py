

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import (
    StructField,
    StructType,
    IntegerType,
    StringType
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

    customer_schema = StructType(
        [
            StructField("customer_id", IntegerType(), True),
            StructField("customer_name", StringType(), True),
            StructField("customer_address", StringType(), True),
            StructField("date_of_joining", StringType(), True)
        ]
    )

    sales_schema = StructType(
        [
            StructField("customer_id", IntegerType(), True),
            StructField("product_id", IntegerType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("date_of_purchase", StringType(), True)
        ]
    )

    product_schema = StructType(
        [
            StructField("product_id", IntegerType(), True),
            StructField("product_name", StringType(), True),
            StructField("product_price", IntegerType(), True)
        ]
    )

    # Data
    customer_data =  [
        (1, 'manish', 'patna', "30-05-2022"),
        (2, 'vikash', 'kolkata', "12-03-2023"),
        (3, 'nikita', 'delhi', "25-06-2023"),
        (4, 'rahul', 'ranchi', "24-03-2023"),
        (5, 'mahesh', 'jaipur', "22-03-2023"),
        (6, 'prantosh', 'kolkata', "18-10-2022"),
        (7, 'raman', 'patna', "30-12-2022"),
        (8, 'prakash', 'ranchi', "24-02-2023"),
        (9, 'ragini', 'kolkata', "03-03-2023"),
        (10, 'raushan', 'jaipur', "05-02-2023")
    ]

    sales_data = [
        (1, 22, 10, "01-06-2022"),
        (1, 27, 5, "03-02-2023"),
        (2, 5, 3, "01-06-2023"),
        (5, 22, 1, "22-03-2023"),
        (7, 22, 4, "03-02-2023"),
        (9, 5, 6, "03-03-2023"),
        (2, 1, 12, "15-06-2023"),
        (1, 56, 2, "25-06-2023"),
        (5, 12, 5, "15-04-2023"),
        (11, 12, 76, "12-03-2023")
    ]

    product_data = [
        (1, 'fanta', 20),
        (2, 'dew', 22),
        (5, 'sprite', 40),
        (7, 'redbull', 100),
        (12, 'mazza', 45),
        (22, 'coke', 27),
        (25, 'limca', 21),
        (27, 'pepsi', 14),
        (56, 'sting', 10)
    ]

    customer_df = create_dataframe(spark_session=spark, data=customer_data, schema=customer_schema)
    sales_df = create_dataframe(spark_session=spark, data=sales_data, schema=sales_schema)
    product_df = create_dataframe(spark_session=spark, data=product_data, schema=product_schema)

    print("customer dataframe.")
    customer_df.show()
    print(f"Total records in customer_df: {customer_df.count()}")

    print("sales dataframe.")
    sales_df.show()
    print(f"Total records in sales_df: {sales_df.count()}")

    print("product dataframe.")
    product_df.show()
    print(f"Total records in product_df: {product_df.count()}")

    joined_df = customer_df.alias("c").join(
        sales_df.alias("s"),
        on = "customer_id",
        how = "inner"
    )
    joined_df.show()

    left_joined_df = customer_df.alias("c").join(
        sales_df.alias("s"),
        on = "customer_id",
        how = "left"
    )
    left_joined_df.show()

    input("Press ctrl + c to exit the process: ...")
