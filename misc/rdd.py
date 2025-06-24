
""" PySparkJob Class"""
from pyspark.sql import SparkSession

spark = (
    SparkSession.
    builder.
    appName("spark-init").
    master("local[4]").
    getOrCreate()
)

sc = spark.sparkContext

data = [
    1, 2, 3, 4, 5,
    6, 7, 8, 9, 10,
    11, 12
]

rdd1 = sc.parallelize(data)
rdd1.collect()

data_2 = [
    (1, 'Apple'),
    (3, 'Apricot'),
    (1, 'Banana'),
    (2, 'Watermelon'),
    (2, 'Jackfruit'),
]

rdd2 = sc.parallelize(data_2)
print(f'Total Records: {rdd2.count()}')

rdd3 = rdd2.groupByKey()
rdd3_res = rdd3.collect()
print(f'Grouped Records: {rdd3}')

input("Type ctrl + c to exit the session.")
