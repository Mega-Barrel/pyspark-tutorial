
from _spark_session import create_session

def read_json_file(spark, filepath):
    """ Read JSON file"""
    json_df = (
        spark.
        read.
        format("json").
        option("inferschema", "true").
        # option("multiline", "true").
        option("mode", "PERMISSIVE").
        load(filepath)
    )
    return json_df

if __name__ == "__main__":
    spark = create_session()
    # filepath = './data/json_multiline.json'
    filepath = './data/json_flight_data_2010.json'

    df = read_json_file(spark=spark, filepath=filepath)
    df.show(10)

    input('Enter ctrl + c to terminate: ')
