
"""

@author: Abhinav
"""

import os
from pyspark.sql import SparkSession
from config import hive_location,file_location


def main():
    """
    Execution of job starts here.

    """
    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("EventsData") \
        .config("spark.sql.warehouse.dir", hive_location) \
        .enableHiveSupport() \
        .getOrCreate()

    files = os.listdir(file_location)

    for file_name in files:
        try:
            # dataframe creation and transformation
            df = spark.read.json("resources/JSON_Files/{}".format(file_name))
            column_names = [name.split('.')[1] if '.' in name else name for name in df.columns]
            table_name=df.columns[0].split('.')[0]
            df = df.toDF(*column_names)
            df.show()

            # Writing to hive
            df.createOrReplaceTempView("dataframe")
            spark.sql("insert into table {} select * from dataframe".format(table_name))

        except Exception as e:
            print("Following Error occured while running Job:->", e)

if __name__ == "__main__":

    main()




