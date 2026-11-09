import os

from pyspark.sql import SparkSession

MYSQL_URL = os.environ.get("MYSQL_URL", "jdbc:mysql://localhost:3307/pivot")
MYSQL_USER = os.environ.get("MYSQL_USER", "root")
MYSQL_PASSWORD = os.environ.get("MYSQL_PASSWORD", "")


def get_spark(app_name):
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def write_table(df, table_name, mode="overwrite"):
    # truncate keeps the schema.sql table (types, keys) instead of
    # letting Spark drop and recreate it on overwrite
    (
        df.write.format("jdbc")
        .option("url", MYSQL_URL)
        .option("dbtable", table_name)
        .option("user", MYSQL_USER)
        .option("password", MYSQL_PASSWORD)
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .option("truncate", "true")
        .mode(mode)
        .save()
    )
