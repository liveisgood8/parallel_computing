import src.config as config

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local") \
    .appName("Word Count") \
    .getOrCreate()

df = spark.read.csv(
    path=config.DATA_SET_PATH,
    header=True,
    inferSchema=True
)
df.createOrReplaceTempView('data_table')

