import src.config as config

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local") \
    .appName("Word Count") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

df = spark.read.csv(
    path=config.DATA_SET_PATH,
    header=True,
    inferSchema=True
)
df.createOrReplaceTempView('data_table')


def get_truncated_data_frame():
    truncated_df = spark.sql('select TOTALDOLCOL, BTUELCOL, TOTALDOLSPH, HEATHOME, BTUNGSPH, DOLNGSPH CELLAR, '
                             'Climate_Region_Pub, case when Climate_Region_Pub = {} then 1 else 0 end CLASS '
                             'from data_table'.format(config.DATA_SET_REGION_PUB))
    return truncated_df


def get_truncated_data_frame_for_region():
    truncated_df = spark.sql('select TOTALDOLCOL, BTUELCOL, TOTALDOLSPH, HEATHOME, BTUNGSPH, DOLNGSPH CELLAR, '
                             'Climate_Region_Pub, 1 as CLASS '
                             'from data_table where Climate_Region_Pub=' + str(config.DATA_SET_REGION_PUB))
    return truncated_df
