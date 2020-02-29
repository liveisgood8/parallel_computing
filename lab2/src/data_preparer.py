from pyspark.sql import SparkSession
import src.config as config

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
    truncated_df = spark.sql('select case when Climate_Region_Pub = {} then 1 else 0 end CLASS, '
                             'TOTALDOLCOL, BTUELCOL, TOTALDOLSPH, HEATHOME, BTUNGSPH, DOLNGSPH CELLAR '
                             'from data_table'.format(config.DATA_SET_REGION_PUB))
    return truncated_df


def get_truncated_data_frame_for_region():
    truncated_df = spark.sql('select 1 as CLASS, '
                             'TOTALDOLCOL, BTUELCOL, TOTALDOLSPH, HEATHOME, BTUNGSPH, DOLNGSPH CELLAR '
                             'from data_table where Climate_Region_Pub=' + str(config.DATA_SET_REGION_PUB))
    return truncated_df
