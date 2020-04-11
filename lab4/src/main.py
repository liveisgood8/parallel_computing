from pyspark.sql.functions import split, explode
from pyspark.sql.types import *

from src.parser import spark


def get_df_with_atomic_hashtag(original_df):
    splitted_hashtags = explode(split(original_df['hashtags'], ','))
    return original_df\
        .withColumn('hashtags', splitted_hashtags)


def get_top_hashtags(original_df):
    return get_df_with_atomic_hashtag(original_df)\
        .groupBy('hashtags')\
        .count()\
        .orderBy('count', ascending=False)\
        .limit(10)


def get_top_hashtag_for_place(df_with_atomic_hastags, place):
    return df_with_atomic_hastags\
        .where(df_with_atomic_hastags['place'] == place)\
        .groupBy('hashtags')\
        .count()\
        .orderBy('count', ascending=False)\
        .limit(1)\
        .collect()[0]


def get_top_hashtags_for_top_places(original_df):
    top_places = get_top_places(original_df).collect()
    temp_list = []
    df_with_atomic_hastags = get_df_with_atomic_hashtag(original_df)
    for top_places_row in top_places:
        place = top_places_row[0]
        top_hashtag, top_hashtag_count = get_top_hashtag_for_place(df_with_atomic_hastags, place)
        temp_list.append([place, top_hashtag, top_hashtag_count])

    schema = StructType([
        StructField('place', StringType()),
        StructField('hashtag', StringType()),
        StructField('count', IntegerType()),
    ])
    return spark.createDataFrame(temp_list, schema=schema)


def get_top_places(original_df):
    return original_df\
        .where(original_df['place'].isNotNull())\
        .groupBy('place')\
        .count()\
        .orderBy('count', ascending=False)\
        .limit(10)


def save_df_as_csv(df, path):
    df.toPandas().to_csv(path)


def main():
    df = spark.sql('select * from data_table')

    # top_hashtags = get_top_hashtags(df)  # Топ 10 хэштэгов
    # top_hashtags.show()
    #
    # top_places = get_top_places(df)  # Топ 10 мест из которых было больше всего твитов
    # save_df_as_csv(top_places, '../dataset/top_places.csv')
    # top_places.show()
    #
    # top_hashtag_for_top_places = get_top_hashtags_for_top_places(df)  # Топовый хэштэг для топ 10 мест по твитам
    # top_hashtag_for_top_places.show()

    


if __name__ == '__main__':
    main()
