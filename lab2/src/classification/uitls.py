import numpy as np

from pyspark.sql import DataFrame


def convert_df_to_np_arr(df: DataFrame, labels_df: DataFrame):
    data = np.array(df)
    labels_data = np.ravel(np.array(labels_df))
    return data, labels_data
