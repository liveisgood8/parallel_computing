import numpy as np

from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from pyspark.sql import DataFrame
from .uitls import convert_df_to_np_arr
from ..config import TEST_SIZE


class LogisticClassifier:
    def __init__(self, df: DataFrame, labels_df: DataFrame):
        self.model = LogisticRegression()

        data, labels_data = convert_df_to_np_arr(df, labels_df)

        self.train, self.test, self.train_labels, self.test_labels = train_test_split(
            data, labels_data, test_size=TEST_SIZE, random_state=0)

        # Обучаем на тренировочных данных
        self.model.fit(self.train, self.train_labels)

    def get_score(self):
        return self.model.score(self.test, self.test_labels)

    def predict(self, test_df: DataFrame):
        test = np.array(test_df)
        # test = np.reshape(test, newshape=(1, -1)) # if test_df contains only one row

        predictions = self.model.predict(test)

        return predictions
