import numpy as np

from sklearn.ensemble import RandomForestClassifier
from pyspark.sql import DataFrame


class ForestClassifier:
    def __init__(self, train_df: DataFrame, train_labels_df: DataFrame):
        # Создаём модель леса из сотни деревьев
        self.model = RandomForestClassifier(n_estimators=100,
                                            bootstrap=True,
                                            max_features='sqrt')

        train = np.array(train_df)
        train_labels = np.ravel(np.array(train_labels_df))

        # Обучаем на тренировочных данных
        self.model.fit(train, train_labels)

    def predict(self, test_df: DataFrame):
        test = np.array(test_df)
        test = np.reshape(test, newshape=(1, -1))

        # Действующая классификация
        predictions = self.model.predict(test)

        return predictions
