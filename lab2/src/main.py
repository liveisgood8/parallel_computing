import numpy as np
import src.data_preparer as data_preparer

from src.classification.random_forest import ForestClassifier


def main():
    data_frame = data_preparer.get_truncated_data_frame()
    data_frame_my_region = data_preparer.get_truncated_data_frame_for_region()

    forest_classifier = ForestClassifier(
        train_df=data_frame.drop('CLASS').collect(),
        train_labels_df=data_frame.select('CLASS').collect(),
    )

    print(forest_classifier.predict(data_frame.drop('CLASS').collect()[0]))
    print(forest_classifier.predict(data_frame_my_region.drop('CLASS').collect()[0]))


main()
