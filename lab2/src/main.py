import src.data_preparer as data_preparer

from src.classification.random_forest import ForestClassifier
from src.classification.logistic_regression import LogisticClassifier


def main():
    full_data_frame = data_preparer.get_truncated_data_frame()

    data_frame = full_data_frame.drop('CLASS').collect()
    labels_data_frame = full_data_frame.select('CLASS').collect()

    forest_classifier = ForestClassifier(
        df=data_frame,
        labels_df=labels_data_frame,
    )

    log_regression_classifier = LogisticClassifier(
        df=data_frame,
        labels_df=labels_data_frame,
    )

    print('Random forest classifier score:', forest_classifier.get_score())
    print('Logistic regression classifier score:', log_regression_classifier.get_score())


if __name__ == "__main__":
    main()
