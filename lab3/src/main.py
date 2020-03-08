import os
import src.data_preparer as data_preparer

from src.classification.random_forest import ForestClassifier
from src.classification.logistic_regression import LogisticClassifier
from src.config import OUTPUT_DATA_SET_DIR


truncated_data_set_path = os.path.join(OUTPUT_DATA_SET_DIR, 'truncated.csv')
predicted_data_set_path = os.path.join(OUTPUT_DATA_SET_DIR, 'truncated_predicted.csv')


def remove_old_data_sets():
    if os.path.exists(truncated_data_set_path):
        os.remove(truncated_data_set_path)
    if os.path.exists(predicted_data_set_path):
        os.remove(predicted_data_set_path)


def main():
    remove_old_data_sets()

    full_data_frame = data_preparer.get_truncated_data_frame()
    full_data_frame.toPandas().to_csv(truncated_data_set_path, index=False)

    data_frame = full_data_frame.drop('CLASS', 'Climate_Region_Pub').collect()
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

    forest_predictions = forest_classifier.predict(data_frame)
    regression_predictions = forest_classifier.predict(data_frame)

    pd_df = full_data_frame.toPandas()

    pd_df['FOREST_PREDICTED_CLASS'] = forest_predictions
    pd_df['REGRESSION_PREDICTED_CLASS'] = regression_predictions

    pd_df.to_csv(predicted_data_set_path, index=False)


if __name__ == "__main__":
    main()
