import src.data_preparer as data_preparer


def main():
    data_frame = data_preparer.get_truncated_data_frame_for_region()

    data_frame.select('Climate_Region_Pub').show()


main()
