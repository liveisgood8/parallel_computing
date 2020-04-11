from src.parser import spark


def main():
    df = spark.sql('select id from data_table')


if __name__ == '__main__':
    main()
