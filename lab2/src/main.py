from src.parser import spark


def main():
    print('-- task №1 --')
    avg_sqft_difference = spark.sql(
        'select gross_sqft, gross_sqft - (select avg(gross_sqft) from data_table) as avg_difference '
        'from data_table'
    )
    avg_sqft_difference.show()

    print('-- task №2 --')
    avg_sqft_by_years = spark.sql('select year_built, avg(gross_sqft) '
                                  'from data_table group by year_built')
    avg_sqft_by_years.show()

    print('-- task №3 --')
    avg_price_by_class_and_neighborhood = spark.sql(
        'select neighborhood, building_class_category, avg(sale_price) from data_table '
        'group by neighborhood, building_class_category')
    avg_price_by_class_and_neighborhood.show()

    print('-- task №4 --')
    df = spark.sql('select * from data_table')
    print('original df count:', df.count())
    modified_df = df\
        .where(df.year_built != 0)\
        .where(df.year_built > 2000)
    print('truncated df count:', df.count())
    modified_df.show()


if __name__ == '__main__':
    main()
