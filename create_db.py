from sql_scripts import run_sql

# Define schemas to be created
schemas = ["raw", "metadata", "staging", "intermediate", "marts"]

# Define sql queries to run
create_schemas_sql = "create schema if not exists "

# Define raw table
create_raw_table = ("create table if not exists raw.raw_fuel_watch__historic_fuel_price (PUBLISH_DATE DATE, "
                    "TRADING_NAME VARCHAR, BRAND_DESCRIPTION VARCHAR, PRODUCT_DESCRIPTION VARCHAR, PRODUCT_PRICE "
                    "VARCHAR, ADDRESS VARCHAR, LOCATION VARCHAR, POSTCODE VARCHAR, AREA_DESCRIPTION VARCHAR, "
                    "REGION_DESCRIPTION VARCHAR, loaded_at TIMESTAMP, source_file_name VARCHAR);")

# Define dim date table
create_dim_date = ("create table if not exists raw.raw_dim_date (date date, date_key int, day varchar, day_of_week "
                   "varchar, day_of_week_name_eeee varchar, day_of_week_name_ee varchar, day_of_month varchar, "
                   "day_of_year varchar, day_number_string varchar, week varchar, week_name_long varchar, "
                   "week_name_short varchar, week_number_string varchar, week_start_date varchar, week_end_date "
                   "varchar, month varchar, month_name_mmmm varchar, month_name_mmm varchar, month_number_mm varchar, "
                   "month_year_dash_yy varchar, month_year_index varchar, quarter varchar, quarter_name_qqqq varchar, "
                   "quarter_name_qqq varchar, quarter_number_qq varchar, quarter_year_dash_yy varchar, "
                   "quarter_year_index varchar, year_quarter_yyyy_qq varchar, year varchar, year_index varchar, "
                   "year_month_yyyy_mm varchar, month_start_date varchar, month_end_date varchar, loaded_at timestamp);")

# Define metadata table
create_metadata = ("create table if not exists metadata.metadata (csv_name varchar default null,csv_row_count int "
                   "default null,loaded_row_count int default null, csv_dld_at timestamp default null, "
                   "csv_url varchar default null, loaded_at timestamp default null);")


def create_db(connection):
    # Create project schemas
    for schema in schemas:
        sql = f"{create_schemas_sql}{schema}"
        run_sql(connection, sql)

    # Create raw table
    run_sql(connection, create_raw_table)

    # Create metadata table
    run_sql(connection, create_metadata)

    # Truncate existing raw date table
    # Create raw date table
    run_sql(connection, create_dim_date)
