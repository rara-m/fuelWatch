import pyspark
from pyspark.sql.functions import *
from pyspark import SparkContext
from pyspark.sql.types import IntegerType, StringType, FloatType, StructField
from pyspark.sql import functions as f
from datetime import datetime, timedelta
import time
from load_raw import raw_loader
from sql_scripts import insert_raw_meta, check_last_load

sc = SparkContext()
spark = pyspark.sql.SparkSession(sc, jsparkSession=None)
raw_meta = {}
entity_raw_fuel_data = 'raw.raw_fuel_watch__historic_fuel_price'
fuel_column = 'PUBLISH_DATE'
entity_raw_dim_date = 'raw.raw_dim_date'
date_column = 'date'


def read_csv(conn, files, storage_path, schema='raw', entity=entity_raw_fuel_data, column=fuel_column):
    req_cols = ["PUBLISH_DATE",
                "TRADING_NAME",
                "BRAND_DESCRIPTION",
                "PRODUCT_DESCRIPTION",
                "PRODUCT_PRICE",
                "ADDRESS",
                "LOCATION",
                "POSTCODE",
                "AREA_DESCRIPTION",
                "REGION_DESCRIPTION",
                "loaded_at",
                "source_file_name"]
    for file in files:
        latest_load = check_last_load(conn, entity, column)
        file_name = file[0]
        dld_at = file[1]
        csv_url = file[2]
        file_path = f"{storage_path}{file_name}"
        print(file_path)
        df = spark.read.option("header", True).csv(file_path)
        df2 = df.withColumn("loaded_at", f.lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))) \
            .withColumn("source_file_name", lit(file_name)) \
            .withColumn("PUBLISH_DATE", to_date(df["PUBLISH_DATE"], "dd/MM/yyyy"))
        df3 = df2.select(req_cols)
        row_count = df3.count()
        raw_loader(df3, conn, entity)
        completed_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        insert_raw_meta(row_count, completed_at, dld_at, csv_url, file_name, conn)


def create_dim_date(conn, schema='raw', entity=entity_raw_dim_date, column=date_column):
    latest_load = check_last_load(conn, entity, column)
    current_date = datetime.now()
    startdate = datetime.strptime(str(latest_load), "%Y-%m-%d %H:%M:%S") # Fuelwatch dataset starts in January 2001
    loaded_at = datetime.now()  # NOTE: returns UTC+0 date time
    enddate = current_date + timedelta(days=365 * 1)  # end +1 week from current date

    ddl_schema = StructType([
        StructField("new_column_name", StringType(), True),
        StructField("expression", StringType(), True)
    ])

    ddl_data_with_comments = [
        ("DateKey", "cast(date_format(date, 'yyyyMMdd') as int)"),  # Unique identifier for date
        ("Day", "day(date)"),  # Day of the month
        ("DayOfWeek", "dayofweek(date)"),  # Numeric representation of the day of the week
        # Full day name (e.g., Monday, Tuesday)
        ("DayOfWeekNameEEEE", "date_format(date, 'EEEE')"),
        # Short day name (e.g., Mon, Tue)
        ("DayOfWeekNameEE", "date_format(date, 'EEE')"),
        ("DayOfMonth", "cast(date_format(date, 'd') as int)"),  # Numeric day of the month
        ("DayOfYear", "cast(date_format(date, 'D') as int)"),  # Numeric day of the year
        ("DayNumberString", "date_format(date, 'dd')"),  # String representation of day with leading zero
        ("Week", "weekofyear(date)"),  # Week of the year
        ("WeekNameLong", "concat('Week ', lpad(weekofyear(date), 2, '0'))"),  # Long week name
        ("WeekNameShort", "concat('W ', lpad(weekofyear(date), 2, '0'))"),  # Short week name
        ("WeekNumberString", "lpad(weekofyear(date), 2, '0')"),  # String representation of week with leading zero
        ("WeekStartDate", "cast(date_trunc('week', date) as date)"),  # Start date of week(Monday)
        ("WeekEndDate", "date_add(cast(date_trunc('week', date) as date),6)"),  # End date of week(Sunday)
        ("Month", "month(date)"),  # Numeric month
        ("MonthNameMMMM", "date_format(date, 'MMMM')"),  # Full month name
        ("MonthNameMMM", "date_format(date, 'MMM')"),  # Short month name
        ("MonthNumberMM", "date_format(date, 'MM')"),  # String representation of month with leading zero
        ("MonthYearMMM-YY", "concat(date_format(date, 'MMM'), '-',  substring(year(date), -2, 2))"),
        # for example Jan-24
        ("MonthYearIndex", "(year(date)-year(current_date()))*12 + (month(date)-month(current_date()))"),
        # for example 0 for current month, -1 for last month
        ("Quarter", "quarter(date)"),  # Numeric quarter
        ("QuarterNameQQQQ", "date_format(date, 'QQQQ')"),  # Full quarter name
        ("QuarterNameQQQ", "date_format(date, 'QQQ')"),  # Short quarter name
        ("QuarterNumberQQ", "date_format(date, 'QQ')"),  # String representation of quarter
        ("QuarterYearQQ-YY", "concat(date_format(date, 'QQQ'), '-', substring(year(date), -2, 2))"),
        # for example Q1-24
        (
            "QuarterYearIndex",
            "concat( (year(date) - year(current_date()))*4 + (quarter(date)-quarter(current_date())))"),
        # for example 0 for this quarter and -1 for last quater
        ("YearQuarterYYYYQQ", "concat(year(date), date_format(date, 'QQ'))"),
        # for example 202401, useful for sort order
        ("Year", "year(date)"),  # Year
        ("YearIndex", "year(date)-year(current_date())"),  # for example 0 for current year, -1 for last year
        ("YearMonthyyyyMM", "date_format(date, 'yyyyMM')"),  # Year and month in yyyMM format
        ("MonthStartDate", "cast(date_trunc('month', date) as date)"),  # Start date of the month
        ("MonthEndtDate", "cast(last_day(date) as date)")  # Last date of the month
    ]

    # Create DataFrame with the specified schema
    ddl_df = spark.createDataFrame(ddl_data_with_comments, schema=ddl_schema)

    start = int(startdate.timestamp())
    stop = int(enddate.timestamp())
    df = spark.range(start, stop, 60 * 60 * 24).select(col("id").cast("timestamp").cast("date").alias("Date"))
    for row in ddl_df.collect():
        new_column_name = row["new_column_name"]
        expression = expr(row["expression"])
        df = df.withColumn(new_column_name, expression)

    final = df.withColumn('loaded_at', lit(loaded_at))
    raw_loader(final, conn,  entity)
