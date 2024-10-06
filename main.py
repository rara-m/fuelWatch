# Import modules
from create_db import create_db
from connect import connect
from extract import extract_data, get_link
from transform import create_dim_date
from sql_scripts import check_last_load


entity = 'raw.raw_fuel_watch__historic_fuel_price'
column = 'PUBLISH_DATE'

# Check connection
conn = connect()


create_db(conn)

#create_dim_date(conn)

# This function calls the insert_csv_meta and read_csv functions
# Read_csv calls raw_loader
extract_data(conn)


