from datetime import date, datetime
import requests
import csv
import re
from sql_scripts import check_last_load, insert_csv_meta
from transform import read_csv

base_url = 'https://warsydprdstafuelwatch.blob.core.windows.net/historical-reports/FuelWatchRetail'
file_list = []
file_meta = {}
table = 'raw.raw_fuel_watch__historic_fuel_price'
column = 'PUBLISH_DATE'


def get_storage():
    # if you are working with cloud, you'll need to write some code here to get PATs, blob directory url etc.
    # assign the output to storage_path
    # in this case I am saving to local machine
    storage_path = "../raw/"
    return storage_path


def end_date():
    end_date = datetime.now().date()
    end_year = date.today().strftime('%Y')
    end_month = date.today().strftime('%m')
    end_date_int = int("".join([end_year, end_month]))
    return end_date, end_date_int, end_year, end_month


def start_date(conn, entity, column):
    start_date = check_last_load(conn, entity, column)
    start_year = start_date.strftime('%Y')
    start_month = start_date.strftime('%m')
    start_date_int = int("".join([start_year, start_month]))
    return start_date, start_date_int, start_year, start_month


def get_link(conn, table, column):
    year = int(start_date(conn, table, column)[2])
    month = int(start_date(conn, table, column)[3])
    check_working_date = start_date(conn, table, column)[1]
    check_end = end_date()[1]
    url_list = []

    while check_working_date < check_end:
        if month == 13:
            month = 1
            year += 1
            check_working_date = int(f"{year}0{month}")
        elif month < 10:
            file_name_one_int = f"{base_url}-0{month}-{year}.csv"
            url_list.append(file_name_one_int)
            check_working_date = int(f"{year}0{month}")
            month += 1
        elif 10 <= month <= 12:
            file_name_two_int = f"{base_url}-{month}-{year}.csv"
            url_list.append(file_name_two_int)
            check_working_date = int(f"{year}{month}")
            month += 1
    return url_list


def extract_data(conn, table=table, column=column):
    file_path = get_storage()
    url_list = get_link(conn, table, column)
    for url in url_list:
        try:
            # Send GET request
            response = requests.get(url)
            response.raise_for_status()
            print("Response 200 OK")
            csv_dld_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            # Decode content
            content = response.text
            # Create a CSV reader object from the decoded content
            csv_reader = csv.reader(content.splitlines())

            # Extract the header and rows from the CSV reader
            header = next(csv_reader)
            rows = list(csv_reader)
            row_count = len(rows)
            csv_url = url

            # Specify the destination filename for the locally saved CSV file
            file_name = re.findall('[^/]*$', url)[0]
            csv_file = f"{file_path}{file_name}"
            file_list.append((file_name, csv_dld_at, csv_url))

            file_meta[file_name] = row_count

            # Write the CSV data to a local file
            with open(csv_file, "w", newline="") as csvfile:
                csv_writer = csv.writer(csvfile)

                # Write the header
                csv_writer.writerow(header)

                # Write the rows
                csv_writer.writerows(rows)

        except requests.exceptions.HTTPError as e:
            print("Response not 200:", e)

    # Inserts csv file name and row count into metadata table
    for key, value in file_meta.items():
        insert_csv_meta(key, value, conn)

    read_csv(conn, file_list, file_path)
