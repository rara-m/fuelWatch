from datetime import datetime


def run_sql(conn, sql):
    try:
        with conn as conn:
            with conn.cursor() as cur:
                print(f"Running query: {sql}")
                cur.execute(sql)
                print("Completed")
    except (Exception, conn.DatabaseError) as error:
        print(error)
        conn.rollback()
        cur.close()
        return 1


def check_last_load(conn, entity, column):
    try:
        with (conn.cursor() as cur):
            cur.execute(f"select coalesce(max({column}), '2001-01-01') from {entity};")
            result = cur.fetchall()
            date = result[0][0]
            latest_load = datetime.strptime(str(date), "%Y-%m-%d")
            # hardcoded value for debugging
            #latest_load = datetime.strptime('2024-09-01', "%Y-%m-%d")
        return latest_load

    except (Exception, conn.DatabaseError) as error:
        print(error)
        cur.close()


def insert_csv_meta(file_name, row_count, conn):
    sql = """insert into metadata.metadata(csv_name, csv_row_count) values(%s, %s);"""
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (file_name, row_count))
            conn.commit()

    except (Exception, conn.DatabaseError) as error:
        print(error)
        conn.rollback()
        cur.close()
        return 1
    print(".csv metadata inserted")


def insert_raw_meta(loaded_row_count, loaded_at, dld_at, csv_url, file, conn):
    sql = """update metadata.metadata set loaded_row_count = %s, loaded_at = %s, csv_dld_at = %s, csv_url = %s where 
    csv_name = %s;"""
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (loaded_row_count, loaded_at, dld_at, csv_url, file))
            conn.commit()

    except (Exception, conn.DatabaseError) as error:
        print(error)
        conn.rollback()
        cur.close()
        return 1
    print("raw metadata inserted")


def get_ingested_csv(conn):
    sql = """select csv_name from metadata.metadata"""
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            result = cur.fetchall()

    except (Exception, conn.DatabaseError) as error:
        print(error)
        conn.rollback()
        cur.close()
        return 1
    return result
