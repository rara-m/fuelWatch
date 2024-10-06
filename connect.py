from configparser import ConfigParser
import psycopg2


def load_config(filename='db.ini', section='postgresql'):
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)

    # get section and update dictionary with connection string key:value pairs
    db = {}
    if section in parser:
        for key in parser[section]:
            db[key] = parser[section][key]
    else:
        raise Exception(
            'Section {0} not found in the {1} file'.format(section, filename))
    return db


db = load_config()


def connect(config=db):
        """ Connect to the PostgreSQL database server """
        if config is None:
            config = db
        try:
            # connecting to the PostgreSQL server
            with psycopg2.connect(**config) as conn:
                print('Connected to the PostgreSQL server.')
                return conn
        except (psycopg2.DatabaseError, Exception) as error:
            print(error)


'''
# for debug purposes
if __name__ == '__main__':
    load_config()
'''