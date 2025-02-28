# config.py
# returns a dictionary based on the provided section in the provided .ini file (defaults to database.ini and postgresql)
from configparser import ConfigParser
import os

AIRFLOW_HOME = os.getenv('AIRFLOW_HOME')


def config(filename=AIRFLOW_HOME + '/dags/database.ini', section='postgresql'):
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

# for debug purposes
if __name__ == '__main__':
    config()