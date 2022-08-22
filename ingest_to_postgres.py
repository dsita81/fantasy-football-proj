import numpy as np
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from config import config

host_name = 'database-nfl-fantasy-stats.cqggy1q69qgf.us-east-1.rds.amazonaws.com'
dbname = 'database-nfl-fantasy-stats'
port = '5432'
username = 'postgres'
password = 'skins111'
conn = None


def connect_to_postgres():
    params = config()
    try:
        conn = psycopg2.connect(**params)
    except psycopg2.OperationalError as e:
        raise e
    else:
        print("Connected!")
    return conn
    
def create_table(curr, table, columns): 
    print('Creating new table....')

    create_script = '''CREATE TABLE IF NOT EXISTS {table_name} (
                        {table_columns}
                        )'''.format(table_name=table, table_columns=columns)
    #print(create_script)

    curr.execute(create_script)

    print("Table Created.")



def copy_csv_to_db():
    table_file = open(csv_file_name)
    print("File opened in memory...")
    copy_script = '''COPY {table_name} FROM STDIN WITH
        CSV
        HEADER
        DELIMITER AS ',' 
    '''.format(table_name=table)

    cur.copy_expert(sql=copy_script, file=table_file)
    print("File copied to database...")
    conn.commit()

    cur.close()
    #except (Exception, psycopg2.DatabaseError) as error:
    #    print(error)
    #finally:
    #    if conn is not None:
    #        conn.close()
    #    print('Database connection closed.')