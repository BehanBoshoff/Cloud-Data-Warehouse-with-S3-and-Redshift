import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


"""
    COPIES DATA FROM S3 INTO STAGING TABLES 
"""
def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


"""
    EXTRACTS DATA FROM STAGING TABLES AND INSERTS INTO STAR SCHEMA
"""
def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()