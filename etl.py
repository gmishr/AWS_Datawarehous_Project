import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

'''
Below function is written to execute queries t
o load staging table placed in S3 Bucket
'''
def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()

'''
Below function is written to execute queries to 
load data from staging redshift table to final tables.
'''
def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

'''
Below is main function written to connect to redshift cluster and run
drop table and create table functions.
'''
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