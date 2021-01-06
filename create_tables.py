import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

'''
Below function is written to run drop table queries.
'''
def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
'''
Below function is written to run create table queries.
'''

def create_tables(cur, conn):
    for query in create_table_queries:
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

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()