import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Load data to staging tables from S3 using COPY command
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Load data to fact and dimension tables from staging tables
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Load data to Redshift Cluster data warehouse from S3
    """
    
    # load parameters
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # get connection to Redshift Cluster
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print(conn)
    
    # load data to staging tables
    print("loading staging tables...")
    load_staging_tables(cur, conn)
    
    # load data to fact and dimension tables
    print("loading fact and dim tables...")
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()