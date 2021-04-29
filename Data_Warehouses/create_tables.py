import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Delete exists tables
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Creating staging tables, fact table and dimension tables
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Connect to Redshift Cluster and create staging table and data warehouse table
    """
    
    #Load parameters
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    #Get connection to Redshift Cluster
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print(conn)

    #delete tables
    print("droping tables...")
    drop_tables(cur, conn)
    
    #create tables
    print("creating tables...")
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()