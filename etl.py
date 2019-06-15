import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    
    """ 
    The function loads the staging table
    staging_events, staging_songs the tables
    in database based on copy_table_queries
    written in sql_queries
    
    Paramters :
    
    cur : cursor of the database for which connection is established
    
    conn : connection object
    
    """
    
    print('loading staging')
    
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()
        
    print('load staging completed')


def insert_tables(cur, conn):
    
    """ 
    The function loads the final table
    songplays, users, songs, artists, time
    in database based on insert_table_queries
    written in sql_queries
    
    Paramters :
    
    cur : cursor of the database for which connection is established
    
    conn : connection object
    
    """
    
    print('insert started')
    
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()
        
    print('insert completed')


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
