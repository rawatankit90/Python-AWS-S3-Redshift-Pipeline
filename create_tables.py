import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    
    """ 
    The function drops the tables in database based on query
    
    Paramters :
    
    cur : cursor of the database for which connection is established
    
    conn : connection object
    
    """

    
    print('drop table started')
    
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
        
    print('drop table completed')


def create_tables(cur, conn):

    """ 
    The function creates the tables in database based on query
    
    Paramters :
    
    cur : cursor of the database for which connection is established
    
    conn : connection object
    
    """
    
    print('create table started')
    
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
        
    print('Create table completed')


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
