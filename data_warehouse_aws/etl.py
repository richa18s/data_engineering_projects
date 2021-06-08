"""
    Author:
        Udacity, RichaS
    Date Created:
        06/02/2021
    Description:
        Script usage (console): 
         - python etl.py
        Tasks:
         - Establish connection to AWS redshift
         - Process the song and log data sets (in JSON) available in s3 bucket
           and load the data in staging tables
         - Populates fact and dimension tables in star schema from staging tables
"""

import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    This method gets log and song data set from s3 bucket and populates the data into staging tables
    Args:
        cur: Database cursor
        conn: Database connection
    Returns: 
        None
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    This method loads data from staging to fact and dimension tables
    Args:
        cur: Database cursor
        conn: Database connection
    Returns: 
        None
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Driving method to invoke load_staging_tables, insert_tables 
    Args:
        None
        
    Returns: 
        None
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()