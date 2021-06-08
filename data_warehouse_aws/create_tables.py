"""
    Author:
        Udacity, RichaS
    Date Created:
        06/02/2021
    Description:
        Script usage (console): 
         - python create_tables.py
        Tasks:
         - Establish connection to AWS redshift
         - Drop the tables if exists
         - Create the tables from DDLS in sql_queries.py
"""

import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    This method drops the staging, fact and dimension tables if exists
    Args:
        cur: Database cursor
        conn: Database connection
    Returns: 
        None
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    This method creates the staging, fact and dimension tables
    Args:
        cur: Database cursor
        conn: Database connection
    Returns: 
        None
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Driving method to invoke drop_tables and create_tables method
    Args:
        None
    Returns: 
        None
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()