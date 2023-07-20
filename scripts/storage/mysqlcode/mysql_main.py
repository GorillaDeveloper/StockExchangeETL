import pandas as pd
from sqlalchemy import create_engine, DDL, MetaData, Table, Column, Integer, String, Float
from sqlalchemy.orm import sessionmaker
import scripts.storage.mysqlcode.queries_and_procedures as qprocedures
# Your DataFrame (assuming you have it stored in 'your_dataframe')

# Configuration for MySQL container
host = "172.20.0.2"
port = 3306
user = "root"
password = "root"
database_name = "psx_data"

engine = None
connection = None
metadata = MetaData()

def create_tables():
    global metadata
    qprocedures.create_market_summary_table(metadata)

def connect():
    global engine, metadata
    connection_str = f"mysql+mysqlconnector://{user}:{password}@{host}:{port}"
    engine = create_engine(connection_str)

    connection = engine.connect()
    create_database_ddl = DDL(f"CREATE DATABASE IF NOT EXISTS {database_name}")
    connection.execute(create_database_ddl)
    connection.close()

    engine = create_engine(f"mysql+mysqlconnector://{user}:{password}@{host}:{port}/{database_name}")

    # Create the table if it doesn't exist
    metadata.create_all(engine)

def insert_dataframe(df, table_name):
    global engine

    # Connect to the MySQL database and create the table if not already created
    connect()

    # Insert DataFrame data into the table using 'append' mode
    Session = sessionmaker(bind=engine)
    session = Session()
    try:
        df.to_sql(name=table_name, con=engine, index=False, if_exists='append')
        session.commit()
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()

def close_connection():
    # global connection
    engine.dispose()
    # connection.close()

