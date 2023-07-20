from sqlalchemy import create_engine, DDL, MetaData, Table, Column, Integer, String, Float

def create_market_summary_table(metadata):

    # Define the table schema using the Table object
    your_table = Table(
        'market_summary',
        metadata,
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('DATE_TIME', String(20)),
        Column('COMPANY_SYMBOL_NAME', String(50)),
        Column('COMPANY_CODE', String(10)),
        Column('COMPANY_NAME', String(100)),
        Column('OPEN_RATE', Float),
        Column('HIGHEST_RATE', Float),
        Column('LOWEST_RATE', Float),
        Column('LAST_RATE', Float),
        Column('TURN_OVER', Integer),
        Column('PREVIOUS_DAY_RATE', Float)
    )
    return your_table