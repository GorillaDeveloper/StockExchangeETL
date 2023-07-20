import mysql.connector

def check_database_exists(host, port, user, password, database_name):
    mydb = mysql.connector.connect(
        host=host,
        port=port,
        user=user,
        password=password
    )
    cursor = mydb.cursor()
    cursor.execute("SHOW DATABASES")
    databases = cursor.fetchall()
    for db in databases:
        if db[0] == database_name:
            cursor.close()
            mydb.close()
            return True
    cursor.close()
    mydb.close()
    return False

def create_database(host, port, user, password, database_name):
    mydb = mysql.connector.connect(
        host=host,
        port=port,
        user=user,
        password=password
    )
    cursor = mydb.cursor()
    cursor.execute(f"CREATE DATABASE {database_name}")
    mydb.commit()
    cursor.close()
    mydb.close()

def connect_to_database(host, port, user, password, database_name):
    mydb = mysql.connector.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database_name
    )
    return mydb

def execute_query(connection, query):
    cursor = connection.cursor()
    cursor.execute(query)
    connection.commit()
    cursor.close()

def execute_select(connection, query):
    cursor = connection.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    cursor.close()
    return result

def execute_procedure(connection, procedure_name, args=None):
    cursor = connection.cursor()
    if args:
        cursor.callproc(procedure_name, args)
    else:
        cursor.callproc(procedure_name)
    result = []
    for result_set in cursor.stored_results():
        result.extend(result_set.fetchall())
    cursor.close()
    return result

def create_table(connection, query):
    execute_query(connection, query)
    print("Table created successfully.")

def select_data(connection, query):
    result = execute_select(connection, query)
    for row in result:
        print(row)

def insert_data(connection, query):
    execute_query(connection, query)
    print("Data inserted successfully.")

def update_data(connection, query):
    execute_query(connection, query)
    print("Data updated successfully.")

def delete_data(connection, query):
    execute_query(connection, query)
    print("Data deleted successfully.")
