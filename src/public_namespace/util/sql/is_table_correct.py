from collections import OrderedDict
import psycopg2
import re

from .check_table_exists import check_table_exists


# Define a mapping for equivalent data types
#  For some reason when we pull from the database DECIMAL will become nmumeric.
#  Edge Case: TEXT UNIQUE will become TEXT.
DATA_TYPE_EQUIVALENTS = {
    'NUMERIC': 'DECIMAL',
    'TEXT UNIQUE': 'TEXT',
    'VARCHAR(*)': 'CHARACTER VARYING',
    'VARCHAR(*) UNIQUE': 'CHARACTER VARYING',
    # Add more equivalent data types as needed
}


''' Capitalize all data types.
    For some reason when we pull from the database DECIMAL will become nmumeric.
    Edge Case: TEXT UNIQUE will become TEXT.
'''
def normalize_data_type(data_type):
    if 'VARCHAR' in data_type:
        # Replace int with *
        data_type = re.sub(r'VARCHAR\(\d+\)', 'VARCHAR(*)', data_type)
  
  
    # Normalize data type to standard format for comparison
    normalized_type = DATA_TYPE_EQUIVALENTS.get(data_type.upper(), data_type)
  
    return normalized_type.upper()
  

def is_table_correct(conn, table_name, columns):
    try:
        # Check if the table exists in the database
        with conn.cursor() as cursor:
            table_exists = check_table_exists(conn, table_name)

            if not table_exists:
                return False

            # If the table exists, check if its columns match the provided columns
            cursor.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table_name}';")
            existing_columns = cursor.fetchall()

            # Convert existing columns to an OrderedDict for easy comparison and sorting
            existing_columns_dict = OrderedDict(sorted({col[0]: normalize_data_type(col[1]) for col in existing_columns}.items()))

            # Convert the provided columns to an OrderedDict for easy comparison and sorting
            provided_columns_dict = OrderedDict(sorted({col[0]: normalize_data_type(col[1]) for col in columns}.items()))

            if existing_columns_dict != provided_columns_dict:
                return False

        # Close the connection
        conn.close()

        return True
      
    except psycopg2.InterfaceError:
        # Reconnect to the database and retry
        conn.close()
        # Extract the database connection parameters from the dsn string
        dsn_params = dict(param.split('=') for param in conn.dsn.split())

        # Reconnect with the correct password
        dsn_params['password'] = 'postgres'
        new_conn = psycopg2.connect(**dsn_params)
        new_conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

        # Retry the operation after reconnecting
        return check_table_exists(new_conn, table_name)
    except psycopg2.OperationalError:
        # Handle the case when the connection to the database cannot be established
        return False