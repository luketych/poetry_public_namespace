import psycopg2


def check_table_exists(conn, table_name):
    try:
        # Check if the table exists in the database
        with conn.cursor() as cursor:
            cursor.execute(f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name}');")
            table_exists = cursor.fetchone()[0]

        return table_exists
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
      
      
# def check_table_exists(table_name):
#     command = f'docker exec grafana-postgres psql -U postgres -d postgres -c "\\dt" | grep {table_name}'
#     result = subprocess.run(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
#     output = result.stdout.decode('utf-8').strip()

#     return len(output) >