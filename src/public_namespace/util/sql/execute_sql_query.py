def execute_sql_query(conn, query):
    with conn.cursor() as cursor:
        cursor.execute(query)