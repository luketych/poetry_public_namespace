import psycopg2


def bulk_fetch(conn, table_name: str):
    try:
        with conn.cursor() as cursor:
            cursor.execute(f"SELECT * FROM {table_name}")
            
            # Get column names from cursor description
            header = [col[0] for col in cursor.description]
            
            rows = cursor.fetchall()
            print(f"Successfully fetched {len(rows)} rows from {table_name}")
            
            return header, rows
    except psycopg2.Error as e:
        print(f"Error fetching data: {e}")