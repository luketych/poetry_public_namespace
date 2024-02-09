import psycopg2


def bulk_delete(conn, table_name: str, date_list: list = None):
    try:
        with conn.cursor() as cursor:
            if date_list:
                date_condition = " OR ".join([f"date = '{date}'" for date in date_list])
                where_clause = f"WHERE {date_condition}"

                cursor.execute(f"DELETE FROM {table_name} {where_clause}")
            else:
                cursor.execute(f"DELETE FROM {table_name}")

            num_rows_deleted = cursor.rowcount

            conn.commit()

            print(f"Successfully deleted {num_rows_deleted} rows from {table_name}")
    except psycopg2.Error as e:
        print(f"Error deleting data: {e}")