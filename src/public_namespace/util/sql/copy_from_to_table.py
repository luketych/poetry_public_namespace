# def copy_from_to_table(conn, source_table, destination_table):
#     with conn.cursor() as cursor:
#         # cursor.execute(f"INSERT INTO {destination_table} SELECT * FROM {source_table} ON CONFLICT DO NOTHING;")
#         cursor.execute(f"INSERT INTO {destination_table} SELECT * FROM {source_table} ON CONFLICT (hash) DO UPDATE SET * = EXCLUDED.* ;")
        
        
def copy_from_to_table(conn, source_table, destination_table):
    with conn.cursor() as cursor:
        cursor.execute(f"SELECT * FROM {source_table} LIMIT 0")
        column_names = [desc[0] for desc in cursor.description]
        column_names_str = ', '.join(column_names)
        
        conflict_columns = ', '.join(column_names)
        
        update_columns = ', '.join([f"{col} = EXCLUDED.{col}" for col in column_names])
        
        query = f"INSERT INTO {destination_table} ({column_names_str}) SELECT {column_names_str} FROM {source_table} ON CONFLICT (hash) DO UPDATE SET {update_columns};"
        cursor.execute(query)