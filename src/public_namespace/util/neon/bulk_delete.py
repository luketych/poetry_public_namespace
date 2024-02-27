import psycopg2


def bulk_delete(conn, table_name, hash_col, hashes_to_remove, batch_size=100):
    # Start a new transaction
    with conn.cursor() as cur:
        try:
            # Chop hashes_to_remove into batches of batch_size
            batches = [hashes_to_remove[i:i + batch_size] for i in range(0, len(hashes_to_remove), batch_size)]
            
            for batch in batches:
                # Format a list of string placeholders for the SQL query
                placeholders = ','.join(['%s'] * len(batch))
                # Prepare the SQL DELETE query
                delete_query = f"DELETE FROM {table_name} WHERE {hash_col} IN ({placeholders})"
                # Execute the DELETE query
                cur.execute(delete_query, tuple(batch))
            
            # Commit the transaction
            conn.commit()
            
            # If you want to return the number of deleted rows, you can capture it here
            # You can return something more meaningful depending on your needs
            return hashes_to_remove
            
        except Exception as e:
            # Rollback the transaction if any exception occurs
            conn.rollback()
            raise e
