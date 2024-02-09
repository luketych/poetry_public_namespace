import math
import psycopg2


def batch_insert(conn, table_name, pd_df, batch_size=100):
    dict_li = [row.to_dict() for _, row in pd_df.iterrows()]
    columns = pd_df.columns

    insert_statement = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(['%s']*len(columns))})"
    
    try:
        with conn.cursor() as cursor:
            total_rows = len(dict_li)
            
            # Calculate the number of batches needed
            num_batches = math.ceil(total_rows / batch_size)

            for i in range(num_batches):
                start_idx = i * batch_size
                end_idx = (i + 1) * batch_size

                # Extract a batch of rows
                batch_dict_li = dict_li[start_idx:end_idx]

                # Execute the batch INSERT statement
                cursor.executemany(insert_statement, [tuple(d.values()) for d in batch_dict_li])

                # Commit the changes
                conn.commit()

                print(f"Inserted batch {i + 1}/{num_batches} ({len(batch_dict_li)} rows) into {table_name}")

            print(f"Successfully inserted {total_rows} rows into {table_name}")
    except psycopg2.Error as e:
        print(f"Error inserting data: {e}")