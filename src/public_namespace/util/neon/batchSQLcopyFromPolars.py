import io
from io import StringIO
import polars as pl
import psycopg2
from termcolor import colored


def batchSQLcopyFromPolars(conn, table_name, df, chunk_size=1000):
    if not isinstance(df, pl.DataFrame):
      raise TypeError("Input dataframe must be a Polars DataFrame.")
      
    pg_cursor = conn.cursor()

    # Prepare DataFrame for COPY
    df.insert_column(0, 'created_at', '1970-01-01T00:00:00Z')
    df.insert_column(1, 'updated_at', '1970-01-01T00:00:00Z')

    # Write DataFrame to StringIO
    output = StringIO()
    df.write_csv(output, sep=',', header=False, index=False)
    output.seek(0)

    # Read StringIO line by line in chunks and perform COPY operation
    lines = output.readlines(chunk_size)
    while lines:
        # Check if this is the last iteration

        chunk_str = ''.join(lines)
        
        # remove the last \n
        chunk_str = chunk_str[:-1]

        try:
            with conn.cursor() as cursor:
                cursor.copy_from(StringIO(chunk_str), table_name, sep=',', null='')
                conn.commit()
                
                # remove the last line from the chunk
        except (Exception, psycopg2.DatabaseError) as error:
            print(colored(f"Error: {error}", 'red'))
            conn.rollback()
            
        finally:
            lines = output.readlines(chunk_size)

    print(f"Finished running batchSQLcopyFromPolars(): {len(df)} rows into {table_name}")

    pg_cursor.close()
    
    
    return conn