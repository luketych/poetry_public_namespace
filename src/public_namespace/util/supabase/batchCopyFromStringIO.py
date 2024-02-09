from io import StringIO

 
def batchCopyFromStringIO(conn, table_name, pd_df, chunk_size=1000):
    pd_df.insert(0, 'created_at', '1970-01-01T00:00:00Z')
    pd_df.insert(1, 'updated_at', '1970-01-01T00:00:00Z')
    
  
    output = StringIO()
    pd_df.to_csv(output, sep=',', header=False, index=False)
    output.seek(0)
    
    #print(output.getvalue()[:output.getvalue().index('\n') * 3])
    
    print("Number of Columns:", len(pd_df.columns))
    print(pd_df.columns)
    
    print("First row:")
    print(pd_df.iloc[1])
    print(output.getvalue()[:output.getvalue().index('\n') * 2])


    # Read StringIO line by line in chunks and perform COPY operation
    lines = output.readlines(chunk_size)
    while lines:
        #chunk_str = ''.join(lines)
        chunk_str = '\n'.join(lines)
        
        with conn.cursor() as cursor:
            cursor.copy_from(StringIO(chunk_str), table_name, sep=',', null='')
            conn.commit()
        lines = output.readlines(chunk_size)

    print(f"Successfully inserted {len(pd_df)} rows into {table_name}")




# def batch_copy_from_stringio(conn, table_name, pd_df, batch_size=1000):
#     # Remove the 'created_at' and 'updated_at' columns from the DataFrame
#     pd_df = pd_df.drop(['created_at', 'updated_at'], axis=1, errors='ignore')
    
#     print(pd_df['camp_id'].head())


#     total_rows = len(pd_df)
#     start = 0

#     while start < total_rows:
#         end = min(start + batch_size, total_rows)

#         batch_df = pd_df.iloc[start:end]

#         output = StringIO()
#         batch_df.to_csv(output, sep=',', header=False, index=False)
#         output.seek(0)
        
#         print(output.getvalue()[:output.getvalue().index('\n') * 3])

#         print(f"Inserting rows {start + 1} to {end}...")

#         with conn.cursor() as cursor:
#             cursor.copy_from(output, table_name, null='', columns=batch_df.columns)
#             conn.commit()

#         start = end

#     print("Insertion complete.")

 
        
# def batch_copy_from_stringio(conn, table_name, pd_df, batch_size=1000):
#     for start in range(0, len(pd_df), batch_size):
#         end = start + batch_size
#         batch_df = pd_df.iloc[start:end]
#         batch_copy_from_stringio(conn, table_name, batch_df)