from io import StringIO


def copyFromStringIO(conn, table_name, pd_df):
    pd_df.insert(0, 'created_at', '1970-01-01T00:00:00Z')
    pd_df.insert(1, 'updated_at', '1970-01-01T00:00:00Z') 
  
    output = StringIO()
    pd_df.to_csv(output, sep=',', header=False, index=False)
    output.seek(0)
    
    print("First 3 rows of StringIO:")
    print(output.getvalue()[:output.getvalue().index('\n') * 3])
    
    #print(pd_df)


    with conn.cursor() as cursor:
        cursor.copy_from(output, table_name, null='')
        conn.commit()