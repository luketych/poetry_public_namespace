import psycopg2


def bulk_insert(conn, table_name, pd_df):
    dict_li = [row.to_dict() for _, row in pd_df.iterrows()]
    columns = pd_df.columns

    insert_statement = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(['%s']*len(columns))})"
    
    try:
        with conn.cursor() as cursor:
            cursor.executemany(insert_statement, [tuple(d.values()) for d in dict_li])
            conn.commit()
            print(f"Successfully inserted {len(dict_li)} rows into {table_name}")
    except psycopg2.Error as e:
        print(f"Error inserting data: {e}")