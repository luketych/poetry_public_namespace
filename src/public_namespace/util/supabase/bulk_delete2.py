

def bulk_delete2(supabase_client, table_name, hash_col, hashes_to_remove, batch_size=100):
  
    # chop hashes_to_remove into batches of batch_size:
    batches = [hashes_to_remove[i:i + batch_size] for i in range(0, len(hashes_to_remove), batch_size)]
  
    resps = []
    for batch in batches:
        resp = supabase_client.table(table_name).delete().in_(hash_col, batch).execute()
        
        resps.append(resp)
  
    
    return resps