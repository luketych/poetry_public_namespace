from supabase import create_client, Client
from termcolor import colored


def insert(data: dict, table_name: str, supabase: Client):
    try:
        data, count = supabase.table(table_name).insert(data).execute()
        print(data)
    except Exception as e:
        if e.code == '23505':  # DUPLICATE KEY
            print(colored("(23505) DUPLICATE KEY", "yellow"))
            print(colored(e.details, "yellow"))
        else:
            print(e)
            exit(1)