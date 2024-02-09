import csv


def check_csv_for_dupes(csv_file_path, hash_column_name):
    hash_values = set()
    duplicates = []

    with open(csv_file_path, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            hash_value = row.get(hash_column_name)
            if hash_value is not None:
                if hash_value in hash_values:
                    duplicates.append(row)
                else:
                    hash_values.add(hash_value)

    return duplicates



# Example usage:
# csv_file_path = "./pipelines/google_daily/output_data/google_daily.csv"
# hash_column_name = "hash"
# duplicate_rows = check_csv_for_dupes(csv_file_path, hash_column_name)

# if duplicate_rows:
#     print("Duplicate rows found:")
#     for row in duplicate_rows:
#         print(row)
# else:
#     print("No duplicate rows found.")
