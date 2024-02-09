import csv
from io import StringIO


def split_csv_row(row):
    # Create a CSV reader using a StringIO buffer
    csv_reader = csv.reader(StringIO(row))

    # Extract the split data
    split_data = next(csv_reader)

    # Remove any leading or trailing whitespace
    split_data = [item.strip() for item in split_data]


    return split_data