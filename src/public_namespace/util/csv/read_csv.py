import csv


def read_csv(input_file):
    data = []

    with open(input_file, 'r') as csv_file:
        reader = csv.DictReader(csv_file)
        for row in reader:
            data.append(row)
    
    return data