import csv
import os


# removing column _fivetran_id will also remove _fivetran_id_x and _fivetran_id_y
def remove_cols(input_file, output_file, columns_to_remove):
    with open(input_file, 'r') as input_csv, open(output_file, 'w', newline='') as output_csv:
        reader = csv.DictReader(input_csv)
        fieldnames = reader.fieldnames
        
        # Find columns with matching postfix
        columns_with_postfix = []
        for column in fieldnames:
            for column_to_remove in columns_to_remove:
                if column.startswith(column_to_remove):
                    columns_with_postfix.append(column)
        
        # Remove columns with matching postfix
        fieldnames = [column for column in fieldnames if column not in columns_with_postfix]
        
        writer = csv.DictWriter(output_csv, fieldnames=fieldnames)
        writer.writeheader()
        
        for row in reader:
            for column_name in columns_with_postfix:
                del row[column_name]
            writer.writerow(row)
    
    removed_columns = ', '.join(columns_with_postfix)
    print(f"Columns {removed_columns} removed successfully. New CSV file saved as '{output_file}'.")


# Example usage:

# input_file = './pipelines/google_daily/data/merged/ad_stats_campaign_stats_ad_history.csv'
# output_file = './pipelines/google_daily/data/mutated/ad_stats_campaign_stats_ad_history_cleaned.csv'
# columns_to_remove = ['_fivetran_id']

# remove_cols(input_file, output_file, columns_to_remove)