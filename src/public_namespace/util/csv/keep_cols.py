import csv


def keep_cols(input_file, output_file, columns_to_keep):
    with open(input_file, 'r') as input_csv, open(output_file, 'w', newline='') as output_csv:
        reader = csv.DictReader(input_csv)
        fieldnames = reader.fieldnames
        
        # Find columns to keep
        columns_to_remove = [column for column in fieldnames if column not in columns_to_keep]
        
        writer = csv.DictWriter(output_csv, fieldnames=columns_to_keep)
        writer.writeheader()
        
        for row in reader:
            for column_name in columns_to_remove:
                del row[column_name]
            writer.writerow(row)
    
    removed_columns = ', '.join(columns_to_remove)
    print(f"Columns {removed_columns} removed successfully. New CSV file saved as '{output_file}'.")




# Example usage:


# input_file = './pipelines/google_daily/data/merged/ad_stats_campaign_stats_ad_history.csv'
# output_file = './pipelines/google_daily/data/mutated/ad_stats_campaign_stats_ad_history_cleaned.csv'
# columns_to_keep = ['camp_id', 'date_x', 'ad_group_id_x', 'ad_id', 'campaign_id', 'clicks_x', 'conversions_x', 'conversions_value_x', 'cost_micros_x', 'cost_per_conversion', 'impressions_x', 'interactions_x', 'clicks_y', 'conversions_y', 'conversions_value_y', 'cost_micros_y', 'id_x', 'impressions_y', 'interactions_y', 'ad_group_id_y', 'id_y', 'final_urls']

# keep_cols(input_file, output_file, columns_to_keep)