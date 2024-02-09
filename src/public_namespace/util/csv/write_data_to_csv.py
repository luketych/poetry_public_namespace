import csv
import os
 
        
def write_data_to_csv(data, output_dir, output_file):
    # Check if the output directory exists, create it if it doesn't
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # Write the data to a new file in the output directory
    with open(os.path.join(output_dir, output_file), 'w', newline='') as csv_output:
        if data:
            writer = csv.DictWriter(csv_output, fieldnames=data[0].keys())
            writer.writeheader()
            writer.writerows(data)
            print(f"File {output_file} has been written to {output_dir}")
        else:
            print("No data to write.")