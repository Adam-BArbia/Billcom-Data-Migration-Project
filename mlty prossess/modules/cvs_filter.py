import os
import pandas as pd
import yaml
import logging as log

def delete_files_in_directory(directory):
    # Get list of all files in the directory
    file_list = os.listdir(directory)

    # Iterate over all files and delete each one
    for file_name in file_list:
        file_path = os.path.join(directory, file_name)
        try:
            if os.path.isfile(file_path):
                os.remove(file_path)
                log.info(f"Deleted file: {file_path}")
            else:
                log.info(f"{file_path} is not a file.")
        except Exception as e:
            log.info(f"Failed to delete {file_path}: {e}")

def filter_csv_files(input_files, output_file):
    # Create an empty DataFrame to store the filtered rows
    combined_df = pd.DataFrame()

    # Iterate through each CSV file
    for file in input_files:
        log.info(f"Processing file: {file}")
        # Read the CSV file into a DataFrame
        df = pd.read_csv(file)
        
        # Filter rows where email_correction_status, phone_correction_status, or dob_correction_status is "non-correctable" or "correctable"
        filtered_df = df[
            (df['email_correction_status'].isin(['non-correctable', 'correctable'])) |
            (df['phone_correction_status'].isin(['non-correctable', 'correctable'])) |
            (df['dob_correction_status'].isin(['non-correctable', 'correctable']))
        ]
        
        # Append the filtered rows to the combined DataFrame
        combined_df = pd.concat([combined_df, filtered_df], ignore_index=True)

    # Save the combined DataFrame to a new CSV file
    combined_df.to_csv(output_file, index=False)
    log.info(f"Output file saved as: {output_file}")

#   *  .  . *       *    .        .        .   *    ..
# .    *        .   ###     .      .        .            *
#    *.   *        #####   .     *      *        *    .
#  ____       *  ######### *    .  *      .        .  *   .
# /   /\  .     ###\#|#/###   ..    *    .      *  .  ..  *
#/___/  ^8/      ###\|/###  *    *            .      *   *
#|   ||%%(        # }|{  #
#|___|,  \\  ejm    }|{
#Home is where the stars align.

def load_yaml(yaml_file):
    with open(yaml_file, 'r') as file:
        return yaml.safe_load(file)

def lauch_filter():
    log.info(f"Filtering started")
    # Load input directory and output file from YAML
    yaml_data = load_yaml('config.yaml')
    input_directory = yaml_data['input_directory']
    output_file = yaml_data['output_cvs']

    # List all CSV files in the input directory
    input_files = []
    for file in os.listdir(input_directory):
        if file.startswith('transform_chunk_') and file.endswith('.csv'):
            input_files.append(os.path.join(input_directory, file))

    # Call the function to filter and combine the CSV files
    filter_csv_files(input_files, output_file)

    delete_files_in_directory(yaml_data['input_directory'])