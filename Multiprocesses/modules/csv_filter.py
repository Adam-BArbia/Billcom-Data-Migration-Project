import os
import pandas as pd
import yaml
import logging as log
import multiprocessing

def delete_files_with_prefix(directory, prefix):
    # Get list of all files in the directory
    file_list = os.listdir(directory)
    files_to_delete = [os.path.join(directory, f) for f in file_list if f.startswith(prefix)]

    # Delete each file in the list
    for file_path in files_to_delete:
        try:
            if os.path.isfile(file_path):
                os.remove(file_path)
                log.info(f"Deleted file: {file_path}")
            else:
                log.info(f"{file_path} is not a file.")
        except Exception as e:
            log.info(f"Failed to delete {file_path}: {e}")

def filter_csv_file(file):
    log.info(f"Processing file: {file}")
    # Read the CSV file into a DataFrame
    df = pd.read_csv(file)
    
    # Filter rows where email_correction_status, phone_correction_status, or dob_correction_status is "non-correctable" or "correctable"
    filtered_df = df[
        (df['email_correction_status'].isin(['non-correctable', 'correctable'])) |
        (df['phone_correction_status'].isin(['non-correctable', 'correctable'])) |
        (df['dob_correction_status'].isin(['non-correctable', 'correctable']))
    ]
    
    return filtered_df

def filter_csv_files(input_files, output_file, num_processes):
    with multiprocessing.Pool(processes=num_processes) as pool:
        # Process CSV files in parallel
        results = pool.map(filter_csv_file, input_files)
    
    # Combine the results
    combined_df = pd.concat(results, ignore_index=True)
    
    # Save the combined DataFrame to a new CSV file
    combined_df.to_csv(output_file, index=False)
    log.info(f"Output file saved as: {output_file}")

def load_yaml(yaml_file):
    with open(yaml_file, 'r') as file:
        return yaml.safe_load(file)

#   *  .  . *       *    .        .        .   *    ..
# .    *        .   ###     .      .        .            *
#    *.   *        #####   .     *      *        *    .
#  ____       *  ######### *    .  *      .        .  *   .
# /   /\  .     ###\#|#/###   ..    *    .      *  .  ..  *
#/___/  ^8/      ###\|/###  *    *            .      *   *
#|   ||%%(        # }|{  #
#|___|,  \\  ejm    }|{
#Home is where the stars align.

def process_files(task_queue):
    while not task_queue.empty():
        try:
            task_type, file = task_queue.get_nowait()
            if task_type == 'delete':
                delete_files_with_prefix(*file)
            elif task_type == 'filter':
                filtered_df = filter_csv_file(file)
                task_queue.put(('append', filtered_df))
            elif task_type == 'append':
                # Append the filtered DataFrame to the final output
                filtered_df.to_csv('filtered_output.csv', mode='a', header=False, index=False)
            task_queue.task_done()
        except multiprocessing.queues.Empty:
            continue

def lauch_filter():
    log.info(f"Filtering started")
    # Load input directory and output file from YAML
    yaml_data = load_yaml('config.yaml')
    input_directory = yaml_data['input_directory']
    output_file = yaml_data['output_cvs']
    num_processes = yaml_data['etl'].get('processes', multiprocessing.cpu_count())

    # Create a queue for tasks
    task_queue = multiprocessing.JoinableQueue()

    # Add delete tasks for extract_chunk files
    task_queue.put(('delete', (input_directory, 'extract_chunk_')))

    # List all CSV files in the input directory and add filter tasks
    input_files = [os.path.join(input_directory, f) for f in os.listdir(input_directory) if f.startswith('transform_chunk_') and f.endswith('.csv')]
    for file in input_files:
        task_queue.put(('filter', file))

    # Create and start worker processes
    processes = []
    for _ in range(num_processes):
        p = multiprocessing.Process(target=process_files, args=(task_queue,))
        processes.append(p)
        p.start()

    # Wait for all tasks to be done
    task_queue.join()

    # Add delete tasks for transform_chunk files
    task_queue.put(('delete', (input_directory, 'transform_chunk_')))

    # Signal workers to exit
    for p in processes:
        p.join()

    log.info("All tasks completed")

if __name__ == "__main__":
    lauch_filter()