import os
import pandas as pd
import yaml
import logging as log
import multiprocessing

def delete_files_with_prefix(directory, prefix):
    file_list = os.listdir(directory)
    files_to_delete = [os.path.join(directory, f) for f in file_list if f.startswith(prefix)]

    for file_path in files_to_delete:
        try:
            if os.path.isfile(file_path):
                os.remove(file_path)
                log.info(f"Deleted file: {file_path}")
            else:
                log.info(f"{file_path} is not a file.")
        except Exception as e:
            log.error(f"Failed to delete {file_path}: {e}")

def filter_csv_file(file):
    log.info(f"Processing file: {file}")
    df = pd.read_csv(file)

    filtered_df = df[
        (df['email_correction_status'].isin(['non-correctable', 'correctable'])) |
        (df['phone_correction_status'].isin(['non-correctable', 'correctable'])) |
        (df['dob_correction_status'].isin(['non-correctable', 'correctable']))
    ]
    
    return filtered_df

def filter_csv_files(input_files, output_file, num_processes):
    with multiprocessing.Pool(processes=num_processes) as pool:
        results = pool.map(filter_csv_file, input_files)

    combined_df = pd.concat(results, ignore_index=True)
    combined_df.to_csv(output_file, index=False)
    log.info(f"Output file saved as: {output_file}")

def load_yaml(yaml_file):
    with open(yaml_file, 'r') as file:
        return yaml.safe_load(file)

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
                filtered_df.to_csv('filtered_output.csv', mode='a', header=False, index=False)
            task_queue.task_done()
        except multiprocessing.queues.Empty:
            continue

def lauch_filter():
    log.info("Filtering started")
    yaml_data = load_yaml('config.yaml')
    input_directory = yaml_data['input_directory']
    output_file = yaml_data['output_cvs']
    num_processes = yaml_data['etl'].get('processes', multiprocessing.cpu_count())

    task_queue = multiprocessing.JoinableQueue()

    task_queue.put(('delete', (input_directory, 'extract_chunk_')))

    input_files = [os.path.join(input_directory, f) for f in os.listdir(input_directory) if f.startswith('transform_chunk_') and f.endswith('.csv')]
    for file in input_files:
        task_queue.put(('filter', file))

    processes = []
    for _ in range(num_processes):
        p = multiprocessing.Process(target=process_files, args=(task_queue,))
        processes.append(p)
        p.start()

    task_queue.join()

    task_queue.put(('delete', (input_directory, 'transform_chunk_')))

    for p in processes:
        p.join()

    log.info("All tasks completed")

if __name__ == "__main__":
    lauch_filter()
