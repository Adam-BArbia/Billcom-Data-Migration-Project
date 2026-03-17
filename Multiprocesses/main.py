import argparse
import yaml
import multiprocessing
import logging as log
import os
from modules.logging_config import configure_logging
from modules.extract import extract_data_chunk
from modules.transform import transform_data_chunk
from modules.load import load_data_chunk
from modules.database import connect_to_mysql
from modules.csv_filter import lauch_filter  

def get_total_rows(conn, config):
    cursor = conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM {config['mysql']['input']}")
    total_rows = cursor.fetchone()[0]
    cursor.close()
    return total_rows

def run_etl_process(task_queues, done_event, config, conn_pool, chunk_size):
    conn = None
    while not done_event.is_set():
        try:
            task_type, chunk_number = task_queues.get_nowait()
            if task_type == 'extract':
                if not conn:
                    conn = connect_to_mysql(config['mysql'])
                extract_data_chunk(conn, config, chunk_number, chunk_size)
            elif task_type == 'transform':
                transform_data_chunk(config, chunk_number)
            elif task_type == 'load':
                if not conn:
                    conn = connect_to_mysql(config['mysql'])
                load_data_chunk(conn, config, chunk_number)
            task_queues.task_done()
        except multiprocessing.queues.Empty:
            continue

    # Ensure connection is closed before the worker terminates
    if conn:
        conn.close()

def cleanup(directory):
    loger = log.getLogger(__name__)
    for filename in os.listdir(directory):
        if filename.startswith(('transform_chunk_')):
            os.remove(os.path.join(directory, filename))
            loger.info(f"Removed temporary file: {filename}")

#                 _  _
#                | )/ )
#             \\ |//,' __
#             (")(_)-"()))=-
#                (\\
#                             _   _
#  HEELP                     ( | / )
#                          \\ \|/,' __
#    \_o_/                 (")(_)-"()))=-
#       )                     <\\
#      /\__
#_____ \ ________________________________
#BUGS!!!!

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the data migration script with specified config file.")
    parser.add_argument('--config', type=str, required=True, help="Path to the configuration YAML file.")
    args = parser.parse_args()
    
    with open(args.config, 'r') as f:
        config = yaml.safe_load(f)
    
    configure_logging(config)
    
    # Number of processes
    num_processes = config['etl'].get('processes', (multiprocessing.cpu_count()-1))
    
    # Create task queues for each task type
    task_queues = multiprocessing.JoinableQueue()

    # Connect to MySQL to get the total number of rows
    conn = connect_to_mysql(config['mysql'])
    total_rows = get_total_rows(conn, config)
    conn.close()

    # Calculate chunk size
    chunk_size = total_rows // config['etl']['chunk_number']
    
    # Add tasks to the queue
    for chunk_number in range(config['etl']['chunk_number']):
        task_queues.put(('extract', chunk_number))
    for chunk_number in range(config['etl']['chunk_number']):
        task_queues.put(('transform', chunk_number))
    for chunk_number in range(config['etl']['chunk_number']):
        task_queues.put(('load', chunk_number))

    # Event to signal when all tasks are done
    done_event = multiprocessing.Event()

    # Create and start worker processes
    processes = []
    for _ in range(num_processes):
        p = multiprocessing.Process(target=run_etl_process, args=(task_queues, done_event, config, None, chunk_size))
        processes.append(p)
        p.start()

    # Wait for all tasks to be done
    task_queues.join()

    # Signal workers to exit
    done_event.set()

    # Wait for all worker processes to finish
    for p in processes:
        p.join()

    log.info("All tasks completed")
    # Run the filter function after all ETL processes are done
    log.info("Starting filtering process")
    # Run the filter function after all ETL processes are done
    lauch_filter()
    log.info("Filtering process completed")

    # Cleanup: remove intermediate files
    cleanup(config['paths']['csv_output_directory'])