import argparse
import yaml
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor, as_completed
import logging
from functools import partial
import os

from modules.database import connect_to_mysql
from modules.extract import extract_data_chunk
from modules.transform import transform_data_chunk
from modules.load import load_data_chunk
from modules.logging_config import configure_logging
from modules.csv_filter import lauch_filter

def get_total_rows(config):
    conn = connect_to_mysql(config['mysql'])
    cursor = conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM {config['mysql']['input']}")
    total_rows = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    return total_rows

def process_chunk(config, chunk_number, chunk_size):
    try:
        # Extract
        conn = connect_to_mysql(config['mysql'])
        extract_data_chunk(conn, config, chunk_number, chunk_size)
        conn.close()

        # Transform
        transform_data_chunk(config, chunk_number)

        # Load
        conn = connect_to_mysql(config['mysql'])
        load_data_chunk(conn, config, chunk_number)
        conn.close()

        return f"Chunk {chunk_number} processed successfully"
    except Exception as e:
        return f"Error processing chunk {chunk_number}: {str(e)}"

def main(config_path):
    # Load configuration
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)

    # Configure logging
    configure_logging(config)
    logger = logging.getLogger(__name__)

    # Calculate chunk size and number of chunks
    total_rows = get_total_rows(config)
    chunk_size = total_rows // config['etl']['chunk_number']
    num_chunks = config['etl']['chunk_number']

    # Determine number of processes
    num_processes = min(config['etl'].get('processes', mp.cpu_count()), num_chunks)

    logger.info(f"Starting ETL process with {num_processes} processes for {num_chunks} chunks")

    # Create a partial function with the config and chunk_size already applied
    process_chunk_partial = partial(process_chunk, config, chunk_size=chunk_size)

    # Run the ETL process using ProcessPoolExecutor
    with ProcessPoolExecutor(max_workers=num_processes) as executor:
        futures = [executor.submit(process_chunk_partial, i) for i in range(num_chunks)]
        
        for future in as_completed(futures):
            result = future.result()
            logger.info(result)

    logger.info("ETL process completed")

    # Run the filter function after all ETL processes are done
    logger.info("Starting filtering process")
    lauch_filter()
    logger.info("Filtering process completed")

    # Cleanup: remove intermediate files
    cleanup(config['paths']['csv_output_directory'])

def cleanup(directory):
    logger = logging.getLogger(__name__)
    for filename in os.listdir(directory):
        if filename.startswith(('extract_chunk_', 'transform_chunk_')):
            os.remove(os.path.join(directory, filename))
            logger.info(f"Removed temporary file: {filename}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the data migration script with specified config file.")
    parser.add_argument('--config', type=str, required=True, help="Path to the configuration YAML file.")
    args = parser.parse_args()
    
    main(args.config)