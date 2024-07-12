import mysql.connector
import logging as log
import pandas as pd
from modules.uuid_generator import UUID_generator

def load_data_chunk(conn, config, chunk_number):
    try:
        chunk_path = f"{config['paths']['csv_output_directory']}transform_chunk_{chunk_number}.csv"
        df = pd.read_csv(chunk_path)
        
        cursor = conn.cursor()
        for _, row in df.iterrows():
            cursor.execute("""
                INSERT INTO main_table (uuid, name, email, phone_number, date_of_birth)
                VALUES (%s, %s, %s, %s, %s)
            """, (UUID_generator(), row['name'], row['valid_email'], row['valid_phone'], row['valid_dob']))
        
        conn.commit()
        log.info(f"Loaded chunk {chunk_number} into the database")
    except mysql.connector.Error as e:
        log.error(f"Error loading data: {e}")
        raise


#           Ya...___|__..aab     .   .
#            Y88a  Y88o  Y88a   (     )
#             Y88b  Y88b  Y88b   `.oo'
#             :888  :888  :888  ( (`-'
#    .---.    d88P  d88P  d88P   `.`.
#   / .-._)  d8P'"""|"""'-Y8P      `.`.
#  ( (`._) .-.  .-. |.-.  .-.  .-.   ) )
#   \ `---( O )( O )( O )( O )( O )-' /
#    `.    `-'  `-'  `-'  `-'  `-'  .' CJ
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#       "Up with the Jolly Roger boys,
#          And off we go to sea.
#          There's heaps of fun
#        When the Jolly Roger's hung,
#        And the wind is on the lee."
#         - C.F. Chudleigh Candish