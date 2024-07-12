import mysql.connector
import logging as log
import pandas as pd

def extract_data_chunk(conn, config, chunk_number, chunk_size):
    try:
        cursor = conn.cursor(dictionary=True)
        offset = chunk_number * chunk_size
        
        cursor.execute(f"SELECT * FROM {config['mysql']['input']} LIMIT {chunk_size} OFFSET {offset}")
        rows = cursor.fetchall()
        
        df = pd.DataFrame(rows)
        chunk_path = f"{config['paths']['csv_output_directory']}extract_chunk_{chunk_number}.csv"
        df.to_csv(chunk_path, index=False)
        
        log.info(f"Extracted chunk {chunk_number} to {chunk_path}")
    except mysql.connector.Error as e:
        log.error(f"Error extracting data: {e}")
        raise

#         ,
#       _=|_
#     _[_## ]_
#_  +[_[_+_]P/    _    |_       ____      _=--|-~
# ~---\_I_I_[=\--~ ~~--[o]--==-|##==]-=-~~  o]H
#-~ /[_[_|_]_]\\  -_  [[=]]    |====]  __  !j]H
#  /    "|"    \      ^U-U^  - |    - ~ .~  U/~
# ~~--__~~~--__~~-__   H_H_    |_     --   _H_
#-. _  ~~~#######~~~     ~~~-    ~~--  ._ - ~~-=
#           ~~~=~~  -~~--  _     . -      _ _ -
#
#       ----------------------------------
#      |        June, 21th, 1969          |
#      |             02:56GMT             |
#      |  Here Men from the Planet Earth  |
#      |   First set Foot upon the Moon   |
#      | We came in Peace for all Mankind |
#      ---------------------------=apx=--