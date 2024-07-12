from modules.regex_validator import validate_and_correct_data_chunk
import logging as log

def transform_data_chunk(config, chunk_number):
    try:
        chunk_path = f"{config['paths']['csv_output_directory']}extract_chunk_{chunk_number}.csv"
        output_path = f"{config['paths']['csv_output_directory']}transform_chunk_{chunk_number}.csv"
        validate_and_correct_data_chunk(chunk_path, output_path)
        
        log.info(f"Transformed chunk {chunk_number} to {output_path}")
    except Exception as e:
        log.error(f"Error transforming data: {e}")
        raise

#          __ __
#        ,;::\::\
#      ,'/' `/'`/
#  _\,: '.,-'.-':.
# -./"'  :    :  :\/,
#  ::.  ,:____;__; :-
#  :"  ( .`-*'o*',);
#   \.. ` `---'`' /
#    `:._..-   _.'
#    ,;  .     `.
#   /"'| |       \
#  ::. ) :        :
#  |" (   \       |
#  :.(_,  :       ;
#   \'`-'_/      /
#    `...   , _,'
#     |,|  : |
#     |`|  | |
#     |,|  | |
# ,--.;`|  | '..--.
#/;' "' ;  '..--. ))
#\:.___(___   ) ))'
#       SSt`-'-''
#Procrastination level: Garfield.