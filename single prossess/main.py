import argparse
import yaml
from moduls.database import connect_to_mysql, execute_sql_script, validate_and_insert_data
from moduls.logging_config import configure_logging

def main(config_path):
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)

    configure_logging(config)

    # Connect to MySQL
    conn = connect_to_mysql(config['mysql'])
    if conn:
        # Execute SQL script to create tables
        execute_sql_script(conn, config)

        # Validate and insert data from source table
        validate_and_insert_data(conn, config)

        # Close connection
        conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the data migration script with specified config file.")
    parser.add_argument('--config', type=str, required=True, help="Path to the configuration YAML file.")
    args = parser.parse_args()
    main(args.config)
    
    
"""                  .
                    / V\
                  / `  /
                 <<   |
                 /    |
               /      |
             /        |
           /    \  \ /
          (      ) | |
  ________|   _/_  | |
<__________\______)\__)
"""