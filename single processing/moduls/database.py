import mysql.connector
import logging as log
import pandas as pd
from moduls.uuid_generator import UUID_generator
from moduls.regex_validator import validate_and_correct_data

def connect_to_mysql(mysql_config):
    try:
        conn = mysql.connector.connect(
            host=mysql_config['host'],
            user=mysql_config['user'],
            password=mysql_config['password'],
            database=mysql_config['database']
        )
        log.info("Connected to MySQL")
        return conn
    except mysql.connector.Error as e:
        log.error(f"Error connecting to MySQL: {e}")
        raise

def execute_sql_script(conn, config):
    try:
        sql_script_path = config['paths']['sql_script']
        with open(sql_script_path, 'r') as f:
            sql_commands = f.read().split(';')
            cursor = conn.cursor()
            for command in sql_commands:
                command = command.strip()
                if command:
                    cursor.execute(command)
                    log.info(f"Executed SQL command: {command}")
            conn.commit()
            log.info("SQL script executed successfully")
    except mysql.connector.Error as e:
        log.error(f"Error executing SQL script: {e}")
        raise

def validate_and_insert_data(conn, config):
    try:
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM custemer_source")
        rows = cursor.fetchall()

        df = pd.DataFrame(rows)

        # Validate and correct data using Pandas
        df, invalid_entries_correctable, invalid_entries_non_correctable = validate_and_correct_data(df)

        # Add UUID
        df['uuid'] = df.apply(lambda _: UUID_generator(), axis=1)

        # Insert valid data into main_table
        for _, row in df.iterrows():
            cursor.execute("""
                INSERT INTO main_table (uuid, name, email, phone_number, date_of_birth)
                VALUES (%s, %s, %s, %s, %s)
            """, (row['uuid'], row['name'], row['valid_email'], row['valid_phone'], row['valid_dob']))

        conn.commit()

        # Save invalid entries to CSV
        if not invalid_entries_correctable.empty or not invalid_entries_non_correctable.empty:
            csv_output_directory = config['paths']['csv_output_directory']

            if not invalid_entries_correctable.empty:
                correctable_csv_path = csv_output_directory + 'correctable_invalid_entries.csv'
                invalid_entries_correctable.to_csv(correctable_csv_path, index=False)
                log.info(f"Correctable invalid entries saved to {correctable_csv_path}")

            if not invalid_entries_non_correctable.empty:
                non_correctable_csv_path = csv_output_directory + 'non_correctable_invalid_entries.csv'
                invalid_entries_non_correctable.to_csv(non_correctable_csv_path, index=False)
                log.info(f"Non-correctable invalid entries saved to {non_correctable_csv_path}")

    except mysql.connector.Error as e:
        log.error(f"Error validating and inserting data: {e}")
        raise
