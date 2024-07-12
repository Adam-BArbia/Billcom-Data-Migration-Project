#["example.com", "test.com", "sample.tn","example.tn", "test.tn", "sample.tn","gamil.com","ieee.org"]
import mysql.connector
import argparse
import yaml
from modules.database import connect_to_mysql
from modules.logging_config import configure_logging
from random import randint, choice
import string
import logging as log

def random_email(valid=True):
    if valid:
        domains = ["example.com", "test.com", "sample.tn","example.tn", "test.tn", "sample.tn","gamil.com","ieee.org"]
        return ''.join(choice(string.ascii_lowercase) for i in range(10)) + '@' + choice(domains)
    else:
        return ''.join(choice(string.ascii_lowercase) for i in range(10)) + choice(string.ascii_letters)

def random_phone(valid=True):
    if valid:
        return ''.join(choice(string.digits) for i in range(8))
    else:
        return ''.join(choice(string.digits) for i in range(7)) + choice(string.ascii_letters)

def random_dob(valid=True):
    if valid:
        year = randint(1970, 2006)
        month = randint(1, 12)
        day = randint(1, 28)
        return f"{year:04d}-{month:02d}-{day:02d}"
    else:
        return f"{randint(1970, 2000):04d}-{randint(13, 99):02d}-{randint(32, 99):02d}"

def main(config_path, N):
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    configure_logging(config)
    log.info("Starting data migration script")
    
    try:
        conn = connect_to_mysql(config['mysql'])
        log.info("Connected to MySQL database")
    except mysql.connector.Error as err:
        log.error(f"Error connecting to MySQL: {err}")
        return
    
    try:
        cursor = conn.cursor()
        cursor.execute("USE email_recavaring")

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS custemer_source (
                id INT PRIMARY KEY AUTO_INCREMENT,
                name VARCHAR(100) NOT NULL,
                email VARCHAR(100) NOT NULL,
                phone_number VARCHAR(15) NOT NULL,
                date_of_birth VARCHAR(10) NOT NULL
            );
        """)
        cursor.execute("TRUNCATE TABLE custemer_source;")
        log.info("Table custemer_source created")
        
        for i in range(N):
            valid_entry = choice([True, False])
            email = random_email(valid=valid_entry)
            phone = random_phone(valid=valid_entry)
            dob = random_dob(valid=valid_entry)
            name = ''.join(choice(string.ascii_lowercase) for i in range(7))
            try:
                cursor.execute(
                    "INSERT INTO custemer_source (name, phone_number, date_of_birth, email) VALUES (%s, %s, %s, %s)",
                    (name, phone, dob, email)
                )
                log.info(f"Inserted record {i + 1}: {name}, {phone}, {dob}, {email}")
            except mysql.connector.Error as err:
                log.error(f"Error inserting record {i + 1}: {err}")
        
        conn.commit()
        cursor.close()
        conn.close()
        log.info("Data migration completed and database connection closed")
    except mysql.connector.Error as err:
        log.error(f"Database operation error: {err}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the data migration script with specified config file.")
    parser.add_argument('--config', type=str, required=True, help="Path to the configuration YAML file.")
    parser.add_argument('--repeat', type=int, required=True, help="Number of rows to insert into the table.")
    args = parser.parse_args()
    main(args.config, args.repeat)
