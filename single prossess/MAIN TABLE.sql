-- Use the email_recavaring database
USE email_recavaring;

-- Create the main table for user information if it doesn't exist
CREATE TABLE IF NOT EXISTS main_table (
    uuid CHAR(36) PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(100) NOT NULL,
    phone_number VARCHAR(15) NOT NULL,
    date_of_birth DATE NOT NULL
);
/*
-- Create the address table with a foreign key to the main table if it doesn't exist
CREATE TABLE IF NOT EXISTS address (
    id INT AUTO_INCREMENT PRIMARY KEY,
    uuid CHAR(36),
    street VARCHAR(100) NOT NULL,
    city VARCHAR(50) NOT NULL,
    state VARCHAR(50) NOT NULL,
    postal_code VARCHAR(20) NOT NULL,
    country VARCHAR(50) NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT FALSE,
    FOREIGN KEY (uuid) REFERENCES main_table(uuid)
);

-- Create a table for contract types with constraints to ensure only one type is true at a time if it doesn't exist
CREATE TABLE IF NOT EXISTS contract_type (
    uuid CHAR(36) PRIMARY KEY,
    BtB BOOLEAN NOT NULL DEFAULT FALSE,
    BtC BOOLEAN NOT NULL DEFAULT FALSE,
    CONSTRAINT CHK_ContractType CHECK (BtB <> BtC)
);
*/
