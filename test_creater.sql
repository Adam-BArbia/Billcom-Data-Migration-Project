USE email_recavaring;

-- Drop and recreate source_table
DROP TABLE IF EXISTS custemer_source;
CREATE TABLE custemer_source (
    CIN VARCHAR(12) NOT NULL PRIMARY KEY,  -- CIN as VARCHAR
    name VARCHAR(45),
    phone_number VARCHAR(15),
    date_of_birth VARCHAR(10),
    email VARCHAR(80)
);

-- Insert data into source_table, including invalid entries
INSERT INTO custemer_source (CIN, name, phone_number, date_of_birth, email)
VALUES
('12910364', 'jhon macfy', '12345678', '2004-04-10', 'jhonmachy12@gmail.com'),         -- valid
('12910363', 'anna gel', '87654321', '2000-05-11', 'billcom_consulting@gmail.com'),   -- valid
('12910362', 'rachel hammigton', '13572468', '2001-06-12', 'billcom_consultinggmail.com'),  -- valid
('12910360', 'billcom consulting', '98765432', '2014-10-10', 'billcom_consulting@gmailcom'),  -- valid
('12910361', 'michael jones', '11112222', '1995-12-25', 'michael.jones@example.com'),  -- valid
('12910365', 'susan smith', '55556666', '1980-08-15', 'susan.smith@example.com'),     -- valid
('12910366', 'alice brown', '77778888', '2002-03-20', 'alice.brown@example.com'),     -- valid
('12910367', 'peter parker', '99990000', '1975-06-30', 'peter.parker@example.com'),   -- valid
('12345678', 'invalid name', 'invalid', 'invalid', 'invalid.email'),      -- invalid
('87654321', 'invalid person', '1234', '2000-13-01', 'invalid@example'),              -- invalid
('00000000', 'empty person', '', '1999-01-01', 'empty@example.com'); 
COMMIT;