-- Setup script for MVP customer database
-- This creates the producer database and tables that match the ecosystem model schema

-- Create the database (run this as postgres superuser)
-- Note: You may need to run this part separately: createdb customer_db

-- Connect to the customer_db database
\c customer_db;

-- Drop tables if they exist (for clean setup)
DROP TABLE IF EXISTS addresses CASCADE;
DROP TABLE IF EXISTS customers CASCADE;

-- Create customers table
CREATE TABLE customers (
    id VARCHAR(20) PRIMARY KEY,
    firstName VARCHAR(100) NOT NULL,
    lastName VARCHAR(100) NOT NULL,
    dob DATE NOT NULL,
    email VARCHAR(100),
    phone VARCHAR(100),
    primaryAddressId VARCHAR(20),
    billingAddressId VARCHAR(20)
);

-- Create addresses table
CREATE TABLE addresses (
    id VARCHAR(20) PRIMARY KEY,
    customerId VARCHAR(20) NOT NULL,
    streetName VARCHAR(100) NOT NULL,
    city VARCHAR(100) NOT NULL,
    state VARCHAR(100) NOT NULL,
    zipCode VARCHAR(30) NOT NULL
);

-- Add foreign key constraints
ALTER TABLE addresses 
ADD CONSTRAINT fk_addresses_customer 
FOREIGN KEY (customerId) REFERENCES customers(id) ON DELETE CASCADE;

ALTER TABLE customers 
ADD CONSTRAINT fk_customers_primary_address 
FOREIGN KEY (primaryAddressId) REFERENCES addresses(id) ON DELETE SET NULL;

ALTER TABLE customers 
ADD CONSTRAINT fk_customers_billing_address 
FOREIGN KEY (billingAddressId) REFERENCES addresses(id) ON DELETE SET NULL;

-- Insert sample data for testing
-- Note: We need to insert customers first, then addresses, then update customer address references

-- Insert customers (without address references initially)
INSERT INTO customers (id, firstName, lastName, dob, email, phone) VALUES
    ('CUST001', 'John', 'Smith', '1985-03-15', 'john.smith@email.com', '+1-555-0101'),
    ('CUST002', 'Jane', 'Johnson', '1990-07-22', 'jane.johnson@email.com', '+1-555-0102'),
    ('CUST003', 'Mike', 'Wilson', '1982-11-08', 'mike.wilson@email.com', '+1-555-0103'),
    ('CUST004', 'Sarah', 'Davis', '1995-05-12', 'sarah.davis@email.com', '+1-555-0104'),
    ('CUST005', 'Tom', 'Brown', '1988-09-30', 'tom.brown@email.com', '+1-555-0105');

-- Insert addresses
INSERT INTO addresses (id, customerId, streetName, city, state, zipCode) VALUES
    -- John Smith's addresses
    ('ADDR001', 'CUST001', '123 Main Street', 'New York', 'NY', '10001'),
    ('ADDR002', 'CUST001', '456 Business Ave', 'New York', 'NY', '10002'),
    
    -- Jane Johnson's addresses  
    ('ADDR003', 'CUST002', '789 Oak Lane', 'Los Angeles', 'CA', '90210'),
    ('ADDR004', 'CUST002', '321 Pine Street', 'Los Angeles', 'CA', '90211'),
    
    -- Mike Wilson's addresses
    ('ADDR005', 'CUST003', '555 Elm Drive', 'Chicago', 'IL', '60601'),
    
    -- Sarah Davis's addresses
    ('ADDR006', 'CUST004', '777 Maple Court', 'Houston', 'TX', '77001'),
    ('ADDR007', 'CUST004', '888 Cedar Way', 'Houston', 'TX', '77002'),
    
    -- Tom Brown's addresses
    ('ADDR008', 'CUST005', '999 Birch Boulevard', 'Phoenix', 'AZ', '85001');

-- Update customers with their address references
UPDATE customers SET primaryAddressId = 'ADDR001', billingAddressId = 'ADDR002' WHERE id = 'CUST001';
UPDATE customers SET primaryAddressId = 'ADDR003', billingAddressId = 'ADDR003' WHERE id = 'CUST002';
UPDATE customers SET primaryAddressId = 'ADDR005', billingAddressId = 'ADDR005' WHERE id = 'CUST003';
UPDATE customers SET primaryAddressId = 'ADDR006', billingAddressId = 'ADDR007' WHERE id = 'CUST004';
UPDATE customers SET primaryAddressId = 'ADDR008', billingAddressId = 'ADDR008' WHERE id = 'CUST005';

-- Create indexes for better performance
CREATE INDEX idx_addresses_customer_id ON addresses(customerId);
CREATE INDEX idx_customers_email ON customers(email);

-- Grant permissions (adjust as needed for your DataSurface user)
-- GRANT SELECT ON ALL TABLES IN SCHEMA public TO datasurface_user;

-- Display summary
SELECT 'Database setup complete!' as message;
SELECT COUNT(*) as customer_count FROM customers;
SELECT COUNT(*) as address_count FROM addresses;

-- Show sample data
SELECT 'Sample customers:' as info;
SELECT id, firstName, lastName, email FROM customers LIMIT 3;

SELECT 'Sample addresses:' as info;  
SELECT id, customerId, streetName, city, state FROM addresses LIMIT 3; 