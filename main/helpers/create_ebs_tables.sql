-- Drop existing tables if they exist
BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE addresses CASCADE CONSTRAINTS';
EXCEPTION
   WHEN OTHERS THEN
      IF SQLCODE != -942 THEN
         RAISE;
      END IF;
END;
/

BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE assignments CASCADE CONSTRAINTS';
EXCEPTION
   WHEN OTHERS THEN
      IF SQLCODE != -942 THEN
         RAISE;
      END IF;
END;
/

-- Create EBS schema and tables
CREATE USER ebs_user IDENTIFIED BY ebs_password;
GRANT CREATE SESSION, CREATE TABLE, CREATE SEQUENCE TO ebs_user;
ALTER USER ebs_user QUOTA UNLIMITED ON USERS;

-- Create addresses table
CREATE TABLE addresses (
    unique_id VARCHAR2(36) PRIMARY KEY,
    worker_unique_id VARCHAR2(36) NOT NULL,
    address_type VARCHAR2(50),
    address_line1 VARCHAR2(200),
    address_line2 VARCHAR2(200),
    city VARCHAR2(100),
    state VARCHAR2(100),
    postal_code VARCHAR2(20),
    country VARCHAR2(100),
    primary_flag CHAR(1),
    effective_from DATE,
    effective_to DATE,
    created_date TIMESTAMP DEFAULT SYSTIMESTAMP,
    last_updated_date TIMESTAMP DEFAULT SYSTIMESTAMP
);

-- Create assignments table
CREATE TABLE assignments (
    unique_id VARCHAR2(36) PRIMARY KEY,
    worker_unique_id VARCHAR2(36) NOT NULL,
    assignment_type VARCHAR2(50),
    assignment_number VARCHAR2(50),
    effective_from DATE,
    effective_to DATE,
    created_date TIMESTAMP DEFAULT SYSTIMESTAMP,
    last_updated_date TIMESTAMP DEFAULT SYSTIMESTAMP
);

-- Create communications table
CREATE TABLE communications (
    unique_id VARCHAR2(36) PRIMARY KEY,
    worker_unique_id VARCHAR2(36) NOT NULL,
    contact_type VARCHAR2(50),
    contact_value VARCHAR2(200),
    primary_flag CHAR(1),
    effective_from DATE,
    effective_to DATE,
    created_date TIMESTAMP DEFAULT SYSTIMESTAMP,
    last_updated_date TIMESTAMP DEFAULT SYSTIMESTAMP
);

-- Create workers table
CREATE TABLE workers (
    unique_id VARCHAR2(36) PRIMARY KEY,
    person_id VARCHAR2(50),
    employee_number VARCHAR2(50),
    first_name VARCHAR2(100),
    last_name VARCHAR2(100),
    birth_date DATE,
    sex CHAR(1),
    marital_status CHAR(1),
    nationality VARCHAR2(2),
    effective_from DATE,
    effective_to DATE,
    created_date TIMESTAMP DEFAULT SYSTIMESTAMP,
    last_updated_date TIMESTAMP DEFAULT SYSTIMESTAMP
);

-- Create indexes
CREATE INDEX idx_addresses_worker_id ON addresses(worker_unique_id);
CREATE INDEX idx_assignments_worker_id ON assignments(worker_unique_id);
CREATE INDEX idx_communications_worker_id ON communications(worker_unique_id);
CREATE INDEX idx_workers_employee_number ON workers(employee_number);

-- Create triggers for last_updated_date
CREATE OR REPLACE TRIGGER trg_addresses_update
BEFORE UPDATE ON addresses
FOR EACH ROW
BEGIN
    :NEW.last_updated_date := SYSTIMESTAMP;
END;
/

CREATE OR REPLACE TRIGGER trg_assignments_update
BEFORE UPDATE ON assignments
FOR EACH ROW
BEGIN
    :NEW.last_updated_date := SYSTIMESTAMP;
END;
/

CREATE OR REPLACE TRIGGER trg_communications_update
BEFORE UPDATE ON communications
FOR EACH ROW
BEGIN
    :NEW.last_updated_date := SYSTIMESTAMP;
END;
/

CREATE OR REPLACE TRIGGER trg_workers_update
BEFORE UPDATE ON workers
FOR EACH ROW
BEGIN
    :NEW.last_updated_date := SYSTIMESTAMP;
END;
/

-- Create sequence for unique IDs
CREATE SEQUENCE ebs_unique_id_seq
    START WITH 1
    INCREMENT BY 1
    NOCACHE
    NOCYCLE; 