# PostgreSQL Docker Setup and Data Import Cheat Sheet

## Prerequisites
- Docker Desktop installed on your Mac
- CSV data files in the following format:
  - workers.csv
  - addresses.csv
  - assignments.csv
  - communications.csv

## Step 1: Start Docker
1. Open Docker Desktop on your Mac
2. Wait for Docker to be running (whale icon in menu bar should be active)

## Step 2: Pull PostgreSQL Image
```bash
docker pull postgres:latest
```

## Step 3: Create PostgreSQL Container
```bash
docker run --name postgres-db \
  -e POSTGRES_PASSWORD=mysecretpassword \
  -d \
  -p 5432:5432 \
  postgres:latest
```

## Step 4: Create Database Tables
Connect to PostgreSQL and create the tables:

```sql
-- Create Workers Table
CREATE TABLE workers (
    UNIQUE_ID UUID PRIMARY KEY,
    PERSON_ID VARCHAR(50),
    EMPLOYEE_NUMBER VARCHAR(50),
    FIRST_NAME VARCHAR(100),
    LAST_NAME VARCHAR(100),
    BIRTH_DATE DATE,
    SEX CHAR(1),
    MARITAL_STATUS CHAR(1),
    NATIONALITY CHAR(2),
    EFFECTIVE_FROM DATE,
    EFFECTIVE_TO DATE
);

-- Create Addresses Table
CREATE TABLE addresses (
    UNIQUE_ID UUID PRIMARY KEY,
    WORKER_UNIQUE_ID UUID REFERENCES workers(UNIQUE_ID),
    ADDRESS_TYPE VARCHAR(50),
    ADDRESS_LINE1 VARCHAR(255),
    ADDRESS_LINE2 VARCHAR(255),
    CITY VARCHAR(100),
    STATE VARCHAR(100),
    POSTAL_CODE VARCHAR(20),
    COUNTRY CHAR(2),
    EFFECTIVE_FROM DATE,
    EFFECTIVE_TO DATE
);

-- Create Assignments Table
CREATE TABLE assignments (
    UNIQUE_ID UUID PRIMARY KEY,
    WORKER_UNIQUE_ID UUID REFERENCES workers(UNIQUE_ID),
    ASSIGNMENT_NUMBER VARCHAR(50),
    POSITION_ID VARCHAR(50),
    DEPARTMENT_ID VARCHAR(50),
    LOCATION_ID VARCHAR(50),
    EFFECTIVE_FROM DATE,
    EFFECTIVE_TO DATE
);

-- Create Communications Table
CREATE TABLE communications (
    UNIQUE_ID UUID PRIMARY KEY,
    WORKER_UNIQUE_ID UUID REFERENCES workers(UNIQUE_ID),
    CONTACT_TYPE VARCHAR(50),
    CONTACT_VALUE VARCHAR(255),
    PRIMARY_FLAG CHAR(1),
    EFFECTIVE_FROM DATE,
    EFFECTIVE_TO DATE
);
```

You can execute these commands using:
```bash
docker exec -i postgres-db psql -U postgres -d postgres
```

## Step 5: Import Data from CSV Files

1. Copy CSV files to container:
```bash
docker cp workers.csv postgres-db:/workers.csv
docker cp addresses.csv postgres-db:/addresses.csv
docker cp assignments.csv postgres-db:/assignments.csv
docker cp communications.csv postgres-db:/communications.csv
```

2. Import data using COPY command:
```bash
docker exec -i postgres-db psql -U postgres -d postgres -c "\COPY workers FROM '/workers.csv' WITH CSV HEADER;"
docker exec -i postgres-db psql -U postgres -d postgres -c "\COPY addresses FROM '/addresses.csv' WITH CSV HEADER;"
docker exec -i postgres-db psql -U postgres -d postgres -c "\COPY assignments FROM '/assignments.csv' WITH CSV HEADER;"
docker exec -i postgres-db psql -U postgres -d postgres -c "\COPY communications FROM '/communications.csv' WITH CSV HEADER;"
```

## Useful Commands

### Container Management
- Start container: `docker start postgres-db`
- Stop container: `docker stop postgres-db`
- Remove container: `docker rm postgres-db`
- Check container status: `docker ps`

### Database Access
- Connect to psql: `docker exec -it postgres-db psql -U postgres -d postgres`
- Execute single query: `docker exec -i postgres-db psql -U postgres -d postgres -c "SELECT * FROM workers LIMIT 5;"`

### Data Verification
```sql
-- Check record counts
SELECT 
    'workers' as table_name, COUNT(*) as record_count FROM workers
UNION ALL
SELECT 'addresses', COUNT(*) FROM addresses
UNION ALL
SELECT 'assignments', COUNT(*) FROM assignments
UNION ALL
SELECT 'communications', COUNT(*) FROM communications;

-- Sample joined data
SELECT w.first_name, w.last_name, 
       a.city, a.state, 
       c.contact_type, c.contact_value,
       asg.assignment_number, asg.position_id
FROM workers w
JOIN addresses a ON w.unique_id = a.worker_unique_id
JOIN communications c ON w.unique_id = c.worker_unique_id
JOIN assignments asg ON w.unique_id = asg.worker_unique_id
LIMIT 3;
```

## Troubleshooting

1. If container won't start:
   - Check if port 5432 is already in use
   - Ensure Docker Desktop is running
   - Check Docker logs: `docker logs postgres-db`

2. If data import fails:
   - Verify CSV file format and encoding
   - Check for proper line endings (LF vs CRLF)
   - Ensure CSV headers match table column names

## Data Backup
To backup your database:
```bash
docker exec -t postgres-db pg_dumpall -c -U postgres > dump_$(date +%Y-%m-%d_%H-%M-%S).sql
```

To restore from backup:
```bash
cat your_dump.sql | docker exec -i postgres-db psql -U postgres
``` 