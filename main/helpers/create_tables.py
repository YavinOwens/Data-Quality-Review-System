from db_connection import DatabaseConnection

def create_tables():
    conn = DatabaseConnection()
    
    # SQL statements
    statements = [
        """
        CREATE TABLE addresses (
            unique_id VARCHAR2(36) PRIMARY KEY,
            worker_unique_id VARCHAR2(36) NOT NULL,
            address_type VARCHAR2(10) NOT NULL,
            address_line1 VARCHAR2(200),
            address_line2 VARCHAR2(200),
            city VARCHAR2(100),
            state VARCHAR2(100),
            postal_code VARCHAR2(20),
            country VARCHAR2(2),
            effective_from DATE,
            effective_to DATE,
            creation_date TIMESTAMP DEFAULT SYSTIMESTAMP,
            last_updated_date TIMESTAMP DEFAULT SYSTIMESTAMP
        )
        """,
        
        "CREATE INDEX idx_addr_worker_id ON addresses(worker_unique_id)",
        
        """
        CREATE TABLE assignments (
            unique_id VARCHAR2(36) PRIMARY KEY,
            worker_unique_id VARCHAR2(36) NOT NULL,
            assignment_number VARCHAR2(10),
            position_id VARCHAR2(10),
            department_id VARCHAR2(10),
            location_id VARCHAR2(10),
            effective_from DATE,
            effective_to DATE,
            creation_date TIMESTAMP DEFAULT SYSTIMESTAMP,
            last_updated_date TIMESTAMP DEFAULT SYSTIMESTAMP
        )
        """,
        
        "CREATE INDEX idx_asgn_worker_id ON assignments(worker_unique_id)",
        
        """
        CREATE OR REPLACE TRIGGER trg_addr_update
        BEFORE UPDATE ON addresses
        FOR EACH ROW
        BEGIN
            :NEW.last_updated_date := SYSTIMESTAMP;
        END;
        """,
        
        """
        CREATE OR REPLACE TRIGGER trg_asgn_update
        BEFORE UPDATE ON assignments
        FOR EACH ROW
        BEGIN
            :NEW.last_updated_date := SYSTIMESTAMP;
        END;
        """
    ]
    
    with conn.get_connection() as connection:
        cursor = connection.cursor()
        
        for statement in statements:
            try:
                cursor.execute(statement)
                print(f"Successfully executed: {statement[:100]}...")
            except Exception as e:
                print(f"Error executing statement: {statement}")
                print(f"Error: {str(e)}")
                raise
        
        connection.commit()
        print("All tables created successfully")

if __name__ == "__main__":
    create_tables() 