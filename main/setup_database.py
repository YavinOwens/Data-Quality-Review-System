from helpers.db_connection import DatabaseConnection

def setup_database():
    try:
        # Create database connection
        db = DatabaseConnection()
        
        # Read and execute the SQL setup file
        with open('setup_database.sql', 'r') as file:
            sql_commands = file.read()
            
        # Execute each SQL command
        for command in sql_commands.split(';'):
            if command.strip():
                db.execute(command)
                
        print("Database setup completed successfully!")
        
    except Exception as e:
        print(f"Error setting up database: {str(e)}")
    finally:
        if 'db' in locals():
            db.close()

if __name__ == "__main__":
    setup_database() 