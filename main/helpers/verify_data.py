from db_connection import DatabaseConnection

def verify_data():
    conn = DatabaseConnection()
    
    # Query to check addresses
    addresses_query = """
    SELECT COUNT(*) as count, 
           COUNT(DISTINCT worker_unique_id) as unique_workers,
           COUNT(DISTINCT address_type) as address_types
    FROM addresses
    """
    
    # Query to check assignments
    assignments_query = """
    SELECT COUNT(*) as count,
           COUNT(DISTINCT worker_unique_id) as unique_workers,
           COUNT(DISTINCT assignment_number) as unique_assignments
    FROM assignments
    """
    
    # Query to check data sample
    sample_query = """
    SELECT a.worker_unique_id, 
           a.address_type,
           a.city,
           a.state,
           asg.assignment_number,
           asg.position_id
    FROM addresses a
    JOIN assignments asg ON a.worker_unique_id = asg.worker_unique_id
    WHERE ROWNUM <= 5
    """
    
    try:
        # Check addresses
        print("\nAddresses Summary:")
        addresses_df = conn.query_to_df(addresses_query)
        print(addresses_df)
        
        # Check assignments
        print("\nAssignments Summary:")
        assignments_df = conn.query_to_df(assignments_query)
        print(assignments_df)
        
        # Check sample data
        print("\nSample Data:")
        sample_df = conn.query_to_df(sample_query)
        print(sample_df)
        
    except Exception as e:
        print(f"Error verifying data: {str(e)}")

if __name__ == "__main__":
    verify_data() 