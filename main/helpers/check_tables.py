from db_connection import DatabaseConnection

def main():
    conn = DatabaseConnection()
    
    # Test connection first
    if not conn.test_connection():
        print("Failed to connect to database")
        return
    
    print("Successfully connected to database")
    
    # Query to get all tables
    query = """
    SELECT owner, table_name 
    FROM all_tables 
    WHERE owner IN ('SYSTEM', 'SYS', 'OUTLN', 'DBSNMP', 'APPQOSSYS', 'AUDSYS', 'CTXSYS', 'DVSYS', 'GSMADMIN_INTERNAL', 'LBACSYS', 'MDSYS', 'OJVMSYS', 'OLAPSYS', 'ORDDATA', 'ORDSYS', 'REMOTE_SCHEDULER_AGENT', 'WMSYS', 'XDB')
    ORDER BY owner, table_name
    """
    
    try:
        df = conn.query_to_df(query)
        if df.empty:
            print("No tables found in the database")
        else:
            print("\nTables in the database:")
            print(df.to_string())
    except Exception as e:
        print(f"Error querying tables: {str(e)}")

if __name__ == "__main__":
    main() 