"""
Database utility functions for Oracle EBS connection and operations
"""

import os
import cx_Oracle
from typing import Dict, List, Any, Optional
from ..config.oracle_config import ORACLE_CONFIG

class OracleConnection:
    def __init__(self, config: Optional[Dict[str, str]] = None):
        """
        Initialize Oracle connection with configuration
        
        Args:
            config: Optional dictionary containing connection parameters
                   If None, uses default config from oracle_config.py
        """
        self.config = config or ORACLE_CONFIG
        self.connection = None
        self._setup_oracle_client()
    
    def _setup_oracle_client(self) -> None:
        """Setup Oracle client environment"""
        # Set Oracle client library path
        oracle_client_path = os.getenv('ORACLE_CLIENT_PATH')
        if oracle_client_path:
            cx_Oracle.init_oracle_client(lib_dir=oracle_client_path)
    
    def connect(self) -> None:
        """Establish connection to Oracle database"""
        try:
            dsn = cx_Oracle.makedsn(
                self.config['host'],
                self.config['port'],
                service_name=self.config['service_name']
            )
            
            self.connection = cx_Oracle.connect(
                user=self.config['user'],
                password=self.config['password'],
                dsn=dsn
            )
            print(f"Successfully connected to Oracle database at {self.config['host']}")
            
        except cx_Oracle.Error as error:
            print(f"Error connecting to Oracle database: {error}")
            raise
    
    def disconnect(self) -> None:
        """Close database connection"""
        if self.connection:
            self.connection.close()
            print("Database connection closed")
    
    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        Execute a SQL query and return results
        
        Args:
            query: SQL query string
            params: Optional dictionary of query parameters
            
        Returns:
            List of dictionaries containing query results
        """
        if not self.connection:
            self.connect()
            
        try:
            cursor = self.connection.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
                
            # Get column names
            columns = [col[0] for col in cursor.description]
            
            # Fetch results
            results = []
            for row in cursor:
                results.append(dict(zip(columns, row)))
                
            return results
            
        except cx_Oracle.Error as error:
            print(f"Error executing query: {error}")
            raise
        finally:
            cursor.close()
    
    def execute_many(self, query: str, params_list: List[Dict[str, Any]]) -> None:
        """
        Execute a SQL query multiple times with different parameters
        
        Args:
            query: SQL query string
            params_list: List of parameter dictionaries
        """
        if not self.connection:
            self.connect()
            
        try:
            cursor = self.connection.cursor()
            cursor.executemany(query, params_list)
            self.connection.commit()
            
        except cx_Oracle.Error as error:
            print(f"Error executing batch query: {error}")
            self.connection.rollback()
            raise
        finally:
            cursor.close()
    
    def get_table_metadata(self, table_name: str) -> Dict[str, Any]:
        """
        Get metadata for a specific table
        
        Args:
            table_name: Name of the table
            
        Returns:
            Dictionary containing table metadata
        """
        query = """
        SELECT column_name, data_type, data_length, nullable, column_id
        FROM all_tab_columns
        WHERE table_name = :table_name
        ORDER BY column_id
        """
        
        results = self.execute_query(query, {'table_name': table_name})
        return {row['COLUMN_NAME']: row for row in results}
    
    def get_active_workers(self, effective_date: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get list of active workers as of a specific date
        
        Args:
            effective_date: Optional date string in YYYY-MM-DD format
                          If None, uses current date
                          
        Returns:
            List of dictionaries containing worker information
        """
        if not effective_date:
            effective_date = "TRUNC(SYSDATE)"
        else:
            effective_date = f"TO_DATE('{effective_date}', 'YYYY-MM-DD')"
            
        query = f"""
        SELECT 
            p.PERSON_ID,
            p.EMPLOYEE_NUMBER,
            p.FIRST_NAME,
            p.LAST_NAME,
            p.SEX,
            p.DATE_OF_BIRTH,
            p.NATIONAL_IDENTIFIER,
            p.CURRENT_EMPLOYEE_FLAG,
            a.ASSIGNMENT_ID,
            a.ASSIGNMENT_TYPE,
            a.ASSIGNMENT_STATUS_TYPE_ID,
            a.BUSINESS_GROUP_ID,
            a.ORGANIZATION_ID,
            a.JOB_ID,
            a.POSITION_ID,
            a.GRADE_ID
        FROM PER_ALL_PEOPLE_F p
        JOIN PER_ALL_ASSIGNMENTS_F a
            ON p.PERSON_ID = a.PERSON_ID
        WHERE {effective_date} BETWEEN p.EFFECTIVE_START_DATE AND p.EFFECTIVE_END_DATE
        AND {effective_date} BETWEEN a.EFFECTIVE_START_DATE AND a.EFFECTIVE_END_DATE
        AND p.CURRENT_EMPLOYEE_FLAG = 'Y'
        AND a.ASSIGNMENT_STATUS_TYPE_ID = 1  -- Active assignment status
        """
        
        return self.execute_query(query)
    
    def get_worker_addresses(self, person_ids: List[int], effective_date: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get addresses for specified workers
        
        Args:
            person_ids: List of PERSON_ID values
            effective_date: Optional date string in YYYY-MM-DD format
                          If None, uses current date
                          
        Returns:
            List of dictionaries containing address information
        """
        if not effective_date:
            effective_date = "TRUNC(SYSDATE)"
        else:
            effective_date = f"TO_DATE('{effective_date}', 'YYYY-MM-DD')"
            
        query = f"""
        SELECT 
            a.ADDRESS_ID,
            a.PERSON_ID,
            a.ADDRESS_TYPE,
            a.ADDRESS_LINE1,
            a.CITY,
            a.POSTAL_CODE,
            a.COUNTRY
        FROM PER_ADDRESSES_F a
        WHERE a.PERSON_ID IN ({','.join(map(str, person_ids))})
        AND {effective_date} BETWEEN a.EFFECTIVE_START_DATE AND a.EFFECTIVE_END_DATE
        """
        
        return self.execute_query(query)
    
    def get_worker_communications(self, person_ids: List[int], effective_date: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get contact information for specified workers
        
        Args:
            person_ids: List of PERSON_ID values
            effective_date: Optional date string in YYYY-MM-DD format
                          If None, uses current date
                          
        Returns:
            List of dictionaries containing contact information
        """
        if not effective_date:
            effective_date = "TRUNC(SYSDATE)"
        else:
            effective_date = f"TO_DATE('{effective_date}', 'YYYY-MM-DD')"
            
        query = f"""
        SELECT 
            c.CONTACT_ID,
            c.PERSON_ID,
            c.CONTACT_TYPE,
            c.CONTACT_VALUE,
            c.PRIMARY_FLAG
        FROM PER_CONTACTS_F c
        WHERE c.PERSON_ID IN ({','.join(map(str, person_ids))})
        AND {effective_date} BETWEEN c.EFFECTIVE_START_DATE AND c.EFFECTIVE_END_DATE
        """
        
        return self.execute_query(query) 