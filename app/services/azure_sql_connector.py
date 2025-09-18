import os
import logging
import asyncio
from typing import List, Dict, Any, Optional, Union
from contextlib import asynccontextmanager
import pyodbc
from azure.identity import DefaultAzureCredential, ManagedIdentityCredential
from azure.core.exceptions import AzureError
import struct


class AzureSQLConnector:
    """
    Universal Azure SQL Database connector supporting both traditional SQL Server
    and Azure SQL Database with various authentication methods.
    """
    
    def __init__(self, 
                 server: str,
                 database: str,
                 username: Optional[str] = None,
                 password: Optional[str] = None,
                 use_managed_identity: bool = False,
                 use_azure_ad: bool = False,
                 connection_timeout: int = 30,
                 command_timeout: int = 30):
        """
        Initialize Azure SQL connector.
        
        Args:
            server: SQL Server hostname (e.g., 'myserver.database.windows.net')
            database: Database name
            username: SQL Server username (if using SQL authentication)
            password: SQL Server password (if using SQL authentication)
            use_managed_identity: Use Azure Managed Identity authentication
            use_azure_ad: Use Azure AD authentication
            connection_timeout: Connection timeout in seconds
            command_timeout: Command timeout in seconds
        """
        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.use_managed_identity = use_managed_identity
        self.use_azure_ad = use_azure_ad
        self.connection_timeout = connection_timeout
        self.command_timeout = command_timeout
        
        # Initialize logger
        self.logger = logging.getLogger(__name__)
        
        # Connection pool settings
        self.pool = None
        self.connection_string = self._build_connection_string()
        
    def _get_available_driver(self) -> str:
        """Get available ODBC driver for SQL Server"""
        available_drivers = pyodbc.drivers()
        
        # Priority order of drivers to try
        preferred_drivers = [
            "ODBC Driver 18 for SQL Server",
            "ODBC Driver 17 for SQL Server", 
            "ODBC Driver 13 for SQL Server",
            "SQL Server"
        ]
        
        for driver in preferred_drivers:
            if driver in available_drivers:
                self.logger.info(f"Using ODBC driver: {driver}")
                return driver
                
        # Fallback to first available driver containing "SQL Server"
        for driver in available_drivers:
            if "SQL Server" in driver:
                self.logger.warning(f"Using fallback driver: {driver}")
                return driver
                
        raise Exception(f"No suitable SQL Server ODBC driver found. Available drivers: {available_drivers}")

    def _build_connection_string(self) -> str:
        """Build connection string based on authentication method."""
        
        # Get available driver
        driver = self._get_available_driver()
        
        # Base connection string
        # Try ODBC Driver 18 first, fallback to 17 if not available
        base_conn_str = (
            f"DRIVER={{{driver}}};"
            f"SERVER={self.server};"
            f"DATABASE={self.database};"
            f"Encrypt=yes;"
            f"TrustServerCertificate=no;"
            f"Connection Timeout={self.connection_timeout};"
            f"Command Timeout={self.command_timeout};"
        )
        
        if self.use_managed_identity:
            # Use Managed Identity
            conn_str = base_conn_str + "Authentication=ActiveDirectoryMsi;"
            self.logger.info("Using Managed Identity authentication")
            
        elif self.use_azure_ad:
            # Use Azure AD Interactive authentication
            conn_str = base_conn_str + "Authentication=ActiveDirectoryInteractive;"
            self.logger.info("Using Azure AD Interactive authentication")
            
        elif self.username and self.password:
            # Use SQL Server authentication
            conn_str = base_conn_str + f"UID={self.username};PWD={self.password};"
            self.logger.info("Using SQL Server authentication")
            
        else:
            # Use Windows authentication (default)
            conn_str = base_conn_str + "Trusted_Connection=yes;"
            self.logger.info("Using Windows authentication")
            
        return conn_str
    
    def test_connection(self) -> bool:
        """Test database connection."""
        try:
            with pyodbc.connect(self.connection_string) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                result = cursor.fetchone()
                self.logger.info("Database connection test successful")
                return result[0] == 1
                
        except Exception as e:
            self.logger.error(f"Database connection test failed: {str(e)}")
            return False
    
    @asynccontextmanager
    async def get_connection(self):
        """Get database connection with context manager."""
        connection = None
        try:
            # For async operations, we might need to use threading
            import concurrent.futures
            
            loop = asyncio.get_event_loop()
            with concurrent.futures.ThreadPoolExecutor() as executor:
                connection = await loop.run_in_executor(
                    executor, pyodbc.connect, self.connection_string
                )
                
            yield connection
            
        except Exception as e:
            self.logger.error(f"Failed to get database connection: {str(e)}")
            raise
        finally:
            if connection:
                connection.close()
    
    async def execute_query(self, 
                          query: str, 
                          parameters: Optional[Dict[str, Any]] = None,
                          fetch_results: bool = True) -> Optional[List[Dict[str, Any]]]:
        """
        Execute SQL query asynchronously.
        
        Args:
            query: SQL query to execute
            parameters: Query parameters
            fetch_results: Whether to fetch and return results
            
        Returns:
            Query results as list of dictionaries or None
        """
        try:
            async with self.get_connection() as conn:
                cursor = conn.cursor()
                
                if parameters:
                    cursor.execute(query, parameters)
                else:
                    cursor.execute(query)
                
                if fetch_results:
                    # Get column names
                    columns = [column[0] for column in cursor.description] if cursor.description else []
                    
                    # Fetch all results
                    rows = cursor.fetchall()
                    
                    # Convert to list of dictionaries
                    results = []
                    for row in rows:
                        row_dict = dict(zip(columns, row))
                        results.append(row_dict)
                    
                    self.logger.info(f"Query executed successfully, returned {len(results)} rows")
                    return results
                else:
                    # For INSERT, UPDATE, DELETE operations
                    conn.commit()
                    affected_rows = cursor.rowcount
                    self.logger.info(f"Query executed successfully, affected {affected_rows} rows")
                    return None
                    
        except Exception as e:
            self.logger.error(f"Query execution failed: {str(e)}")
            self.logger.error(f"Query: {query}")
            if parameters:
                self.logger.error(f"Parameters: {parameters}")
            raise
    
    async def execute_stored_procedure(self, 
                                     procedure_name: str, 
                                     parameters: Optional[Dict[str, Any]] = None) -> Optional[List[Dict[str, Any]]]:
        """
        Execute stored procedure.
        
        Args:
            procedure_name: Name of the stored procedure
            parameters: Procedure parameters
            
        Returns:
            Procedure results as list of dictionaries
        """
        try:
            # Build procedure call
            if parameters:
                param_placeholders = ', '.join(['?' for _ in parameters.values()])
                query = f"EXEC {procedure_name} {param_placeholders}"
                return await self.execute_query(query, list(parameters.values()))
            else:
                query = f"EXEC {procedure_name}"
                return await self.execute_query(query)
                
        except Exception as e:
            self.logger.error(f"Stored procedure execution failed: {str(e)}")
            raise
    
    async def bulk_insert(self, 
                         table_name: str, 
                         data: List[Dict[str, Any]],
                         batch_size: int = 1000) -> bool:
        """
        Perform bulk insert operation.
        
        Args:
            table_name: Target table name
            data: List of dictionaries containing data to insert
            batch_size: Number of records per batch
            
        Returns:
            Success status
        """
        try:
            if not data:
                self.logger.warning("No data provided for bulk insert")
                return True
            
            # Get column names from first record
            columns = list(data[0].keys())
            column_names = ', '.join(columns)
            placeholders = ', '.join(['?' for _ in columns])
            
            insert_query = f"INSERT INTO {table_name} ({column_names}) VALUES ({placeholders})"
            
            # Process data in batches
            total_inserted = 0
            
            async with self.get_connection() as conn:
                cursor = conn.cursor()
                
                for i in range(0, len(data), batch_size):
                    batch = data[i:i + batch_size]
                    
                    # Prepare batch data
                    batch_values = []
                    for record in batch:
                        values = [record.get(col) for col in columns]
                        batch_values.append(values)
                    
                    # Execute batch
                    cursor.executemany(insert_query, batch_values)
                    conn.commit()
                    
                    total_inserted += len(batch)
                    self.logger.info(f"Inserted batch: {len(batch)} records (Total: {total_inserted})")
            
            self.logger.info(f"Bulk insert completed successfully: {total_inserted} records")
            return True
            
        except Exception as e:
            self.logger.error(f"Bulk insert failed: {str(e)}")
            raise
    
    async def get_table_schema(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Get table schema information.
        
        Args:
            table_name: Name of the table
            
        Returns:
            List of column information
        """
        query = """
        SELECT 
            COLUMN_NAME,
            DATA_TYPE,
            IS_NULLABLE,
            CHARACTER_MAXIMUM_LENGTH,
            NUMERIC_PRECISION,
            NUMERIC_SCALE,
            COLUMN_DEFAULT
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = ?
        ORDER BY ORDINAL_POSITION
        """
        
        return await self.execute_query(query, [table_name])
    
    async def check_table_exists(self, table_name: str) -> bool:
        """
        Check if table exists in database.
        
        Args:
            table_name: Name of the table to check
            
        Returns:
            True if table exists, False otherwise
        """
        query = """
        SELECT COUNT(*) as table_count
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_NAME = ?
        """
        
        result = await self.execute_query(query, [table_name])
        return result[0]['table_count'] > 0 if result else False


# Factory function for easy connector creation
def create_azure_sql_connector_from_env() -> AzureSQLConnector:
    """
    Create Azure SQL connector using configuration from Config class (Key Vault).
    
    The Config class handles retrieving values from Key Vault with fallback to environment variables.
    """
    from app.config import Config
    
    server = Config.AZURE_SQL_SERVER
    database = Config.AZURE_SQL_DATABASE
    username = Config.AZURE_SQL_USERNAME
    password = Config.AZURE_SQL_PASSWORD
    use_managed_identity = Config.USE_MANAGED_IDENTITY
    use_azure_ad = Config.USE_AZURE_AD
    connection_timeout = Config.SQL_CONNECTION_TIMEOUT
    command_timeout = Config.SQL_COMMAND_TIMEOUT
    
    if not server or not database:
        raise ValueError("AZURE_SQL_SERVER and AZURE_SQL_DATABASE configuration are required")
    
    return AzureSQLConnector(
        server=server,
        database=database,
        username=username,
        password=password,
        use_managed_identity=use_managed_identity,
        use_azure_ad=use_azure_ad,
        connection_timeout=connection_timeout,
        command_timeout=command_timeout
    )


# Example usage and testing
if __name__ == "__main__":
    async def main():
        # Create connector from environment variables
        connector = create_azure_sql_connector_from_env()
        
        # Test connection
        if connector.test_connection():
            print("✅ Database connection successful!")
            
            # Example query
            results = await connector.execute_query("SELECT @@VERSION as version")
            if results:
                print(f"Database version: {results[0]['version']}")
            
        else:
            print("❌ Database connection failed!")
    
    # Run the example
    asyncio.run(main())