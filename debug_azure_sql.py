#!/usr/bin/env python3
"""
Debug script for Azure SQL connectivity issues in Azure Functions
"""

import pyodbc
import sys

def main():
    """Debug Azure SQL connectivity"""
    
    print("=== AZURE SQL DEBUG ===")
    
    # 1. Check available ODBC drivers
    print("\n1. Available ODBC Drivers:")
    drivers = pyodbc.drivers()
    for i, driver in enumerate(drivers, 1):
        print(f"   {i}. {driver}")
    
    if not drivers:
        print("   No ODBC drivers found!")
        return
    
    # 2. Find SQL Server drivers
    print("\n2. SQL Server Drivers:")
    sql_drivers = [d for d in drivers if "SQL Server" in d]
    if sql_drivers:
        for driver in sql_drivers:
            print(f"   ✅ {driver}")
    else:
        print("   ❌ No SQL Server drivers found!")
        return
    
    # 3. Test configuration
    print("\n3. Configuration Test:")
    
    try:
        from app.config import Config
        config = Config.get_azure_sql_config()
        
        print(f"   Server: {config.get('server')}")
        print(f"   Database: {config.get('database')}")
        print(f"   Username: {'***' if config.get('username') else 'None'}")
        print(f"   Password: {'***' if config.get('password') else 'None'}")
        print(f"   Use Managed Identity: {config.get('use_managed_identity')}")
        print(f"   Use Azure AD: {config.get('use_azure_ad')}")
        
    except Exception as e:
        print(f"   ❌ Configuration error: {e}")
        return
    
    # 4. Test connection
    print("\n4. Connection Test:")
    
    try:
        from app.services.azure_sql_connector import AzureSQLConnector
        
        connector = AzureSQLConnector(**config)
        print(f"   Connection string: {connector.connection_string.replace(config.get('password', ''), '***') if config.get('password') else connector.connection_string}")
        
        # Test connection
        if connector.test_connection():
            print("   ✅ Connection successful!")
        else:
            print("   ❌ Connection failed!")
            
    except Exception as e:
        print(f"   ❌ Connection test error: {e}")

if __name__ == "__main__":
    main()