#!/usr/bin/env python3
"""
Database setup script for the ETL pipeline.
Creates database schema and tests connectivity.
"""

import os
import sys
import sqlite3
from pathlib import Path

# Add src directory to path for imports
sys.path.append(str(Path(__file__).parent.parent / "src"))

from utils.config import get_config
from utils.database import get_database_manager
from utils.logging import get_logger


def setup_database():
    """Set up database schema and test connectivity."""
    logger = get_logger("DatabaseSetup")
    
    try:
        logger.info("Starting database setup")
        
        # Get database manager
        db_manager = get_database_manager()
        
        # Test connection
        logger.info("Testing database connection...")
        if not db_manager.test_connection():
            logger.error("Database connection test failed")
            return False
        
        logger.info("Database connection test successful")
        
        # Read SQL schema file
        schema_file = Path(__file__).parent.parent / "sql" / "create_schema.sql"
        if not schema_file.exists():
            logger.error(f"Schema file not found: {schema_file}")
            return False
        
        with open(schema_file, 'r') as f:
            schema_sql = f.read()
        
        # Split SQL into individual statements
        statements = [stmt.strip() for stmt in schema_sql.split(';') if stmt.strip()]
        
        # Execute each statement
        for i, statement in enumerate(statements):
            if statement and not statement.startswith('--'):
                logger.info(f"Executing statement {i+1}/{len(statements)}")
                success = db_manager.execute_sql(statement)
                if not success:
                    logger.error(f"Failed to execute statement {i+1}")
                    return False
        
        logger.info("Database schema created successfully")
        
        # Verify tables were created
        expected_tables = ["SalesData", "ErrorLogging", "CurrencyConversionLog"]
        for table in expected_tables:
            if db_manager.table_exists(table):
                logger.info(f"Table {table} created successfully")
            else:
                logger.error(f"Table {table} was not created")
                return False
        
        # Test sample queries
        logger.info("Testing sample queries...")
        test_queries = [
            "SELECT COUNT(*) FROM SalesData",
            "SELECT COUNT(*) FROM ErrorLogging", 
            "SELECT COUNT(*) FROM CurrencyConversionLog"
        ]
        
        for query in test_queries:
            result = db_manager.query_data(query)
            if result is not None:
                logger.info(f"Query successful: {query}")
            else:
                logger.error(f"Query failed: {query}")
                return False
        
        logger.info("Database setup completed successfully")
        return True
        
    except Exception as e:
        logger.error("Database setup failed", error=e)
        return False


def create_sqlite_test_db():
    """Create a SQLite test database for development."""
    logger = get_logger("SQLiteSetup")
    
    try:
        logger.info("Creating SQLite test database")
        
        # Create test database directory
        test_db_dir = Path(__file__).parent.parent / "data" / "test"
        test_db_dir.mkdir(parents=True, exist_ok=True)
        
        test_db_path = test_db_dir / "test_sales.db"
        
        # Connect to SQLite database
        conn = sqlite3.connect(test_db_path)
        cursor = conn.cursor()
        
        # Create tables
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS SalesData (
                OrderID TEXT PRIMARY KEY,
                ProductID TEXT NOT NULL,
                ProductName TEXT,
                Category TEXT,
                SaleAmount REAL,
                OriginalSaleAmount REAL,
                OriginalCurrency TEXT,
                ExchangeRate REAL,
                OrderDate TEXT NOT NULL,
                Region TEXT NOT NULL,
                CustomerID TEXT,
                Discount REAL,
                Currency TEXT NOT NULL DEFAULT 'USD',
                ProcessingTimestamp TEXT NOT NULL
            )
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS ErrorLogging (
                ErrorID INTEGER PRIMARY KEY AUTOINCREMENT,
                OrderID TEXT,
                ProductID TEXT,
                ErrorType TEXT NOT NULL,
                ErrorMessage TEXT NOT NULL,
                ProcessingStage TEXT NOT NULL,
                ErrorTimestamp TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                RawData TEXT
            )
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS CurrencyConversionLog (
                LogID INTEGER PRIMARY KEY AUTOINCREMENT,
                FromCurrency TEXT NOT NULL,
                ToCurrency TEXT NOT NULL,
                ExchangeRate REAL NOT NULL,
                ConversionTimestamp TEXT NOT NULL,
                Source TEXT NOT NULL,
                RecordCount INTEGER NOT NULL
            )
        """)
        
        # Create indexes
        cursor.execute("CREATE INDEX IF NOT EXISTS IX_SalesData_OrderDate ON SalesData (OrderDate)")
        cursor.execute("CREATE INDEX IF NOT EXISTS IX_SalesData_Region ON SalesData (Region)")
        cursor.execute("CREATE INDEX IF NOT EXISTS IX_SalesData_ProductID ON SalesData (ProductID)")
        cursor.execute("CREATE INDEX IF NOT EXISTS IX_ErrorLogging_Timestamp ON ErrorLogging (ErrorTimestamp)")
        cursor.execute("CREATE INDEX IF NOT EXISTS IX_CurrencyLog_Timestamp ON CurrencyConversionLog (ConversionTimestamp)")
        
        conn.commit()
        conn.close()
        
        logger.info(f"SQLite test database created: {test_db_path}")
        return True
        
    except Exception as e:
        logger.error("SQLite test database creation failed", error=e)
        return False


def main():
    """Main function for database setup."""
    print("Database Setup for Sales ETL Pipeline")
    print("=" * 50)
    
    # Check if we should use SQLite for testing
    use_sqlite = "--sqlite" in sys.argv
    
    if use_sqlite:
        print("Setting up SQLite test database...")
        success = create_sqlite_test_db()
    else:
        print("Setting up Azure SQL Database...")
        success = setup_database()
    
    if success:
        print("\n✅ Database setup completed successfully!")
        if use_sqlite:
            print("SQLite test database is ready for development")
        else:
            print("Azure SQL Database is ready for production use")
        return 0
    else:
        print("\n❌ Database setup failed!")
        return 1


if __name__ == "__main__":
    exit(main()) 