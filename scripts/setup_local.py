#!/usr/bin/env python3
"""
Local Development Setup Script for ETL Pipeline
This script sets up the environment for local development without Docker.
"""

import os
import sys
import sqlite3
from pathlib import Path

# Add src to path
sys.path.append(str(Path(__file__).parent.parent / "src"))

from utils.config import get_config
from utils.logging import get_logger


def setup_sqlite_database():
    """Set up SQLite database for local development."""
    logger = get_logger("LocalSetup")
    
    try:
        # Create data directory if it doesn't exist
        data_dir = Path(__file__).parent.parent / "data"
        data_dir.mkdir(exist_ok=True)
        
        # SQLite database path
        db_path = data_dir / "etl_pipeline.db"
        
        logger.info(f"Setting up SQLite database at: {db_path}")
        
        # Create SQLite database
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Create tables
        cursor.execute('''
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
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS ErrorLogging (
                ErrorID INTEGER PRIMARY KEY AUTOINCREMENT,
                OrderID TEXT,
                ProductID TEXT,
                ErrorType TEXT NOT NULL,
                ErrorMessage TEXT NOT NULL,
                ProcessingStage TEXT NOT NULL,
                ErrorTimestamp TEXT NOT NULL DEFAULT (datetime('now')),
                RawData TEXT
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS CurrencyConversionLog (
                LogID INTEGER PRIMARY KEY AUTOINCREMENT,
                FromCurrency TEXT NOT NULL,
                ToCurrency TEXT NOT NULL,
                ExchangeRate REAL NOT NULL,
                ConversionTimestamp TEXT NOT NULL DEFAULT (datetime('now')),
                Source TEXT NOT NULL,
                RecordCount INTEGER NOT NULL
            )
        ''')
        
        # Create indexes
        cursor.execute('CREATE INDEX IF NOT EXISTS IX_SalesData_OrderDate ON SalesData (OrderDate)')
        cursor.execute('CREATE INDEX IF NOT EXISTS IX_SalesData_Region ON SalesData (Region)')
        cursor.execute('CREATE INDEX IF NOT EXISTS IX_SalesData_ProductID ON SalesData (ProductID)')
        cursor.execute('CREATE INDEX IF NOT EXISTS IX_SalesData_Category ON SalesData (Category)')
        cursor.execute('CREATE INDEX IF NOT EXISTS IX_SalesData_CustomerID ON SalesData (CustomerID)')
        cursor.execute('CREATE INDEX IF NOT EXISTS IX_SalesData_ProcessingTimestamp ON SalesData (ProcessingTimestamp)')
        
        cursor.execute('CREATE INDEX IF NOT EXISTS IX_ErrorLogging_Timestamp ON ErrorLogging (ErrorTimestamp)')
        cursor.execute('CREATE INDEX IF NOT EXISTS IX_ErrorLogging_Type ON ErrorLogging (ErrorType)')
        cursor.execute('CREATE INDEX IF NOT EXISTS IX_ErrorLogging_Stage ON ErrorLogging (ProcessingStage)')
        
        cursor.execute('CREATE INDEX IF NOT EXISTS IX_CurrencyLog_Timestamp ON CurrencyConversionLog (ConversionTimestamp)')
        cursor.execute('CREATE INDEX IF NOT EXISTS IX_CurrencyLog_FromCurrency ON CurrencyConversionLog (FromCurrency)')
        cursor.execute('CREATE INDEX IF NOT EXISTS IX_CurrencyLog_ToCurrency ON CurrencyConversionLog (ToCurrency)')
        
        # Create views
        cursor.execute('''
            CREATE VIEW IF NOT EXISTS vw_SalesSummary AS
            SELECT 
                Region,
                Category,
                COUNT(*) as TotalOrders,
                SUM(SaleAmount) as TotalSales,
                AVG(SaleAmount) as AverageOrderValue,
                MIN(OrderDate) as FirstOrderDate,
                MAX(OrderDate) as LastOrderDate
            FROM SalesData
            WHERE SaleAmount IS NOT NULL
            GROUP BY Region, Category
        ''')
        
        cursor.execute('''
            CREATE VIEW IF NOT EXISTS vw_ErrorSummary AS
            SELECT 
                ProcessingStage,
                ErrorType,
                COUNT(*) as ErrorCount,
                MIN(ErrorTimestamp) as FirstError,
                MAX(ErrorTimestamp) as LastError
            FROM ErrorLogging
            GROUP BY ProcessingStage, ErrorType
        ''')
        
        cursor.execute('''
            CREATE VIEW IF NOT EXISTS vw_CurrencyConversionSummary AS
            SELECT 
                FromCurrency,
                ToCurrency,
                COUNT(*) as ConversionCount,
                AVG(ExchangeRate) as AverageRate,
                MIN(ConversionTimestamp) as FirstConversion,
                MAX(ConversionTimestamp) as LastConversion
            FROM CurrencyConversionLog
            GROUP BY FromCurrency, ToCurrency
        ''')
        
        conn.commit()
        conn.close()
        
        logger.info("SQLite database setup completed successfully")
        return str(db_path)
        
    except Exception as e:
        logger.error(f"Failed to set up SQLite database: {e}")
        raise


def create_local_config():
    """Create local configuration file."""
    logger = get_logger("LocalSetup")
    
    try:
        config_path = Path(__file__).parent.parent / "config_local.yaml"
        
        if config_path.exists():
            logger.info("Local config file already exists")
            return
        
        # Read template config
        template_path = Path(__file__).parent.parent / "config_template.yaml"
        
        if not template_path.exists():
            logger.error("Config template not found")
            return
        
        with open(template_path, 'r') as f:
            config_content = f.read()
        
        # Replace database configuration for SQLite
        config_content = config_content.replace(
            "driver: \"ODBC+Driver+17+for+SQL+Server\"",
            "driver: \"sqlite\""
        )
        config_content = config_content.replace(
            "host: \"localhost\"",
            "host: \"data/etl_pipeline.db\""
        )
        config_content = config_content.replace(
            "port: 1433",
            "port: null"
        )
        
        # Write local config
        with open(config_path, 'w') as f:
            f.write(config_content)
        
        logger.info(f"Local config file created: {config_path}")
        
    except Exception as e:
        logger.error(f"Failed to create local config: {e}")
        raise


def setup_directories():
    """Create necessary directories."""
    logger = get_logger("LocalSetup")
    
    try:
        base_dir = Path(__file__).parent.parent
        
        # Create directories
        directories = [
            "logs",
            "data",
            "data/processed",
            "data/rejected"
        ]
        
        for dir_path in directories:
            full_path = base_dir / dir_path
            full_path.mkdir(parents=True, exist_ok=True)
            logger.info(f"Created directory: {full_path}")
        
    except Exception as e:
        logger.error(f"Failed to create directories: {e}")
        raise


def main():
    """Main setup function."""
    logger = get_logger("LocalSetup")
    
    logger.info("Starting local development setup...")
    
    try:
        # Create directories
        setup_directories()
        
        # Set up SQLite database
        db_path = setup_sqlite_database()
        
        # Create local config
        create_local_config()
        
        logger.info("Local development setup completed successfully!")
        logger.info(f"SQLite database: {db_path}")
        logger.info("To run the pipeline locally, use: python src/main.py")
        
    except Exception as e:
        logger.error(f"Local setup failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main() 