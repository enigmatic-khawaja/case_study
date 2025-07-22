#!/usr/bin/env python3
"""
Docker Setup Script for ETL Pipeline
This script sets up the database and environment for Docker deployment.
"""

import os
import sys
import time
import subprocess
from pathlib import Path

# Add src to path
sys.path.append(str(Path(__file__).parent.parent / "src"))

from utils.config import get_config
from utils.database import get_database_manager
from utils.logging import get_logger


def wait_for_sql_server(max_retries=30, delay=2):
    """
    Wait for SQL Server to be ready.
    
    Args:
        max_retries: Maximum number of retry attempts
        delay: Delay between retries in seconds
        
    Returns:
        True if SQL Server is ready, False otherwise
    """
    logger = get_logger("DockerSetup")
    config = get_config()
    db_config = config.get_database_config()
    
    logger.info("Waiting for SQL Server to be ready...")
    
    for attempt in range(max_retries):
        try:
            # Connect to master database to test SQL Server readiness
            import pymssql
            conn = pymssql.connect(
                server=db_config['host'],
                port=db_config['port'],
                user=db_config['username'],
                password=db_config['password'],
                database='master'
            )
            conn.close()
            logger.info("SQL Server is ready!")
            return True
        except Exception as e:
            logger.info(f"Attempt {attempt + 1}/{max_retries}: SQL Server not ready yet ({e})")
        
        time.sleep(delay)
    
    logger.error("SQL Server failed to start within the expected time")
    return False


def create_database():
    """Create the database if it doesn't exist."""
    logger = get_logger("DockerSetup")
    config = get_config()
    db_config = config.get_database_config()
    
    try:
        # Connect to master database to create our database
        import pymssql
        conn = pymssql.connect(
            server=db_config['host'],
            port=db_config['port'],
            user=db_config['username'],
            password=db_config['password'],
            database='master',
            autocommit=True  # Enable autocommit to avoid transaction issues
        )
        
        cursor = conn.cursor()
        
        # Check if database exists
        cursor.execute(f"SELECT name FROM sys.databases WHERE name = '{db_config['database']}'")
        if not cursor.fetchone():
            logger.info(f"Creating database: {db_config['database']}")
            cursor.execute(f"CREATE DATABASE [{db_config['database']}]")
            logger.info("Database created successfully")
        else:
            logger.info(f"Database {db_config['database']} already exists")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"Failed to create database: {e}")
        raise


def setup_schema():
    """Set up the database schema."""
    logger = get_logger("DockerSetup")
    
    try:
        # Read and execute the schema creation script
        schema_file = Path(__file__).parent.parent / "sql" / "create_schema.sql"
        
        if not schema_file.exists():
            logger.error(f"Schema file not found: {schema_file}")
            return False
        
        with open(schema_file, 'r') as f:
            schema_sql = f.read()
        
        # Split the schema into separate batches for SQL Server compatibility
        db_manager = get_database_manager()
        
        # Extract different parts of the schema
        lines = schema_sql.split('\n')
        
        # Batch 1: Tables
        table_statements = []
        # Batch 2: Indexes  
        index_statements = []
        # Batch 3: Views
        view_statements = []
        # Batch 4: Extended properties
        property_statements = []
        
        current_batch = table_statements
        current_statement = []
        
        for line in lines:
            line = line.strip()
            if line.startswith('--'):
                continue
            if not line:
                continue
                
            current_statement.append(line)
            
            if line.endswith(';'):
                statement = '\n'.join(current_statement)
                if 'CREATE TABLE' in statement:
                    table_statements.append(statement)
                elif 'CREATE INDEX' in statement:
                    index_statements.append(statement)
                elif 'CREATE VIEW' in statement:
                    view_statements.append(statement)
                elif 'EXEC sp_addextendedproperty' in statement:
                    property_statements.append(statement)
                elif 'PRINT' in statement:
                    # Skip PRINT statements
                    pass
                current_statement = []
        
        # Execute tables first
        logger.info("Creating tables...")
        for statement in table_statements:
            try:
                # Extract table name from CREATE TABLE statement
                if 'CREATE TABLE' in statement:
                    table_name = statement.split('CREATE TABLE')[1].split('(')[0].strip()
                    if db_manager.table_exists(table_name):
                        logger.info(f"Table {table_name} already exists, skipping creation")
                        continue
                
                db_manager.execute_sql(statement)
            except Exception as e:
                error_msg = str(e).lower()
                if "already exists" in error_msg or "there is already an object" in error_msg:
                    logger.info("Table already exists, skipping...")
                else:
                    logger.error(f"Failed to execute table creation: {statement[:50]}...")
                    return False
        
        # Execute indexes
        logger.info("Creating indexes...")
        for statement in index_statements:
            try:
                db_manager.execute_sql(statement)
            except Exception as e:
                error_msg = str(e).lower()
                if "already exists" in error_msg or "there is already an object" in error_msg:
                    logger.info("Index already exists, skipping...")
                else:
                    logger.error(f"Failed to execute index creation: {statement[:50]}...")
                    return False
        
        # Execute views (each in separate batch)
        logger.info("Creating views...")
        for statement in view_statements:
            try:
                # Extract view name from CREATE VIEW statement
                if 'CREATE VIEW' in statement:
                    view_name = statement.split('CREATE VIEW')[1].split('AS')[0].strip()
                    if db_manager.view_exists(view_name):
                        logger.info(f"View {view_name} already exists, skipping creation")
                        continue
                
                db_manager.execute_sql(statement)
            except Exception as e:
                error_msg = str(e).lower()
                if "already exists" in error_msg or "there is already an object" in error_msg:
                    logger.info("View already exists, skipping...")
                else:
                    logger.error(f"Failed to execute view creation: {statement[:50]}...")
                    return False
        
        # Execute extended properties (with error handling for existing properties)
        logger.info("Adding extended properties...")
        for statement in property_statements:
            try:
                db_manager.execute_sql(statement)
            except Exception as e:
                error_msg = str(e).lower()
                if "already exists" in error_msg or "property cannot be added" in error_msg:
                    logger.info("Extended property already exists, skipping...")
                else:
                    logger.error(f"Failed to execute property creation: {statement[:50]}...")
                    # Do not return False here, just continue
        
        logger.info("Database schema created successfully")
        return True
        
    except Exception as e:
        logger.error(f"Failed to set up schema: {e}")
        return False


def verify_setup():
    """Verify that the setup was successful."""
    logger = get_logger("DockerSetup")
    
    try:
        db_manager = get_database_manager()
        
        # Check if tables exist
        required_tables = ['SalesData', 'ErrorLogging', 'CurrencyConversionLog']
        
        for table in required_tables:
            if not db_manager.table_exists(table):
                logger.error(f"Required table {table} does not exist")
                return False
        
        logger.info("All required tables exist")
        
        # Check if views exist
        required_views = ['vw_SalesSummary', 'vw_ErrorSummary', 'vw_CurrencyConversionSummary']
        
        for view in required_views:
            try:
                result = db_manager.query_data(f"SELECT TOP 1 * FROM {view}")
                if result is None:
                    logger.error(f"Required view {view} does not exist or is not accessible")
                    return False
            except Exception as e:
                logger.error(f"Failed to verify view {view}: {e}")
                return False
        
        logger.info("All required views exist")
        return True
        
    except Exception as e:
        logger.error(f"Failed to verify setup: {e}")
        return False


def main():
    """Main setup function."""
    logger = get_logger("DockerSetup")
    
    logger.info("Starting Docker setup for ETL Pipeline...")
    
    try:
        # Wait for SQL Server to be ready
        if not wait_for_sql_server():
            logger.error("SQL Server is not ready. Exiting.")
            sys.exit(1)
        
        # Create database
        create_database()
        
        # Set up schema
        if not setup_schema():
            logger.error("Failed to set up database schema. Exiting.")
            sys.exit(1)
        
        # Verify setup
        if not verify_setup():
            logger.error("Setup verification failed. Exiting.")
            sys.exit(1)
        
        logger.info("Docker setup completed successfully!")
        
    except Exception as e:
        logger.error(f"Docker setup failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main() 