"""
Database utilities for Azure SQL Database operations.
Handles connection management, schema creation, and data loading.
"""

import pandas as pd
from typing import Dict, Any, List, Optional, Tuple
from sqlalchemy import create_engine, text, MetaData, Table, Column, String, Float, DateTime, Integer
from sqlalchemy.exc import SQLAlchemyError
import logging
from urllib.parse import quote_plus
from .config import get_config
from .logging import get_logger


class DatabaseManager:
    """Manages database connections and operations for the ETL pipeline."""
    
    def __init__(self):
        """Initialize database manager with configuration."""
        self.config = get_config().get_database_config()
        self.logger = get_logger("DatabaseManager")
        self.connection_string = self._build_connection_string()
        self.engine = None
        
    def _build_connection_string(self) -> str:
        """
        Build database connection string.
        
        Returns:
            Connection string for the database
        """
        driver = self.config.get('driver', 'ODBC+Driver+17+for+SQL+Server')
        
        # Handle SQLite for local development
        if driver.lower() == 'sqlite':
            host = self.config.get('host', 'data/etl_pipeline.db')
            return f"sqlite:///{host}"
        
        # Handle SQL Server
        host = self.config.get('host', 'localhost')
        port = self.config.get('port', 1433)
        database = self.config.get('database')
        username = self.config.get('username')
        password = self.config.get('password')
        
        # URL-encode the password to handle special characters like @
        encoded_password = quote_plus(password)
        
        # For SQL Server, we can use either pyodbc or pymssql
        # Using pymssql for better compatibility
        return f"mssql+pymssql://{username}:{encoded_password}@{host}:{port}/{database}"
    
    def get_engine(self):
        """
        Get SQLAlchemy engine instance.
        
        Returns:
            SQLAlchemy engine
        """
        import time
        from sqlalchemy import create_engine, text
        max_retries = 12  # 12 x 5s = 60s
        delay = 5
        attempt = 0
        if self.engine is not None:
            return self.engine
        # Step 1: Connect to master DB to ensure ETLDatabase exists
        master_conn_str = self.connection_string.replace(f"/{self.config.get('database')}", "/master")
        while attempt < max_retries:
            try:
                master_engine = create_engine(
                    master_conn_str,
                    echo=False,
                    pool_pre_ping=True,
                    pool_recycle=3600,
                    pool_size=self.config.get('pool_size', 10),
                    max_overflow=self.config.get('max_overflow', 20)
                )
                with master_engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
                    # Create ETLDatabase if it doesn't exist, using AUTOCOMMIT
                    dbname = self.config.get('database')
                    conn.execution_options(isolation_level='AUTOCOMMIT').execute(
                        text(f"IF DB_ID('{dbname}') IS NULL CREATE DATABASE [{dbname}]")
                    )
                self.logger.info("Ensured ETLDatabase exists.")
                break
            except Exception as e:
                attempt += 1
                self.logger.warning(f"[master] Database connection attempt {attempt} failed: {e}. Retrying in {delay} seconds...")
                time.sleep(delay)
                delay = min(delay * 2, 30)
        if attempt == max_retries:
            self.logger.error("Failed to connect to SQL Server master DB after multiple attempts.")
            raise Exception("Could not connect to the database after multiple retries.")
        # Step 2: Connect to ETLDatabase
        attempt = 0
        delay = 5
        while self.engine is None and attempt < max_retries:
            try:
                self.engine = create_engine(
                    self.connection_string,
                    echo=False,
                    pool_pre_ping=True,
                    pool_recycle=3600,
                    pool_size=self.config.get('pool_size', 10),
                    max_overflow=self.config.get('max_overflow', 20)
                )
                with self.engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
                self.logger.info("Database engine created successfully for ETLDatabase.")
                break
            except Exception as e:
                attempt += 1
                self.logger.warning(f"[ETLDatabase] Database connection attempt {attempt} failed: {e}. Retrying in {delay} seconds...")
                time.sleep(delay)
                delay = min(delay * 2, 30)
        if self.engine is None:
            self.logger.error("Failed to create database engine for ETLDatabase after multiple attempts.")
            raise Exception("Could not connect to the ETLDatabase after multiple retries.")
        return self.engine
    
    def test_connection(self) -> bool:
        """
        Test database connection.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            engine = self.get_engine()
            with engine.connect() as conn:
                result = conn.execute(text("SELECT 1"))
                result.fetchone()
            self.logger.info("Database connection test successful")
            return True
        except Exception as e:
            self.logger.error("Database connection test failed", error=e)
            return False
    
    def execute_sql(self, sql: str, params: Optional[Dict[str, Any]] = None) -> bool:
        """
        Execute SQL statement.
        
        Args:
            sql: SQL statement to execute
            params: Parameters for the SQL statement
            
        Returns:
            True if successful, False otherwise
        """
        try:
            engine = self.get_engine()
            with engine.begin() as conn:
                if params:
                    conn.execute(text(sql), params)
                else:
                    conn.execute(text(sql))
            return True
        except Exception as e:
            # Don't log errors for extended properties that already exist
            if "property cannot be added" in str(e).lower() and "already exists" in str(e).lower():
                raise  # Re-raise without logging
            else:
                self.logger.error(f"Failed to execute SQL: {sql}", error=e)
                raise  # Re-raise the exception so calling code can handle it
    
    def create_table(self, table_name: str, columns: List[Dict[str, Any]], 
                    primary_key: Optional[str] = None) -> bool:
        """
        Create database table.
        
        Args:
            table_name: Name of the table
            columns: List of column definitions
            primary_key: Primary key column name
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Build CREATE TABLE SQL
            column_definitions = []
            for col in columns:
                col_def = f"{col['name']} {col['type']}"
                if col.get('nullable') is False:
                    col_def += " NOT NULL"
                if col.get('default'):
                    col_def += f" DEFAULT {col['default']}"
                column_definitions.append(col_def)
            
            if primary_key:
                column_definitions.append(f"PRIMARY KEY ({primary_key})")
            
            sql = f"""
            CREATE TABLE {table_name} (
                {', '.join(column_definitions)}
            )
            """
            
            return self.execute_sql(sql)
            
        except Exception as e:
            self.logger.error(f"Failed to create table {table_name}", error=e)
            return False
    
    def table_exists(self, table_name: str) -> bool:
        """
        Check if table exists in database.
        
        Args:
            table_name: Name of the table to check
            
        Returns:
            True if table exists, False otherwise
        """
        try:
            engine = self.get_engine()
            with engine.connect() as conn:
                result = conn.execute(text(f"""
                    SELECT COUNT(*) 
                    FROM INFORMATION_SCHEMA.TABLES 
                    WHERE TABLE_NAME = '{table_name}'
                """))
                count = result.fetchone()[0]
                return count > 0
        except Exception as e:
            self.logger.error(f"Failed to check if table {table_name} exists", error=e)
            return False
    
    def view_exists(self, view_name: str) -> bool:
        """
        Check if view exists in database.
        
        Args:
            view_name: Name of the view to check
            
        Returns:
            True if view exists, False otherwise
        """
        try:
            engine = self.get_engine()
            with engine.connect() as conn:
                result = conn.execute(text(f"""
                    SELECT COUNT(*) 
                    FROM sys.views 
                    WHERE name = '{view_name}'
                """))
                count = result.fetchone()[0]
                return count > 0
        except Exception as e:
            self.logger.error(f"Failed to check if view {view_name} exists", error=e)
            return False
    
    def load_dataframe(self, df: pd.DataFrame, table_name: str, 
                      if_exists: str = 'append', index: bool = False) -> bool:
        """
        Load pandas DataFrame into database table.
        
        Args:
            df: DataFrame to load
            table_name: Target table name
            if_exists: How to behave if table exists ('fail', 'replace', 'append')
            index: Whether to include DataFrame index
            
        Returns:
            True if successful, False otherwise
        """
        try:
            engine = self.get_engine()
            df.to_sql(
                table_name, 
                engine, 
                if_exists=if_exists, 
                index=index,
                method='multi',
                chunksize=1000
            )
            self.logger.info(f"Successfully loaded {len(df)} rows into {table_name}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to load data into {table_name}", error=e)
            return False
    
    def query_data(self, sql: str, params: Optional[Dict[str, Any]] = None) -> Optional[pd.DataFrame]:
        """
        Execute query and return results as DataFrame.
        
        Args:
            sql: SQL query to execute
            params: Parameters for the query
            
        Returns:
            DataFrame with query results or None if failed
        """
        try:
            engine = self.get_engine()
            if params:
                df = pd.read_sql(text(sql), engine, params=params)
            else:
                df = pd.read_sql(text(sql), engine)
            return df
        except Exception as e:
            self.logger.error(f"Failed to execute query: {sql}", error=e)
            return None
    
    def get_table_schema(self, table_name: str) -> Optional[List[Dict[str, Any]]]:
        """
        Get table schema information.
        
        Args:
            table_name: Name of the table
            
        Returns:
            List of column definitions or None if failed
        """
        try:
            sql = f"""
            SELECT 
                COLUMN_NAME,
                DATA_TYPE,
                IS_NULLABLE,
                COLUMN_DEFAULT
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = '{table_name}'
            ORDER BY ORDINAL_POSITION
            """
            
            df = self.query_data(sql)
            if df is not None:
                return df.to_dict('records')
            return None
        except Exception as e:
            self.logger.error(f"Failed to get schema for table {table_name}", error=e)
            return None
    
    def create_index(self, table_name: str, index_name: str, 
                    columns: List[str], index_type: str = 'NONCLUSTERED') -> bool:
        """
        Create database index.
        
        Args:
            table_name: Name of the table
            index_name: Name of the index
            columns: List of column names for the index
            index_type: Type of index (CLUSTERED/NONCLUSTERED)
            
        Returns:
            True if successful, False otherwise
        """
        try:
            columns_str = ', '.join(columns)
            sql = f"""
            CREATE {index_type} INDEX {index_name}
            ON {table_name} ({columns_str})
            """
            return self.execute_sql(sql)
        except Exception as e:
            self.logger.error(f"Failed to create index {index_name} on {table_name}", error=e)
            return False
    
    def truncate_table(self, table_name: str) -> bool:
        """
        Truncate table (remove all data).
        
        Args:
            table_name: Name of the table to truncate
            
        Returns:
            True if successful, False otherwise
        """
        try:
            sql = f"TRUNCATE TABLE {table_name}"
            return self.execute_sql(sql)
        except Exception as e:
            self.logger.error(f"Failed to truncate table {table_name}", error=e)
            return False
    
    def drop_table(self, table_name: str) -> bool:
        """
        Drop table from database.
        
        Args:
            table_name: Name of the table to drop
            
        Returns:
            True if successful, False otherwise
        """
        try:
            sql = f"DROP TABLE IF EXISTS {table_name}"
            return self.execute_sql(sql)
        except Exception as e:
            self.logger.error(f"Failed to drop table {table_name}", error=e)
            return False
    
    def get_table_row_count(self, table_name: str) -> Optional[int]:
        """
        Get row count for a table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Number of rows or None if failed
        """
        try:
            sql = f"SELECT COUNT(*) FROM {table_name}"
            result = self.query_data(sql)
            if result is not None and len(result) > 0:
                return result.iloc[0, 0]
            return None
        except Exception as e:
            self.logger.error(f"Failed to get row count for table {table_name}", error=e)
            return None


# Global database manager instance
_db_manager = None


def get_database_manager() -> DatabaseManager:
    """
    Get global database manager instance.
    
    Returns:
        DatabaseManager instance
    """
    global _db_manager
    if _db_manager is None:
        _db_manager = DatabaseManager()
    return _db_manager


def reset_database_manager() -> None:
    """Reset the global database manager instance."""
    global _db_manager
    _db_manager = None 