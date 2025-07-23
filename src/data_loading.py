"""
Data loading module for the ETL pipeline.
Handles loading processed data into Azure SQL Database.
"""

import pandas as pd
from typing import Dict, Any, Optional, Tuple, List
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit

from utils.config import get_config
from utils.logging import get_logger
from utils.database import get_database_manager


class DataLoader:
    """Handles loading processed data into the target database."""
    
    def __init__(self, spark_session: Optional[SparkSession] = None):
        """
        Initialize data loader.
        
        Args:
            spark_session: PySpark session (optional, will create if not provided)
        """
        self.config = get_config()
        self.logger = get_logger("DataLoader")
        self.spark = spark_session or self._create_spark_session()
        self.db_manager = get_database_manager()
        self.processing_config = self.config.get_processing_config()
        
    def _create_spark_session(self) -> SparkSession:
        """Create PySpark session with configuration."""
        spark_config = self.config.get_spark_config()
        
        builder = SparkSession.builder \
            .appName(spark_config.get('app_name', 'SalesETLPipeline')) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        
        for key, value in spark_config.items():
            if key not in ['app_name']:
                builder = builder.config(key, value)
        
        return builder.getOrCreate()
    
    def create_database_schema(self) -> bool:
        """
        Create database schema for the target tables.
        Returns:
            True if successful, False otherwise
        """
        try:
            self.logger.info("Creating database schema (drop and recreate tables)")

            # Define table schemas
            sales_table_schema = [
                {"name": "OrderID", "type": "VARCHAR(50)", "nullable": False},
                {"name": "ProductID", "type": "VARCHAR(50)", "nullable": False},
                {"name": "ProductName", "type": "VARCHAR(200)", "nullable": True},
                {"name": "Category", "type": "VARCHAR(100)", "nullable": True},
                {"name": "SaleAmount", "type": "DECIMAL(10,2)", "nullable": True},
                {"name": "OriginalSaleAmount", "type": "DECIMAL(10,2)", "nullable": True},
                {"name": "OriginalCurrency", "type": "VARCHAR(3)", "nullable": True},
                {"name": "ExchangeRate", "type": "DECIMAL(10,6)", "nullable": True},
                {"name": "OrderDate", "type": "DATETIME", "nullable": False},
                {"name": "Region", "type": "VARCHAR(50)", "nullable": False},
                {"name": "CustomerID", "type": "VARCHAR(50)", "nullable": True},
                {"name": "Discount", "type": "DECIMAL(3,2)", "nullable": True},
                {"name": "Currency", "type": "VARCHAR(3)", "nullable": False},
                {"name": "ProcessingTimestamp", "type": "DATETIME", "nullable": False}
            ]

            error_logging_schema = [
                {"name": "ErrorID", "type": "INT IDENTITY(1,1)", "nullable": False},
                {"name": "OrderID", "type": "VARCHAR(50)", "nullable": True},
                {"name": "ProductID", "type": "VARCHAR(50)", "nullable": True},
                {"name": "ErrorType", "type": "VARCHAR(100)", "nullable": False},
                {"name": "ErrorMessage", "type": "TEXT", "nullable": False},
                {"name": "ProcessingStage", "type": "VARCHAR(100)", "nullable": False},
                {"name": "ErrorTimestamp", "type": "DATETIME", "nullable": False, "default": "GETDATE()"},
                {"name": "RawData", "type": "TEXT", "nullable": True}
            ]

            currency_conversion_log_schema = [
                {"name": "LogID", "type": "INT IDENTITY(1,1)", "nullable": False},
                {"name": "FromCurrency", "type": "VARCHAR(3)", "nullable": False},
                {"name": "ToCurrency", "type": "VARCHAR(3)", "nullable": False},
                {"name": "ExchangeRate", "type": "DECIMAL(10,6)", "nullable": False},
                {"name": "ConversionTimestamp", "type": "DATETIME", "nullable": False},
                {"name": "Source", "type": "VARCHAR(50)", "nullable": False},
                {"name": "RecordCount", "type": "INT", "nullable": False}
            ]

            tables_created = []
            dropped_tables = []

            # Drop and create SalesData table
            if self.db_manager.table_exists("SalesData"):
                self.db_manager.drop_table("SalesData")
                dropped_tables.append("SalesData")
            success = self.db_manager.create_table(
                "SalesData", 
                sales_table_schema, 
                primary_key="OrderID"
            )
            if success:
                tables_created.append("SalesData")
                self.logger.info("Created SalesData table")
            else:
                self.logger.error("Failed to create SalesData table")
                return False

            # Drop and create ErrorLogging table
            if self.db_manager.table_exists("ErrorLogging"):
                self.db_manager.drop_table("ErrorLogging")
                dropped_tables.append("ErrorLogging")
            success = self.db_manager.create_table(
                "ErrorLogging", 
                error_logging_schema, 
                primary_key="ErrorID"
            )
            if success:
                tables_created.append("ErrorLogging")
                self.logger.info("Created ErrorLogging table")
            else:
                self.logger.error("Failed to create ErrorLogging table")
                return False

            # Drop and create CurrencyConversionLog table
            if self.db_manager.table_exists("CurrencyConversionLog"):
                self.db_manager.drop_table("CurrencyConversionLog")
                dropped_tables.append("CurrencyConversionLog")
            success = self.db_manager.create_table(
                "CurrencyConversionLog", 
                currency_conversion_log_schema, 
                primary_key="LogID"
            )
            if success:
                tables_created.append("CurrencyConversionLog")
                self.logger.info("Created CurrencyConversionLog table")
            else:
                self.logger.error("Failed to create CurrencyConversionLog table")
                return False

            # Create indexes for better performance
            self._create_indexes()
            self.logger.info(
                f"Database schema created successfully. Tables dropped: {dropped_tables}, Tables created: {tables_created}"
            )
            return True

        except Exception as e:
            self.logger.error("Failed to create database schema", error=e)
            return False
    
    def _create_indexes(self) -> None:
        """Create database indexes for better query performance."""
        try:
            # Indexes for SalesData table
            indexes = [
                ("SalesData", "IX_SalesData_OrderDate", ["OrderDate"], "NONCLUSTERED"),
                ("SalesData", "IX_SalesData_Region", ["Region"], "NONCLUSTERED"),
                ("SalesData", "IX_SalesData_ProductID", ["ProductID"], "NONCLUSTERED"),
                ("SalesData", "IX_SalesData_Category", ["Category"], "NONCLUSTERED"),
                ("SalesData", "IX_SalesData_CustomerID", ["CustomerID"], "NONCLUSTERED"),
                ("ErrorLogging", "IX_ErrorLogging_Timestamp", ["ErrorTimestamp"], "NONCLUSTERED"),
                ("ErrorLogging", "IX_ErrorLogging_Type", ["ErrorType"], "NONCLUSTERED"),
                ("CurrencyConversionLog", "IX_CurrencyLog_Timestamp", ["ConversionTimestamp"], "NONCLUSTERED")
            ]
            
            for table_name, index_name, columns, index_type in indexes:
                if not self._index_exists(table_name, index_name):
                    success = self.db_manager.create_index(table_name, index_name, columns, index_type)
                    if success:
                        self.logger.info(f"Created index {index_name} on {table_name}")
                    else:
                        self.logger.warning(f"Failed to create index {index_name} on {table_name}")
                        
        except Exception as e:
            self.logger.error("Failed to create indexes", error=e)
    
    def _index_exists(self, table_name: str, index_name: str) -> bool:
        """Check if index exists on table."""
        try:
            sql = f"""
            SELECT COUNT(*) 
            FROM sys.indexes 
            WHERE name = '{index_name}' AND object_id = OBJECT_ID('{table_name}')
            """
            result = self.db_manager.query_data(sql)
            return result is not None and result.iloc[0, 0] > 0
        except Exception:
            return False
    
    def load_sales_data(self, df: DataFrame, table_name: str = "SalesData") -> bool:
        """
        Load sales data into the target table.
        
        Args:
            df: Sales data DataFrame
            table_name: Target table name
            
        Returns:
            True if successful, False otherwise
        """
        try:
            self.logger.info(f"Loading sales data into {table_name}")

            # Cast problematic datetime columns to string in Spark before toPandas
            datetime_cols = ["OrderDate", "ProcessingTimestamp"]
            for col_name in datetime_cols:
                if col_name in df.columns:
                    df = df.withColumn(col_name, col(col_name).cast("string"))

            # Now convert to Pandas DataFrame
            pandas_df = df.toPandas()

            self.logger.info(f"DataFrame columns: {list(pandas_df.columns)}")
            self.logger.info(f"DataFrame dtypes before fix: {pandas_df.dtypes}")
            if not pandas_df.empty:
                self.logger.info(f"Sample row: {pandas_df.iloc[0].to_dict()}")
            # Explicitly set dtypes to match SQL Server schema
            schema = [
                ("OrderID", str),
                ("ProductID", str),
                ("ProductName", str),
                ("Category", str),
                ("SaleAmount", float),
                ("OriginalSaleAmount", float),
                ("OriginalCurrency", str),
                ("ExchangeRate", float),
                ("OrderDate", 'datetime64[ns]'),  # <-- correct dtype
                ("Region", str),
                ("CustomerID", str),
                ("Discount", float),
                ("Currency", str),
                ("ProcessingTimestamp", 'datetime64[ns]')  # <-- correct dtype
            ]
    
            for col_name, dtype in schema:
                if col_name in pandas_df.columns:
                    if dtype == 'datetime64[ns]':
                        pandas_df[col_name] = pd.to_datetime(pandas_df[col_name], errors='coerce')
                    else:
                        pandas_df[col_name] = pandas_df[col_name].astype(dtype, errors='ignore')
            # Fix any remaining datetime columns with comprehensive conversion
            datetime_columns = [col for col, dtype in schema if dtype == 'datetime64[ns]']
            for col_name in datetime_columns:
                if col_name in pandas_df.columns:
                    # Always force conversion to datetime64[ns], regardless of current dtype
                    try:
                        pandas_df[col_name] = pd.to_datetime(pandas_df[col_name], errors='coerce').astype('datetime64[ns]')
                    except Exception as e:
                        self.logger.warning(f"Could not convert {col_name} to datetime64[ns]: {e}. Dropping column.")
                        pandas_df = pandas_df.drop(columns=[col_name])
            self.logger.info(f"Dtypes after final datetime fix: {pandas_df.dtypes}")
            if not pandas_df.empty:
                self.logger.info(f"Sample row after fix: {pandas_df.iloc[0].to_dict()}")
            # Ensure column order matches schema (drop any extra columns)
            pandas_df = pandas_df[[col for col, _ in schema if col in pandas_df.columns]]
            # Load to SQL
            success = self.db_manager.load_dataframe(pandas_df, table_name)
            if success:
                record_count = len(pandas_df)
                self.logger.info(f"Loaded {record_count} records into {table_name}")
                return True
            else:
                self.logger.error(f"Failed to load data into {table_name}")
                return False
        except Exception as e:
            print("traceback.format_exc()")
            import traceback    
            self.logger.info(traceback.format_exc())
            self.logger.error(f"Failed to load sales data into {table_name}", error=e)
            return False
    
    def load_error_records(self, error_records: List[Dict[str, Any]], table_name: str = "ErrorLogging") -> bool:
        """
        Load error records into the error logging table.
        
        Args:
            error_records: List of error record dictionaries
            table_name: Target table name
            
        Returns:
            True if successful, False otherwise
        """
        if not error_records:
            return True
        
        try:
            self.logger.info(f"Loading error records into {table_name}")
            
            # Ensure error_records is a list of dicts with string keys
            dict_records = []
            for rec in error_records:
                if hasattr(rec, 'dict'):
                    rec = rec.dict()
                # If keys are not strings, convert them
                rec_fixed = {str(k): v for k, v in rec.items()}
                dict_records.append(rec_fixed)
            pandas_df = pd.DataFrame(dict_records)
            # Rename columns if they are not strings
            pandas_df.columns = [str(c) for c in pandas_df.columns]
            # Convert RawData or any object columns to string
            for col_name in pandas_df.columns:
                if pandas_df[col_name].dtype == 'object':
                    pandas_df[col_name] = pandas_df[col_name].astype(str)
            
            # Fix datetime columns
            for col_name, dtype in pandas_df.dtypes.items():
                if 'datetime' in str(dtype):
                    pandas_df[col_name] = pandas_df[col_name].astype('datetime64[ns]')
            
            # Drop RejectionTimestamp if not in SQL table
            if "RejectionTimestamp" in pandas_df.columns:
                pandas_df = pandas_df.drop(columns=["RejectionTimestamp"])
            
            # Load into database
            success = self.db_manager.load_dataframe(
                pandas_df, 
                table_name, 
                if_exists='append',
                index=False
            )
            
            if success:
                self.logger.info(f"Successfully loaded {len(error_records)} error records into {table_name}")
                return True
            else:
                self.logger.error(f"Failed to load error records into {table_name}")
                return False
                
        except Exception as e:
            self.logger.error(f"Failed to load error records into {table_name}", error=e)
            return False
    
    def load_currency_conversion_logs(self, conversion_logs: List[Dict[str, Any]], 
                                    table_name: str = "CurrencyConversionLog") -> bool:
        """
        Load currency conversion logs into the database.
        
        Args:
            conversion_logs: List of conversion log dictionaries
            table_name: Target table name
            
        Returns:
            True if successful, False otherwise
        """
        if not conversion_logs:
            return True
        
        try:
            self.logger.info(f"Loading {len(conversion_logs)} currency conversion logs into {table_name}")
            
            # Convert to pandas DataFrame
            log_df = pd.DataFrame(conversion_logs)
            
            # Ensure required columns exist
            required_columns = ["FromCurrency", "ToCurrency", "ExchangeRate", "ConversionTimestamp", "Source", "RecordCount"]
            for col in required_columns:
                if col not in log_df.columns:
                    log_df[col] = None
            
            # Convert timestamp if present
            if "ConversionTimestamp" in log_df.columns:
                log_df["ConversionTimestamp"] = pd.to_datetime(log_df["ConversionTimestamp"], errors='coerce').astype('datetime64[ns]')
            
            # Load into database
            success = self.db_manager.load_dataframe(
                log_df, 
                table_name, 
                if_exists='append',
                index=False
            )
            
            if success:
                self.logger.info(f"Successfully loaded {len(conversion_logs)} conversion logs into {table_name}")
                return True
            else:
                self.logger.error(f"Failed to load conversion logs into {table_name}")
                return False
                
        except Exception as e:
            self.logger.error(f"Failed to load currency conversion logs into {table_name}", error=e)
            return False
    
    def load_all_data(self, enriched_df: DataFrame, error_records: List[Dict[str, Any]] = None,
                     conversion_logs: List[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Load all processed data into the database.
        
        Args:
            enriched_df: Enriched sales data DataFrame
            error_records: List of error records (optional)
            conversion_logs: List of currency conversion logs (optional)
            
        Returns:
            Dictionary with loading metrics
        """
        self.logger.info("Starting data loading process")
        
        loading_metrics = {
            "loading_timestamp": datetime.now().isoformat(),
            "sales_records_loaded": 0,
            "error_records_loaded": 0,
            "conversion_logs_loaded": 0,
            "loading_successful": False
        }
        
        try:
            # Step 1: Create database schema if needed
            schema_created = self.create_database_schema()
            if not schema_created:
                raise Exception("Failed to create database schema")
            
            # Step 2: Load sales data
            sales_loaded = self.load_sales_data(enriched_df)
            if sales_loaded:
                loading_metrics["sales_records_loaded"] = enriched_df.count()
            
            # Step 3: Load error records if provided
            if error_records:
                error_loaded = self.load_error_records(error_records)
                if error_loaded:
                    loading_metrics["error_records_loaded"] = len(error_records)
            
            # Step 4: Load currency conversion logs if provided
            if conversion_logs:
                conversion_loaded = self.load_currency_conversion_logs(conversion_logs)
                if conversion_loaded:
                    loading_metrics["conversion_logs_loaded"] = len(conversion_logs)
            
            # Check overall success
            loading_metrics["loading_successful"] = sales_loaded
            
            if loading_metrics["loading_successful"]:
                self.logger.info("Data loading completed successfully")
                self.logger.info(f"Loaded {loading_metrics['sales_records_loaded']} sales records")
                self.logger.info(f"Loaded {loading_metrics['error_records_loaded']} error records")
                self.logger.info(f"Loaded {loading_metrics['conversion_logs_loaded']} conversion logs")
            else:
                self.logger.error("Data loading failed")
            
            return loading_metrics
            
        except Exception as e:
            self.logger.error("Data loading failed", error=e)
            loading_metrics["loading_successful"] = False
            loading_metrics["error"] = str(e)
            raise
    
    def get_loading_summary(self) -> Dict[str, Any]:
        """
        Get summary of data loading process.
        
        Returns:
            Dictionary with loading summary
        """
        try:
            # Get row counts from database
            sales_count = self.db_manager.get_table_row_count("SalesData") or 0
            error_count = self.db_manager.get_table_row_count("ErrorLogging") or 0
            conversion_count = self.db_manager.get_table_row_count("CurrencyConversionLog") or 0
            
            return {
                "sales_records_in_database": sales_count,
                "error_records_in_database": error_count,
                "conversion_logs_in_database": conversion_count,
                "database_connection_status": self.db_manager.test_connection()
            }
            
        except Exception as e:
            self.logger.error("Failed to get loading summary", error=e)
            return {}
    
    def cleanup(self):
        """Clean up resources."""
        if self.spark:
            self.spark.stop()


def main():
    """Main function for testing data loading."""
    try:
        from data_ingestion import DataIngestion
        from data_cleaning import DataCleaner
        from data_enrichment import DataEnricher
        
        # Process data first
        ingestion = DataIngestion()
        sales_df, product_df, _ = ingestion.ingest_all_data()
        
        cleaner = DataCleaner(ingestion.spark)
        cleaned_df, _ = cleaner.clean_sales_data(sales_df)
        
        enricher = DataEnricher(ingestion.spark)
        enriched_df, _ = enricher.enrich_sales_data(cleaned_df, product_df)
        
        # Load data
        loader = DataLoader(ingestion.spark)
        loading_metrics = loader.load_all_data(enriched_df)
        
        print("Data loading completed successfully!")
        print(f"Sales records loaded: {loading_metrics['sales_records_loaded']}")
        print(f"Error records loaded: {loading_metrics['error_records_loaded']}")
        print(f"Conversion logs loaded: {loading_metrics['conversion_logs_loaded']}")
        
        # Get loading summary
        summary = loader.get_loading_summary()
        print(f"Database summary: {summary}")
        
    except Exception as e:
        print(f"Data loading failed: {e}")
    finally:
        if 'ingestion' in locals():
            ingestion.cleanup()
        if 'cleaner' in locals():
            cleaner.cleanup()
        if 'enricher' in locals():
            enricher.cleanup()
        if 'loader' in locals():
            loader.cleanup()


if __name__ == "__main__":
    main()