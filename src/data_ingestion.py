"""
Data ingestion module for the ETL pipeline.
Handles reading and initial processing of data sources.
"""

import pandas as pd
import os
from typing import Dict, Any, Optional, Tuple, List
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.sql.functions import col, to_timestamp, when, lit

from utils.config import get_config
from utils.logging import get_logger
from models.schemas import RawSalesRecord, ProductReference


class DataIngestion:
    """Handles data ingestion from various sources."""
    
    def __init__(self, spark_session: Optional[SparkSession] = None):
        """
        Initialize data ingestion processor.
        
        Args:
            spark_session: PySpark session (optional, will create if not provided)
        """
        self.config = get_config()
        self.logger = get_logger("DataIngestion")
        self.spark = spark_session or self._create_spark_session()
        self.data_sources_config = self.config.get_data_sources_config()
        
    def _create_spark_session(self) -> SparkSession:
        """
        Create PySpark session with configuration.
        
        Returns:
            Configured SparkSession
        """
        spark_config = self.config.get_spark_config()
        
        builder = SparkSession.builder \
            .appName(spark_config.get('app_name', 'SalesETLPipeline')) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        
        # Add additional Spark configurations
        for key, value in spark_config.items():
            if key not in ['app_name']:
                builder = builder.config(key, value)
        
        return builder.getOrCreate()
    
    def read_sales_data(self, file_path: Optional[str] = None) -> DataFrame:
        """
        Read sales data from CSV file.
        
        Args:
            file_path: Path to sales data CSV file
            
        Returns:
            PySpark DataFrame with sales data
        """
        file_path = file_path or self.data_sources_config.get('sales_data_path')
        
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Sales data file not found: {file_path}")
        
        try:
            self.logger.info(f"Reading sales data from: {file_path}")
            
            # Read CSV with PySpark
            df = self.spark.read \
                .option("header", "true") \
                .option("inferSchema", "false") \
                .option("mode", "PERMISSIVE") \
                .option("columnNameOfCorruptRecord", "_corrupt_record") \
                .csv(file_path)
            
            # Define schema for sales data
            sales_schema = StructType([
                StructField("OrderID", StringType(), True),
                StructField("ProductID", StringType(), True),
                StructField("SaleAmount", StringType(), True),  # Read as string to handle nulls
                StructField("OrderDate", StringType(), True),
                StructField("Region", StringType(), True),
                StructField("CustomerID", StringType(), True),
                StructField("Discount", StringType(), True),  # Read as string to handle nulls
                StructField("Currency", StringType(), True)
            ])
            
            # Apply schema and handle data types
            df = self.spark.read \
                .option("header", "true") \
                .option("mode", "PERMISSIVE") \
                .option("columnNameOfCorruptRecord", "_corrupt_record") \
                .schema(sales_schema) \
                .csv(file_path)
            
            # Convert string columns to appropriate types
            df = df.withColumn("SaleAmount", 
                             when(col("SaleAmount").isNotNull() & (col("SaleAmount") != ""), 
                                  col("SaleAmount").cast(DoubleType()))
                             .otherwise(lit(None).cast(DoubleType())))
            
            df = df.withColumn("Discount", 
                             when(col("Discount").isNotNull() & (col("Discount") != ""), 
                                  col("Discount").cast(DoubleType()))
                             .otherwise(lit(None).cast(DoubleType())))
            
            # Filter out corrupt records if the column exists
            if "_corrupt_record" in df.columns:
                df = df.filter(col("_corrupt_record").isNull()).drop("_corrupt_record")
            
            record_count = df.count()
            self.logger.info(f"Successfully read {record_count} sales records")
            
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to read sales data from {file_path}", error=e)
            raise
    
    def read_product_reference(self, file_path: Optional[str] = None) -> DataFrame:
        """
        Read product reference data from CSV file.
        
        Args:
            file_path: Path to product reference CSV file
            
        Returns:
            PySpark DataFrame with product reference data
        """
        file_path = file_path or self.data_sources_config.get('product_reference_path')
        
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Product reference file not found: {file_path}")
        
        try:
            self.logger.info(f"Reading product reference data from: {file_path}")
            
            # Define schema for product reference data
            product_schema = StructType([
                StructField("ProductID", StringType(), False),
                StructField("ProductName", StringType(), False),
                StructField("Category", StringType(), False)
            ])
            
            # Read CSV with PySpark
            df = self.spark.read \
                .option("header", "true") \
                .option("mode", "PERMISSIVE") \
                .option("columnNameOfCorruptRecord", "_corrupt_record") \
                .schema(product_schema) \
                .csv(file_path)
            
            # Filter out corrupt records if the column exists
            if "_corrupt_record" in df.columns:
                df = df.filter(col("_corrupt_record").isNull()).drop("_corrupt_record")
            
            record_count = df.count()
            self.logger.info(f"Successfully read {record_count} product reference records")
            
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to read product reference data from {file_path}", error=e)
            raise
    
    def validate_sales_data_schema(self, df: DataFrame) -> Tuple[bool, List[str]]:
        """
        Validate that sales data DataFrame has expected schema.
        
        Args:
            df: Sales data DataFrame
            
        Returns:
            Tuple of (is_valid, list_of_errors)
        """
        expected_columns = [
            "OrderID", "ProductID", "SaleAmount", "OrderDate", 
            "Region", "CustomerID", "Discount", "Currency"
        ]
        
        actual_columns = [field.name for field in df.schema.fields]
        missing_columns = [col for col in expected_columns if col not in actual_columns]
        
        if missing_columns:
            error_msg = f"Missing required columns: {missing_columns}"
            self.logger.error(error_msg)
            return False, [error_msg]
        
        self.logger.info("Sales data schema validation passed")
        return True, []
    
    def validate_product_reference_schema(self, df: DataFrame) -> Tuple[bool, List[str]]:
        """
        Validate that product reference DataFrame has expected schema.
        
        Args:
            df: Product reference DataFrame
            
        Returns:
            Tuple of (is_valid, list_of_errors)
        """
        expected_columns = ["ProductID", "ProductName", "Category"]
        
        actual_columns = [field.name for field in df.schema.fields]
        missing_columns = [col for col in expected_columns if col not in actual_columns]
        
        if missing_columns:
            error_msg = f"Missing required columns: {missing_columns}"
            self.logger.error(error_msg)
            return False, [error_msg]
        
        self.logger.info("Product reference schema validation passed")
        return True, []
    
    def get_data_summary(self, sales_df: DataFrame, product_df: DataFrame) -> Dict[str, Any]:
        """
        Generate summary statistics for ingested data.
        
        Args:
            sales_df: Sales data DataFrame
            product_df: Product reference DataFrame
            
        Returns:
            Dictionary with data summary statistics
        """
        try:
            sales_count = sales_df.count()
            product_count = product_df.count()
            
            # Get null counts for key fields
            null_counts = {}
            for column in ["OrderID", "ProductID", "SaleAmount", "OrderDate", "Region", "Currency"]:
                null_count = sales_df.filter(col(column).isNull()).count()
                null_counts[column] = null_count
            
            # Get unique values for categorical fields
            unique_regions = sales_df.select("Region").distinct().count()
            unique_currencies = sales_df.select("Currency").distinct().count()
            unique_products = sales_df.select("ProductID").distinct().count()
            
            summary = {
                "sales_records": sales_count,
                "product_records": product_count,
                "null_counts": null_counts,
                "unique_regions": unique_regions,
                "unique_currencies": unique_currencies,
                "unique_products": unique_products,
                "ingestion_timestamp": datetime.now().isoformat()
            }
            
            self.logger.info(f"Data summary: {summary}")
            return summary
            
        except Exception as e:
            self.logger.error("Failed to generate data summary", error=e)
            return {}
    
    def save_raw_data_summary(self, summary: Dict[str, Any], output_path: Optional[str] = None) -> bool:
        """
        Save data summary to file for reference.
        
        Args:
            summary: Data summary dictionary
            output_path: Output file path
            
        Returns:
            True if successful, False otherwise
        """
        try:
            output_path = output_path or os.path.join(
                self.data_sources_config.get('processed_data_path', 'data/processed'),
                'raw_data_summary.json'
            )
            
            # Ensure output directory exists
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            # Convert to pandas DataFrame for easy saving
            import json
            with open(output_path, 'w') as f:
                json.dump(summary, f, indent=2)
            
            self.logger.info(f"Saved data summary to: {output_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to save data summary to {output_path}", error=e)
            return False
    
    def ingest_all_data(self) -> Tuple[DataFrame, DataFrame, Dict[str, Any]]:
        """
        Ingest all data sources and return processed DataFrames.
        
        Returns:
            Tuple of (sales_df, product_df, summary)
        """
        self.logger.info("Starting data ingestion process")
        
        try:
            # Read data sources
            sales_df = self.read_sales_data()
            product_df = self.read_product_reference()
            
            # Validate schemas
            sales_valid, sales_errors = self.validate_sales_data_schema(sales_df)
            product_valid, product_errors = self.validate_product_reference_schema(product_df)
            
            if not sales_valid or not product_valid:
                errors = sales_errors + product_errors
                raise ValueError(f"Schema validation failed: {'; '.join(errors)}")
            
            # Generate summary
            summary = self.get_data_summary(sales_df, product_df)
            
            # Save summary
            self.save_raw_data_summary(summary)
            
            self.logger.info("Data ingestion completed successfully")
            return sales_df, product_df, summary
            
        except Exception as e:
            self.logger.error("Data ingestion failed", error=e)
            raise
    
    def cleanup(self):
        """Clean up resources."""
        if self.spark:
            self.spark.stop()


def main():
    """Main function for testing data ingestion."""
    try:
        ingestion = DataIngestion()
        sales_df, product_df, summary = ingestion.ingest_all_data()
        
        print("Data ingestion completed successfully!")
        print(f"Sales records: {summary.get('sales_records', 0)}")
        print(f"Product records: {summary.get('product_records', 0)}")
        
    except Exception as e:
        print(f"Data ingestion failed: {e}")
    finally:
        ingestion.cleanup()


if __name__ == "__main__":
    main() 