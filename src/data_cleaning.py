"""
Data cleaning module for the ETL pipeline.
Handles data validation, null handling, duplicate removal, and data transformation.
"""

import pandas as pd
from typing import Dict, Any, Optional, Tuple, List
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, when, lit, to_timestamp, regexp_replace, trim, 
    udf, count, isnan, isnull, row_number, desc, initcap
)
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, DoubleType, TimestampType, BooleanType

from utils.config import get_config
from utils.logging import get_logger
from models.schemas import RawSalesRecord, RejectedRecord, DEFAULT_VALIDATION_RULES


class DataCleaner:
    """Handles data cleaning, validation, and transformation."""
    
    def __init__(self, spark_session: Optional[SparkSession] = None):
        """
        Initialize data cleaner.
        
        Args:
            spark_session: PySpark session (optional, will create if not provided)
        """
        self.config = get_config()
        self.logger = get_logger("DataCleaner")
        self.spark = spark_session or self._create_spark_session()
        self.processing_config = self.config.get_processing_config()
        self.validation_config = self.config.get_validation_config()
        self.rejected_records = []
        
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
    
    def validate_required_fields(self, df: DataFrame) -> Tuple[DataFrame, DataFrame]:
        """
        Validate required fields and separate valid/invalid records.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Tuple of (valid_df, invalid_df)
        """
        required_fields = self.validation_config.get('required_fields', [])
        
        # Create validation condition
        validation_condition = lit(True)
        for field in required_fields:
            validation_condition = validation_condition & (
                col(field).isNotNull() & (col(field) != "")
            )
        
        # Split into valid and invalid records
        valid_df = df.filter(validation_condition)
        invalid_df = df.filter(~validation_condition)
        
        invalid_count = invalid_df.count()
        if invalid_count > 0:
            self.logger.warning(f"Found {invalid_count} records with missing required fields")
            
            # Log rejected records
            invalid_records = invalid_df.collect()
            for record in invalid_records:
                record_dict = record.asDict()
                rejected_record = RejectedRecord(
                    OrderID=record_dict.get('OrderID'),
                    ProductID=record_dict.get('ProductID'),
                    RawData=record_dict,
                    ErrorType="Missing Required Fields",
                    ErrorMessage=f"Missing required fields: {[f for f in required_fields if not record_dict.get(f)]}",
                    ProcessingStage="Required Field Validation"
                )
                self.rejected_records.append(rejected_record)
        
        return valid_df, invalid_df
    
    def validate_date_formats(self, df: DataFrame) -> Tuple[DataFrame, DataFrame]:
        """
        Validate and parse date formats.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Tuple of (valid_df, invalid_df)
        """
        date_formats = self.validation_config.get('date_formats', [])
        
        # Create date parsing UDF
        def parse_date(date_str):
            if not date_str:
                return None
            
            for fmt in date_formats:
                try:
                    return datetime.strptime(date_str, fmt)
                except ValueError:
                    continue
            return None
        
        parse_date_udf = udf(parse_date, TimestampType())
        
        # Add parsed date column
        df_with_parsed_date = df.withColumn("ParsedOrderDate", parse_date_udf(col("OrderDate")))
        
        # Split into valid and invalid records
        valid_df = df_with_parsed_date.filter(col("ParsedOrderDate").isNotNull())
        invalid_df = df_with_parsed_date.filter(col("ParsedOrderDate").isNull())
        
        invalid_count = invalid_df.count()
        if invalid_count > 0:
            self.logger.warning(f"Found {invalid_count} records with invalid date formats")
            
            # Log rejected records
            invalid_records = invalid_df.collect()
            for record in invalid_records:
                record_dict = record.asDict()
                rejected_record = RejectedRecord(
                    OrderID=record_dict.get('OrderID'),
                    ProductID=record_dict.get('ProductID'),
                    RawData=record_dict,
                    ErrorType="Invalid Date Format",
                    ErrorMessage=f"Invalid date format: {record_dict.get('OrderDate')}",
                    ProcessingStage="Date Validation"
                )
                self.rejected_records.append(rejected_record)
        
        # Replace OrderDate with parsed date and drop temporary column
        valid_df = valid_df.withColumn("OrderDate", col("ParsedOrderDate")).drop("ParsedOrderDate")
        
        return valid_df, invalid_df
    
    def validate_enum_values(self, df: DataFrame) -> Tuple[DataFrame, DataFrame]:
        """
        Validate enum values for categorical fields.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Tuple of (valid_df, invalid_df)
        """
        valid_regions = self.validation_config.get('valid_regions', [])
        valid_currencies = self.validation_config.get('valid_currencies', [])
        
        # Create validation conditions
        region_condition = col("Region").isNull() | col("Region").isin(valid_regions)
        currency_condition = col("Currency").isNull() | col("Currency").isin(valid_currencies)
        
        validation_condition = region_condition & currency_condition
        
        # Split into valid and invalid records
        valid_df = df.filter(validation_condition)
        invalid_df = df.filter(~validation_condition)
        
        invalid_count = invalid_df.count()
        if invalid_count > 0:
            self.logger.warning(f"Found {invalid_count} records with invalid enum values")
            
            # Log rejected records
            invalid_records = invalid_df.collect()
            for record in invalid_records:
                record_dict = record.asDict()
                errors = []
                if record_dict.get('Region') and record_dict.get('Region') not in valid_regions:
                    errors.append(f"Invalid region: {record_dict.get('Region')}")
                if record_dict.get('Currency') and record_dict.get('Currency') not in valid_currencies:
                    errors.append(f"Invalid currency: {record_dict.get('Currency')}")
                
                rejected_record = RejectedRecord(
                    OrderID=record_dict.get('OrderID'),
                    ProductID=record_dict.get('ProductID'),
                    RawData=record_dict,
                    ErrorType="Invalid Enum Values",
                    ErrorMessage=f"Invalid enum values: {'; '.join(errors)}",
                    ProcessingStage="Enum Validation"
                )
                self.rejected_records.append(rejected_record)
        
        return valid_df, invalid_df
    
    def validate_numeric_ranges(self, df: DataFrame) -> Tuple[DataFrame, DataFrame]:
        """
        Validate numeric field ranges.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Tuple of (valid_df, invalid_df)
        """
        sale_amount_validation = self.validation_config.get('sale_amount_validation', {})
        min_amount = sale_amount_validation.get('min_value', -1000.0)
        max_amount = sale_amount_validation.get('max_value', 10000.0)
        
        # Create validation conditions
        sale_amount_condition = (
            col("SaleAmount").isNull() | 
            ((col("SaleAmount") >= min_amount) & (col("SaleAmount") <= max_amount))
        )
        
        discount_condition = (
            col("Discount").isNull() | 
            ((col("Discount") >= 0) & (col("Discount") <= 1))
        )
        
        validation_condition = sale_amount_condition & discount_condition
        
        # Split into valid and invalid records
        valid_df = df.filter(validation_condition)
        invalid_df = df.filter(~validation_condition)
        
        invalid_count = invalid_df.count()
        if invalid_count > 0:
            self.logger.warning(f"Found {invalid_count} records with invalid numeric ranges")
            
            # Log rejected records
            invalid_records = invalid_df.collect()
            for record in invalid_records:
                record_dict = record.asDict()
                errors = []
                if record_dict.get('SaleAmount') is not None:
                    if record_dict.get('SaleAmount') < min_amount or record_dict.get('SaleAmount') > max_amount:
                        errors.append(f"SaleAmount out of range: {record_dict.get('SaleAmount')}")
                if record_dict.get('Discount') is not None:
                    if record_dict.get('Discount') < 0 or record_dict.get('Discount') > 1:
                        errors.append(f"Discount out of range: {record_dict.get('Discount')}")
                
                rejected_record = RejectedRecord(
                    OrderID=record_dict.get('OrderID'),
                    ProductID=record_dict.get('ProductID'),
                    RawData=record_dict,
                    ErrorType="Invalid Numeric Range",
                    ErrorMessage=f"Invalid numeric ranges: {'; '.join(errors)}",
                    ProcessingStage="Numeric Validation"
                )
                self.rejected_records.append(rejected_record)
        
        return valid_df, invalid_df
    
    def remove_duplicates(self, df: DataFrame) -> Tuple[DataFrame, int]:
        """
        Remove duplicate records based on OrderID and ProductID.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Tuple of (deduplicated_df, duplicate_count)
        """
        if not self.processing_config.get('enable_duplicate_removal', True):
            return df, 0
        
        # Count duplicates
        total_count = df.count()
        
        # Remove duplicates, keeping the first occurrence
        deduplicated_df = df.dropDuplicates(["OrderID", "ProductID"])
        
        duplicate_count = total_count - deduplicated_df.count()
        
        if duplicate_count > 0:
            self.logger.info(f"Removed {duplicate_count} duplicate records")
        
        return deduplicated_df, duplicate_count
    
    def handle_null_values(self, df: DataFrame) -> DataFrame:
        """
        Handle null values with appropriate defaults.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with null values handled
        """
        # Handle null values with defaults
        df_cleaned = df.withColumn("CustomerID", 
                                 when(col("CustomerID").isNull() | (col("CustomerID") == ""), 
                                      lit("UNKNOWN")).otherwise(col("CustomerID")))
        
        df_cleaned = df_cleaned.withColumn("Discount", 
                                         when(col("Discount").isNull(), 
                                              lit(0.0)).otherwise(col("Discount")))
        
        df_cleaned = df_cleaned.withColumn("Currency", 
                                         when(col("Currency").isNull(), 
                                              lit("USD")).otherwise(col("Currency")))
        
        # For SaleAmount, we'll keep nulls as they might be legitimate (e.g., pending orders)
        # but we'll handle them in the enrichment phase
        
        return df_cleaned
    
    def clean_text_fields(self, df: DataFrame) -> DataFrame:
        """
        Clean text fields by trimming whitespace and standardizing.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with cleaned text fields
        """
        # Trim whitespace from string columns
        df_cleaned = df.withColumn("OrderID", trim(col("OrderID")))
        df_cleaned = df_cleaned.withColumn("ProductID", trim(col("ProductID")))
        df_cleaned = df_cleaned.withColumn("Region", trim(col("Region")))
        df_cleaned = df_cleaned.withColumn("CustomerID", trim(col("CustomerID")))
        df_cleaned = df_cleaned.withColumn("Currency", trim(col("Currency")))
        
        # Standardize region names (capitalize first letter)
        df_cleaned = df_cleaned.withColumn("Region", 
                                         when(col("Region").isNotNull(),
                                              initcap(col("Region"))).otherwise(col("Region")))
        
        return df_cleaned
    
    def clean_sales_data(self, df: DataFrame) -> Tuple[DataFrame, Dict[str, Any]]:
        """
        Perform comprehensive data cleaning on sales data.
        
        Args:
            df: Input sales DataFrame
            
        Returns:
            Tuple of (cleaned_df, cleaning_metrics)
        """
        self.logger.info("Starting data cleaning process")
        
        initial_count = df.count()
        cleaning_metrics = {
            "initial_records": initial_count,
            "rejected_records": 0,
            "duplicates_removed": 0,
            "final_records": 0,
            "cleaning_timestamp": datetime.now().isoformat()
        }
        
        try:
            # Step 1: Validate required fields
            df_valid, df_invalid = self.validate_required_fields(df)
            cleaning_metrics["missing_required_fields"] = df_invalid.count()
            
            # Step 2: Validate date formats
            df_valid, df_invalid_dates = self.validate_date_formats(df_valid)
            cleaning_metrics["invalid_dates"] = df_invalid_dates.count()
            
            # Step 3: Validate enum values
            df_valid, df_invalid_enums = self.validate_enum_values(df_valid)
            cleaning_metrics["invalid_enums"] = df_invalid_enums.count()
            
            # Step 4: Validate numeric ranges
            df_valid, df_invalid_numeric = self.validate_numeric_ranges(df_valid)
            cleaning_metrics["invalid_numeric"] = df_invalid_numeric.count()
            
            # Step 5: Remove duplicates
            df_valid, duplicate_count = self.remove_duplicates(df_valid)
            cleaning_metrics["duplicates_removed"] = duplicate_count
            
            # Step 6: Handle null values
            df_valid = self.handle_null_values(df_valid)
            
            # Step 7: Clean text fields
            df_valid = self.clean_text_fields(df_valid)
            
            # Calculate final metrics
            final_count = df_valid.count()
            cleaning_metrics.update({
                "rejected_records": len(self.rejected_records),
                "final_records": final_count,
                "rejection_rate": (len(self.rejected_records) / initial_count * 100) if initial_count > 0 else 0
            })
            # Use configurable error rate threshold (default 0.05 = 5%)
            threshold_fraction = self.validation_config.get('max_error_rate', 0.05)
            threshold = threshold_fraction * 100
            self.logger.info(f"Using rejection rate threshold: {threshold:.2f}%", event_type="config", pipeline_name="SalesETLPipeline")
            if cleaning_metrics["rejection_rate"] > threshold:
                error_msg = f"Rejection rate {cleaning_metrics['rejection_rate']:.2f}% exceeds threshold {threshold:.2f}%. Failing job."
                self.logger.critical(error_msg, event_type="rejection_rate_exceeded", pipeline_name="SalesETLPipeline")
                raise ValueError(error_msg)
            
            self.logger.info(f"Data cleaning completed. Final records: {final_count}")
            self.logger.log_data_validation(
                record_count=initial_count,
                valid_count=final_count,
                invalid_count=len(self.rejected_records)
            )
            
            return df_valid, cleaning_metrics
            
        except Exception as e:
            self.logger.error("Data cleaning failed", error=e, event_type="cleaning_failure", pipeline_name="SalesETLPipeline")
            raise
    
    def save_rejected_records(self, output_path: Optional[str] = None) -> bool:
        """
        Save rejected records to file.
        
        Args:
            output_path: Output file path
            
        Returns:
            True if successful, False otherwise
        """
        if not self.rejected_records:
            return True
        
        try:
            output_path = output_path or "data/rejected/rejected_records.csv"
            
            # Convert to DataFrame and save
            rejected_df = pd.DataFrame([record.dict() for record in self.rejected_records])
            rejected_df.to_csv(output_path, index=False)
            
            self.logger.info(f"Saved {len(self.rejected_records)} rejected records to {output_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to save rejected records to {output_path}", error=e)
            return False
    
    def get_cleaning_summary(self) -> Dict[str, Any]:
        """
        Get summary of data cleaning process.
        
        Returns:
            Dictionary with cleaning summary
        """
        return {
            "rejected_records_count": len(self.rejected_records),
            "rejected_records": self.rejected_records
        }
    
    def cleanup(self):
        """Clean up resources."""
        if self.spark:
            self.spark.stop()


def main():
    """Main function for testing data cleaning."""
    try:
        from data_ingestion import DataIngestion
        
        # Ingest data first
        ingestion = DataIngestion()
        sales_df, product_df, _ = ingestion.ingest_all_data()
        
        # Clean data
        cleaner = DataCleaner(ingestion.spark)
        cleaned_df, metrics = cleaner.clean_sales_data(sales_df)
        
        print("Data cleaning completed successfully!")
        print(f"Initial records: {metrics['initial_records']}")
        print(f"Final records: {metrics['final_records']}")
        print(f"Rejection rate: {metrics['rejection_rate']:.2f}%")
        
        # Save rejected records
        cleaner.save_rejected_records()
        
    except Exception as e:
        print(f"Data cleaning failed: {e}")
    finally:
        if 'ingestion' in locals():
            ingestion.cleanup()
        if 'cleaner' in locals():
            cleaner.cleanup()


if __name__ == "__main__":
    main() 