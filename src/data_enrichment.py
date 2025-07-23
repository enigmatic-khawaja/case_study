"""
Data enrichment module for the ETL pipeline.
Handles product lookup and currency conversion using external API.
"""

import requests
import json
import time
from typing import Dict, Any, Optional, Tuple, List
from datetime import datetime, timedelta
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, when, lit, udf, broadcast, abs as spark_abs, round as spark_round, date_format, isnan
)
from pyspark.sql.types import StringType, DoubleType, TimestampType

from utils.config import get_config
from utils.logging import get_logger
from models.schemas import CurrencyConversionLog

# --- UDF factories that use only plain dicts, not broadcast or SparkContext ---
def make_convert_currency_udf(rates_dict):
    def convert_currency(amount, from_currency):
        if amount is None or from_currency is None:
            return amount
        rate = rates_dict.get(from_currency, 1.0)
        return amount * rate
    return udf(convert_currency, DoubleType())

def make_get_rate_udf(rates_dict):
    def get_exchange_rate(from_currency):
        return rates_dict.get(from_currency, 1.0)
    return udf(get_exchange_rate, DoubleType())


class CurrencyConverter:
    """Handles currency conversion using external API with caching and fallback."""
    
    def __init__(self):
        """Initialize currency converter."""
        self.config = get_config().get_currency_api_config()
        self.logger = get_logger("CurrencyConverter")
        self.base_url = self.config.get('base_url', 'https://api.exchangerate-api.com/v4/latest/')
        self.base_currency = self.config.get('base_currency', 'USD')
        self.timeout = self.config.get('timeout', 10)
        self.retry_attempts = self.config.get('retry_attempts', 3)
        self.cache_duration = timedelta(hours=self.config.get('cache_duration_hours', 1))
        self.fallback_rates = self.config.get('fallback_rates', {})
        
        # In-memory cache for exchange rates
        self.rate_cache = {}
        self.cache_timestamps = {}
        self.conversion_logs = []  # List to accumulate conversion log dicts
    
    def get_exchange_rate(self, from_currency: str, to_currency: str = 'USD') -> Optional[float]:
        """
        Get exchange rate from API or cache.
        
        Args:
            from_currency: Source currency
            to_currency: Target currency (default: USD)
            
        Returns:
            Exchange rate or None if failed
        """
        if from_currency == to_currency:
            return 1.0
        
        cache_key = f"{from_currency}_{to_currency}"
        
        # Check cache first
        if self._is_cache_valid(cache_key):
            rate = self.rate_cache.get(cache_key)
            if rate:
                self.logger.log_currency_conversion(
                    from_currency=from_currency,
                    to_currency=to_currency,
                    rate=rate,
                    source="cache"
                )
                # Log entry for database
                self._add_conversion_log(from_currency, to_currency, rate, "cache")
                return rate
        
        # Try API
        rate = self._fetch_from_api(from_currency, to_currency)
        if rate:
            self._cache_rate(cache_key, rate)
            self.logger.log_currency_conversion(
                from_currency=from_currency,
                to_currency=to_currency,
                rate=rate,
                source="API"
            )
            self._add_conversion_log(from_currency, to_currency, rate, "API")
            return rate
        
        # Try fallback rates
        fallback_rate = self.fallback_rates.get(from_currency)
        if fallback_rate:
            self.logger.warning(f"Using fallback rate for {from_currency}: {fallback_rate}")
            self.logger.log_currency_conversion(
                from_currency=from_currency,
                to_currency=to_currency,
                rate=fallback_rate,
                source="fallback"
            )
            self._add_conversion_log(from_currency, to_currency, fallback_rate, "fallback")
            return fallback_rate
        
        self.logger.error(f"Failed to get exchange rate for {from_currency} -> {to_currency}")
        return None
    
    def _fetch_from_api(self, from_currency: str, to_currency: str) -> Optional[float]:
        """
        Fetch exchange rate from API.
        
        Args:
            from_currency: Source currency
            to_currency: Target currency
            
        Returns:
            Exchange rate or None if failed
        """
        url = f"{self.base_url}{from_currency}"
        
        for attempt in range(self.retry_attempts):
            try:
                response = requests.get(url, timeout=self.timeout)
                response.raise_for_status()
                
                data = response.json()
                rates = data.get('rates', {})
                rate = rates.get(to_currency)
                
                if rate:
                    return rate
                else:
                    self.logger.error(f"Currency {to_currency} not found in API response")
                    return None
                    
            except requests.exceptions.RequestException as e:
                self.logger.warning(f"API request failed (attempt {attempt + 1}): {e}")
                if attempt < self.retry_attempts - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    self.logger.error(f"All API attempts failed for {from_currency} -> {to_currency}")
                    return None
            except json.JSONDecodeError as e:
                self.logger.error(f"Invalid JSON response from API: {e}")
                return None
            except Exception as e:
                self.logger.error(f"Unexpected error fetching exchange rate: {e}")
                return None
        
        return None
    
    def _is_cache_valid(self, cache_key: str) -> bool:
        """Check if cached rate is still valid."""
        if cache_key not in self.cache_timestamps:
            return False
        
        cache_time = self.cache_timestamps[cache_key]
        return datetime.now() - cache_time < self.cache_duration
    
    def _cache_rate(self, cache_key: str, rate: float):
        """Cache exchange rate with timestamp."""
        self.rate_cache[cache_key] = rate
        self.cache_timestamps[cache_key] = datetime.now()
    
    def convert_amount(self, amount: float, from_currency: str, to_currency: str = 'USD') -> Optional[float]:
        """
        Convert amount from one currency to another.
        
        Args:
            amount: Amount to convert
            from_currency: Source currency
            to_currency: Target currency
            
        Returns:
            Converted amount or None if failed
        """
        if amount is None:
            return None
        
        rate = self.get_exchange_rate(from_currency, to_currency)
        if rate is None:
            return None
        
        return amount * rate

    def _add_conversion_log(self, from_currency, to_currency, rate, source):
        # Check if already logged for this run
        for log in self.conversion_logs:
            if log["FromCurrency"] == from_currency and log["ToCurrency"] == to_currency and log["ExchangeRate"] == rate and log["Source"] == source:
                log["RecordCount"] += 1
                return
        self.conversion_logs.append({
            "FromCurrency": from_currency,
            "ToCurrency": to_currency,
            "ExchangeRate": rate,
            "ConversionTimestamp": datetime.now(),
            "Source": source,
            "RecordCount": 1
        })

    def get_conversion_logs(self):
        return self.conversion_logs


# --- Standalone helper for Spark-safe UDFs using global SparkSession ---
def get_spark():
    return SparkSession.builder.getOrCreate()

def get_currency_udfs(rates_dict):
    spark = get_spark()
    broadcast_rates = spark.sparkContext.broadcast(rates_dict)
    def convert_currency(amount, from_currency):
        if amount is None or from_currency is None:
            return amount
        rate = broadcast_rates.value.get(from_currency, 1.0)
        return amount * rate
    def get_exchange_rate(from_currency):
        return broadcast_rates.value.get(from_currency, 1.0)
    return (
        udf(convert_currency, DoubleType()),
        udf(get_exchange_rate, DoubleType())
    )


class DataEnricher:
    """Handles data enrichment including product lookup and currency conversion."""
    
    def __init__(self, spark_session: Optional[SparkSession] = None):
        """
        Initialize data enricher.
        
        Args:
            spark_session: PySpark session (optional, will create if not provided)
        """
        self.config = get_config()
        self.logger = get_logger("DataEnricher")
        self.spark = spark_session or self._create_spark_session()
        self.processing_config = self.config.get_processing_config()
        self.currency_converter = CurrencyConverter()
        self.conversion_logs = []
        
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
    
    def enrich_with_product_data(self, sales_df: DataFrame, product_df: DataFrame) -> DataFrame:
        """
        Enrich sales data with product information.
        
        Args:
            sales_df: Sales data DataFrame
            product_df: Product reference DataFrame
            
        Returns:
            Enriched DataFrame
        """
        if not self.processing_config.get('enable_product_lookup', True):
            self.logger.info("Product lookup disabled, skipping enrichment")
            return sales_df
        
        try:
            self.logger.info("Starting product data enrichment")
            
            # Broadcast product data for better performance
            broadcast_product_df = broadcast(product_df)
            
            # Join sales data with product reference data
            enriched_df = sales_df.join(
                broadcast_product_df,
                sales_df.ProductID == product_df.ProductID,
                "left"
            ).select(
                sales_df["*"],
                product_df.ProductName.alias("ProductName"),
                product_df.Category.alias("Category")
            )
            
            # Count enriched records
            total_records = enriched_df.count()
            enriched_records = enriched_df.filter(col("ProductName").isNotNull()).count()
            
            self.logger.info(f"Product enrichment completed: {enriched_records}/{total_records} records enriched")
            
            return enriched_df
            
        except Exception as e:
            self.logger.error("Product enrichment failed", error=e)
            raise
    
    def convert_currencies(self, df: DataFrame) -> DataFrame:
        """
        Convert all sale amounts to USD using exchange rates.
        Args:
            df: Input DataFrame with sales data
        Returns:
            DataFrame with converted amounts
        """
        if not self.processing_config.get('enable_currency_conversion', True):
            self.logger.info("Currency conversion disabled, skipping conversion")
            return df
        try:
            self.logger.info("Starting currency conversion")

            # Get unique currencies for batch processing
            currencies = df.select("Currency").distinct().collect()
            currency_list = [row.Currency for row in currencies if row.Currency and row.Currency != 'USD']

            # Pre-fetch exchange rates for all currencies (on driver)
            self.logger.info(f"Pre-fetching exchange rates for currencies: {currency_list}")
            rates_dict = {c: self.currency_converter.get_exchange_rate(c, 'USD') or 1.0 for c in currency_list}
            rates_dict['USD'] = 1.0

            # Use UDFs that only close over plain dicts
            convert_currency_udf = make_convert_currency_udf(rates_dict)
            get_rate_udf = make_get_rate_udf(rates_dict)

            # Apply currency conversion
            df_converted = df.withColumn(
                "OriginalSaleAmount", col("SaleAmount")
            ).withColumn(
                "OriginalCurrency", col("Currency")
            ).withColumn(
                "SaleAmount",
                when(col("Currency") == "USD", col("SaleAmount"))
                .otherwise(convert_currency_udf(col("SaleAmount"), col("Currency")))
            ).withColumn(
                "Currency", lit("USD")
            ).withColumn(
                "ExchangeRate",
                when(col("OriginalCurrency") == "USD", lit(1.0))
                .otherwise(get_rate_udf(col("OriginalCurrency")))
            )

            # Count converted records
            total_records = df_converted.count()
            converted_records = df_converted.filter(
                (col("OriginalCurrency") != "USD") & col("SaleAmount").isNotNull()
            ).count()

            self.logger.info(f"Currency conversion completed: {converted_records}/{total_records} records converted")

            # After conversion, store logs
            self.conversion_logs = self.currency_converter.get_conversion_logs()
            return df_converted
        except Exception as e:
            self.logger.error("Currency conversion failed", error=e)
            raise
    
    def add_processing_metadata(self, df: DataFrame) -> DataFrame:
        """
        Add processing metadata to the DataFrame.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with processing metadata
        """
        try:
            # Add processing timestamp
            current_timestamp = datetime.now()
            
            df_with_metadata = df.withColumn(
                "ProcessingTimestamp", lit(current_timestamp)
            )
            
            self.logger.info("Added processing metadata")
            return df_with_metadata
            
        except Exception as e:
            self.logger.error("Failed to add processing metadata", error=e)
            raise
    
    def finalize_sales_data_formatting(self, df: DataFrame, for_csv: bool = False) -> DataFrame:
        """
        Finalize formatting for sales data:
        - Set OriginalSaleAmount to 0.0 if null, and abs if negative
        - Round SaleAmount to 1 decimal place
        - Set SaleAmount to 0.0 if null or NaN
        - Format OrderDate and ProcessingTimestamp to YYYY-MM-DD string if for_csv is True
        """
        # Set OriginalSaleAmount to 0.0 if null, and abs if negative
        df = df.withColumn(
            "OriginalSaleAmount",
            when(col("OriginalSaleAmount").isNull(), lit(0.0))
            .otherwise(spark_abs(col("OriginalSaleAmount")))
        )
        # Round SaleAmount to 1 decimal place
        df = df.withColumn("SaleAmount", spark_round(col("SaleAmount"), 1))
        # Set SaleAmount to 0.0 if still null or NaN
        df = df.withColumn(
            "SaleAmount",
            when(col("SaleAmount").isNull() | isnan(col("SaleAmount")), lit(0.0)).otherwise(col("SaleAmount"))
        )
        # Set SaleAmount to abs value (always non-negative)
        df = df.withColumn("SaleAmount", spark_abs(col("SaleAmount")))
        # Format OrderDate and ProcessingTimestamp to YYYY-MM-DD only for CSV
        if for_csv:
            df = df.withColumn(
                "OrderDate",
                when(col("OrderDate").isNotNull(), date_format(col("OrderDate"), "yyyy-MM-dd")).otherwise(None)
            )
            if "ProcessingTimestamp" in df.columns:
                df = df.withColumn(
                    "ProcessingTimestamp",
                    when(col("ProcessingTimestamp").isNotNull(), date_format(col("ProcessingTimestamp"), "yyyy-MM-dd")).otherwise(None)
                )
        return df

    def enrich_sales_data(self, sales_df: DataFrame, product_df: DataFrame) -> Tuple[DataFrame, Dict[str, Any]]:
        self.logger.info("Starting data enrichment process", event_type="enrichment_start", pipeline_name="SalesETLPipeline")
        initial_count = sales_df.count()
        self.logger.info(f"Initial records for enrichment: {initial_count}", event_type="enrichment", pipeline_name="SalesETLPipeline")
        enrichment_metrics = {
            "initial_records": initial_count,
            "product_enrichment_enabled": self.processing_config.get('enable_product_lookup', True),
            "currency_conversion_enabled": self.processing_config.get('enable_currency_conversion', True),
            "enrichment_timestamp": datetime.now().isoformat()
        }
        try:
            if self.processing_config.get('enable_product_lookup', True):
                self.logger.info("Starting product enrichment...", event_type="product_enrichment", pipeline_name="SalesETLPipeline")
                sales_df = self.enrich_with_product_data(sales_df, product_df)
                enriched_count = sales_df.filter(col("ProductName").isNotNull()).count()
                self.logger.info(f"Product enrichment complete. Product-enriched records: {enriched_count}", event_type="product_enrichment", pipeline_name="SalesETLPipeline")
                enrichment_metrics["product_enriched_records"] = enriched_count
            if self.processing_config.get('enable_currency_conversion', True):
                self.logger.info("Starting currency conversion...", event_type="currency_conversion", pipeline_name="SalesETLPipeline")
                sales_df = self.convert_currencies(sales_df)
                converted_count = sales_df.filter((col("OriginalCurrency") != "USD") & col("SaleAmount").isNotNull()).count()
                self.logger.info(f"Currency conversion complete. Converted records: {converted_count}", event_type="currency_conversion", pipeline_name="SalesETLPipeline")
                enrichment_metrics["currency_converted_records"] = converted_count
            self.logger.info("Adding processing metadata...", event_type="metadata", pipeline_name="SalesETLPipeline")
            sales_df = self.add_processing_metadata(sales_df)
            self.logger.info("Processing metadata added.", event_type="metadata", pipeline_name="SalesETLPipeline")
            self.logger.info("Finalizing sales data formatting...", event_type="formatting", pipeline_name="SalesETLPipeline")
            sales_df = self.finalize_sales_data_formatting(sales_df)
            self.logger.info("Sales data formatting finalized.", event_type="formatting", pipeline_name="SalesETLPipeline")
            final_count = sales_df.count()
            enrichment_metrics.update({
                "final_records": final_count,
                "enrichment_successful": True
            })
            self.logger.info(f"Data enrichment completed. Final records: {final_count}", event_type="enrichment_end", pipeline_name="SalesETLPipeline")
            return sales_df, enrichment_metrics
        except Exception as e:
            self.logger.error("Data enrichment failed", error=e, event_type="enrichment_failure", pipeline_name="SalesETLPipeline")
            enrichment_metrics.update({
                "enrichment_successful": False,
                "error": str(e)
            })
            raise
    def save_enrichment_summary(self, enrichment_metrics: Dict[str, Any]):
        try:
            summary_path = "data/processed/enrichment_summary.json"
            with open(summary_path, 'w') as f:
                json.dump(enrichment_metrics, f, indent=2)
            self.logger.info(f"Enrichment summary saved to {summary_path}", event_type="output", pipeline_name="SalesETLPipeline")
        except Exception as e:
            self.logger.error(f"Failed to save enrichment summary: {e}", event_type="output_failure", pipeline_name="SalesETLPipeline")
    
    def cleanup(self):
        """Clean up resources."""
        if self.spark:
            self.spark.stop()

    def get_conversion_logs(self):
        return self.conversion_logs


def main():
    """Main function for testing data enrichment."""
    try:
        from data_ingestion import DataIngestion
        from data_cleaning import DataCleaner
        
        # Ingest and clean data first
        ingestion = DataIngestion()
        sales_df, product_df, _ = ingestion.ingest_all_data()
        
        cleaner = DataCleaner(ingestion.spark)
        cleaned_df, _ = cleaner.clean_sales_data(sales_df)
        
        # Enrich data
        enricher = DataEnricher(ingestion.spark)
        enriched_df, metrics = enricher.enrich_sales_data(cleaned_df, product_df)
        
        print("Data enrichment completed successfully!")
        print(f"Initial records: {metrics['initial_records']}")
        print(f"Final records: {metrics['final_records']}")
        print(f"Product enrichment enabled: {metrics['product_enrichment_enabled']}")
        print(f"Currency conversion enabled: {metrics['currency_conversion_enabled']}")
        
        # Save enrichment summary
        enricher.save_enrichment_summary(metrics)
        
    except Exception as e:
        print(f"Data enrichment failed: {e}")
    finally:
        if 'ingestion' in locals():
            ingestion.cleanup()
        if 'cleaner' in locals():
            cleaner.cleanup()
        if 'enricher' in locals():
            enricher.cleanup()


if __name__ == "__main__":
    main() 