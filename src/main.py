"""
Main ETL pipeline orchestration module.
Coordinates all stages of the ETL pipeline with comprehensive monitoring and error handling.
"""

import time
import json
import os
from typing import Dict, Any, Optional
from datetime import datetime
from pyspark.sql import SparkSession

from utils.config import get_config
from utils.logging import get_logger
from data_ingestion import DataIngestion
from data_cleaning import DataCleaner
from data_enrichment import DataEnricher
from data_loading import DataLoader


class ETLPipeline:
    """Main ETL pipeline orchestrator."""
    
    def __init__(self):
        """Initialize the ETL pipeline."""
        self.config = get_config()
        self.logger = get_logger("ETLPipeline")
        self.spark = None
        self.pipeline_metrics = {}
        
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
    
    def run_pipeline(self) -> Dict[str, Any]:
        """
        Run the complete ETL pipeline, logging each major step and outcome.
        Returns:
            Dictionary with pipeline execution metrics
        """
        start_time = time.time()
        pipeline_start = datetime.now()
        self.logger.log_pipeline_start("SalesETLPipeline")
        self.logger.info(f"Pipeline started at {pipeline_start.isoformat()}")
        pipeline_metrics = {
            "pipeline_name": "SalesETLPipeline",
            "start_timestamp": pipeline_start.isoformat(),
            "stages": {},
            "overall_success": False,
            "total_duration_seconds": 0
        }
        try:
            self.logger.info("Loading configuration...", event_type="config", pipeline_name="SalesETLPipeline")
            # Initialize Spark session
            self.logger.info("Initializing Spark session...", event_type="spark", pipeline_name="SalesETLPipeline")
            self.spark = self._create_spark_session()
            self.logger.info(f"Spark session created with config: {self.config.get_spark_config()}", event_type="spark", pipeline_name="SalesETLPipeline")
            # Stage 1: Data Ingestion
            self.logger.info("=== STAGE 1: DATA INGESTION ===")
            ingestion_start = time.time()
            ingestion = DataIngestion(self.spark)
            self.logger.info("Starting data ingestion process.")
            sales_df, product_df, ingestion_summary = ingestion.ingest_all_data()
            ingestion_duration = time.time() - ingestion_start
            self.logger.info(f"Data ingestion completed in {ingestion_duration:.2f} seconds. Sales records: {ingestion_summary.get('sales_records', 0)}, Product records: {ingestion_summary.get('product_records', 0)}")
            pipeline_metrics["stages"]["ingestion"] = {
                "duration_seconds": ingestion_duration,
                "success": True,
                "records_processed": ingestion_summary.get("sales_records", 0),
                "product_records": ingestion_summary.get("product_records", 0)
            }
            # Stage 2: Data Cleaning
            self.logger.info("=== STAGE 2: DATA CLEANING ===")
            cleaning_start = time.time()
            cleaner = DataCleaner(self.spark)
            self.logger.info("Starting data cleaning process.")
            cleaned_df, cleaning_metrics = cleaner.clean_sales_data(sales_df)
            cleaner.save_rejected_records()
            cleaning_duration = time.time() - cleaning_start
            self.logger.info(f"Data cleaning completed in {cleaning_duration:.2f} seconds. Initial: {cleaning_metrics.get('initial_records', 0)}, Final: {cleaning_metrics.get('final_records', 0)}, Rejected: {cleaning_metrics.get('rejected_records', 0)}")
            pipeline_metrics["stages"]["cleaning"] = {
                "duration_seconds": cleaning_duration,
                "success": True,
                "initial_records": cleaning_metrics.get("initial_records", 0),
                "final_records": cleaning_metrics.get("final_records", 0),
                "rejected_records": cleaning_metrics.get("rejected_records", 0),
                "rejection_rate": cleaning_metrics.get("rejection_rate", 0),
                "duplicates_removed": cleaning_metrics.get("duplicates_removed", 0)
            }
            # Stage 3: Data Enrichment
            self.logger.info("=== STAGE 3: DATA ENRICHMENT ===")
            enrichment_start = time.time()
            enricher = DataEnricher(self.spark)
            self.logger.info("Starting data enrichment process.")
            enriched_df, enrichment_metrics = enricher.enrich_sales_data(cleaned_df, product_df)
            enricher.save_enrichment_summary(enrichment_metrics)
            # Save all processed (enriched) records to CSV (format dates as strings)
            enriched_df_csv = enricher.finalize_sales_data_formatting(enriched_df, for_csv=True)
            enriched_df_csv.write.csv('data/processed/cleaned_sales_data.csv', header=True, mode='overwrite')
            self.logger.info(f"Processed (enriched) records saved to CSV at data/processed/cleaned_sales_data.csv", event_type="output", pipeline_name="SalesETLPipeline")
            enrichment_duration = time.time() - enrichment_start
            pipeline_metrics["stages"]["enrichment"] = {
                "duration_seconds": enrichment_duration,
                "success": True,
                "initial_records": enrichment_metrics.get("initial_records", 0),
                "final_records": enrichment_metrics.get("final_records", 0),
                "product_enrichment_enabled": enrichment_metrics.get("product_enrichment_enabled", False),
                "currency_conversion_enabled": enrichment_metrics.get("currency_conversion_enabled", False),
                "currency_converted_records": enrichment_metrics.get("currency_converted_records", 0)
            }
            # Stage 4: Data Loading
            self.logger.info("=== STAGE 4: DATA LOADING ===")
            loading_start = time.time()
            loader = DataLoader(self.spark)
            self.logger.info("Starting data loading process.")
            error_records = cleaner.get_cleaning_summary().get("rejected_records", [])
            conversion_logs = enricher.get_conversion_logs()  # <-- Now pass actual logs
            loading_metrics = loader.load_all_data(enriched_df, error_records, conversion_logs)
            loading_duration = time.time() - loading_start
            self.logger.info(f"Data loading completed in {loading_duration:.2f} seconds. Sales loaded: {loading_metrics.get('sales_records_loaded', 0)}, Errors loaded: {loading_metrics.get('error_records_loaded', 0)}, Conversion logs loaded: {loading_metrics.get('conversion_logs_loaded', 0)}")
            pipeline_metrics["stages"]["loading"] = {
                "duration_seconds": loading_duration,
                "success": loading_metrics.get("loading_successful", False),
                "sales_records_loaded": loading_metrics.get("sales_records_loaded", 0),
                "error_records_loaded": loading_metrics.get("error_records_loaded", 0),
                "conversion_logs_loaded": loading_metrics.get("conversion_logs_loaded", 0)
            }
            # Calculate overall metrics
            total_duration = time.time() - start_time
            pipeline_end = datetime.now()
            pipeline_metrics.update({
                "end_timestamp": pipeline_end.isoformat(),
                "total_duration_seconds": total_duration,
                "overall_success": True,
                "final_record_count": enriched_df.count()
            })
            # Log pipeline completion
            self.logger.log_pipeline_end("SalesETLPipeline", total_duration)
            self.logger.info("=== PIPELINE COMPLETED SUCCESSFULLY ===")
            self.logger.info(f"Total execution time: {total_duration:.2f} seconds")
            self.logger.info(f"Final records processed: {pipeline_metrics['final_record_count']}")
            self.logger.info(f"Pipeline metrics summary: {pipeline_metrics}", event_type="summary", pipeline_name="SalesETLPipeline")
            return pipeline_metrics
        except Exception as e:
            total_duration = time.time() - start_time
            pipeline_end = datetime.now()
            pipeline_metrics.update({
                "end_timestamp": pipeline_end.isoformat(),
                "total_duration_seconds": total_duration,
                "overall_success": False,
                "error": str(e)
            })
            self.logger.critical(f"Pipeline failed after {total_duration:.2f} seconds", error=e, event_type="failure", pipeline_name="SalesETLPipeline")
            raise
        finally:
            self._cleanup()
    
    def _cleanup(self):
        """Clean up resources."""
        if self.spark:
            self.spark.stop()
            self.logger.info("Spark session stopped")
    
    def save_pipeline_metrics(self, metrics: Dict[str, Any], output_path: Optional[str] = None) -> bool:
        """
        Save pipeline execution metrics to file.
        
        Args:
            metrics: Pipeline metrics dictionary
            output_path: Output file path
            
        Returns:
            True if successful, False otherwise
        """
        try:
            output_path = output_path or "data/processed/pipeline_metrics.json"
            
            # Ensure output directory exists
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            # Save as JSON
            with open(output_path, 'w') as f:
                json.dump(metrics, f, indent=2)
            
            self.logger.info(f"Saved pipeline metrics to: {output_path}", event_type="output", pipeline_name="SalesETLPipeline")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to save pipeline metrics to {output_path}", error=e, event_type="failure", pipeline_name="SalesETLPipeline")
            return False
    
    def generate_pipeline_report(self, metrics: Dict[str, Any]) -> str:
        """
        Generate a human-readable pipeline execution report.
        
        Args:
            metrics: Pipeline metrics dictionary
            
        Returns:
            Formatted report string
        """
        report = []
        report.append("=" * 60)
        report.append("ETL PIPELINE EXECUTION REPORT")
        report.append("=" * 60)
        report.append("")
        
        # Overall summary
        report.append("OVERALL SUMMARY:")
        report.append(f"  Pipeline Name: {metrics.get('pipeline_name', 'Unknown')}")
        report.append(f"  Start Time: {metrics.get('start_timestamp', 'Unknown')}")
        report.append(f"  End Time: {metrics.get('end_timestamp', 'Unknown')}")
        report.append(f"  Total Duration: {metrics.get('total_duration_seconds', 0):.2f} seconds")
        report.append(f"  Success: {'Yes' if metrics.get('overall_success', False) else 'No'}")
        report.append(f"  Final Records: {metrics.get('final_record_count', 0)}")
        report.append("")
        
        # Stage details
        report.append("STAGE DETAILS:")
        stages = metrics.get('stages', {})
        
        for stage_name, stage_metrics in stages.items():
            report.append(f"  {stage_name.upper()}:")
            report.append(f"    Duration: {stage_metrics.get('duration_seconds', 0):.2f} seconds")
            report.append(f"    Success: {'Yes' if stage_metrics.get('success', False) else 'No'}")
            
            # Stage-specific metrics
            if stage_name == 'ingestion':
                report.append(f"    Sales Records: {stage_metrics.get('records_processed', 0)}")
                report.append(f"    Product Records: {stage_metrics.get('product_records', 0)}")
            elif stage_name == 'cleaning':
                report.append(f"    Initial Records: {stage_metrics.get('initial_records', 0)}")
                report.append(f"    Final Records: {stage_metrics.get('final_records', 0)}")
                report.append(f"    Rejected Records: {stage_metrics.get('rejected_records', 0)}")
                report.append(f"    Rejection Rate: {stage_metrics.get('rejection_rate', 0):.2f}%")
                report.append(f"    Duplicates Removed: {stage_metrics.get('duplicates_removed', 0)}")
            elif stage_name == 'enrichment':
                report.append(f"    Initial Records: {stage_metrics.get('initial_records', 0)}")
                report.append(f"    Final Records: {stage_metrics.get('final_records', 0)}")
                report.append(f"    Product Enrichment: {'Enabled' if stage_metrics.get('product_enrichment_enabled', False) else 'Disabled'}")
                report.append(f"    Currency Conversion: {'Enabled' if stage_metrics.get('currency_conversion_enabled', False) else 'Disabled'}")
                report.append(f"    Currency Converted: {stage_metrics.get('currency_converted_records', 0)}")
            elif stage_name == 'loading':
                report.append(f"    Sales Records Loaded: {stage_metrics.get('sales_records_loaded', 0)}")
                report.append(f"    Error Records Loaded: {stage_metrics.get('error_records_loaded', 0)}")
                report.append(f"    Conversion Logs Loaded: {stage_metrics.get('conversion_logs_loaded', 0)}")
            
            report.append("")
        
        # Error information
        if not metrics.get('overall_success', False):
            report.append("ERROR INFORMATION:")
            report.append(f"  Error: {metrics.get('error', 'Unknown error')}")
            report.append("")
        
        report.append("=" * 60)
        
        return "\n".join(report)


# def main():
    # """Main function to run the ETL pipeline."""
    # try:
    #     pipeline = ETLPipeline()
    #     metrics = pipeline.run_pipeline()
    #     pipeline.save_pipeline_metrics(metrics)
    #     report = pipeline.generate_pipeline_report(metrics)
    #     pipeline.logger.info(report, event_type="report", pipeline_name="SalesETLPipeline")
    #     report_path = "data/processed/pipeline_report.txt"
    #     os.makedirs(os.path.dirname(report_path), exist_ok=True)
    #     with open(report_path, 'w') as f:
    #         f.write(report)
    #     pipeline.logger.info(f"Pipeline report saved to: {report_path}", event_type="output", pipeline_name="SalesETLPipeline")
    #     if metrics.get('overall_success', False):
    #         pipeline.logger.info("✅ ETL Pipeline completed successfully!", event_type="success", pipeline_name="SalesETLPipeline")
    #         return 0
    #     else:
    #         pipeline.logger.error("❌ ETL Pipeline failed!", event_type="failure", pipeline_name="SalesETLPipeline")
    #         return 1
    # except Exception as e:
    #     logger = get_logger("ETLPipeline")
    #     logger.error(f"❌ ETL Pipeline failed with error: {e}", event_type="failure", pipeline_name="SalesETLPipeline")
    #     return 1


# if __name__ == "__main__":
#     exit(main())