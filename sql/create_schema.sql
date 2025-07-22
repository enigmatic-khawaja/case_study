-- Database Schema Creation Script for Sales ETL Pipeline
-- This script creates all necessary tables for the ETL pipeline

-- Drop tables if they exist (for a clean reset)
IF OBJECT_ID('dbo.SalesData', 'U') IS NOT NULL
    DROP TABLE dbo.SalesData;
IF OBJECT_ID('dbo.ErrorLogging', 'U') IS NOT NULL
    DROP TABLE dbo.ErrorLogging;
IF OBJECT_ID('dbo.CurrencyConversionLog', 'U') IS NOT NULL
    DROP TABLE dbo.CurrencyConversionLog;

-- Create SalesData table for cleaned and enriched sales data
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'SalesData')
BEGIN
    EXEC('
        CREATE TABLE SalesData (
            OrderID VARCHAR(50) NOT NULL,
            ProductID VARCHAR(50) NOT NULL,
            ProductName VARCHAR(200) NULL,
            Category VARCHAR(100) NULL,
            SaleAmount DECIMAL(10,2) NULL,
            OriginalSaleAmount DECIMAL(10,2) NULL,
            OriginalCurrency VARCHAR(3) NULL,
            ExchangeRate DECIMAL(10,6) NULL,
            OrderDate DATETIME NOT NULL,
            Region VARCHAR(50) NOT NULL,
            CustomerID VARCHAR(50) NULL,
            Discount DECIMAL(3,2) NULL,
            Currency VARCHAR(3) NOT NULL,
            ProcessingTimestamp DATETIME NOT NULL,
            CONSTRAINT PK_SalesData PRIMARY KEY (OrderID, ProductID)
        )
    ')
END

-- Create ErrorLogging table for tracking rejected records
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'ErrorLogging')
BEGIN
    EXEC('
        CREATE TABLE ErrorLogging (
            ErrorID INT IDENTITY(1,1) PRIMARY KEY,
            OrderID VARCHAR(50) NULL,
            ProductID VARCHAR(50) NULL,
            ErrorType VARCHAR(100) NOT NULL,
            ErrorMessage TEXT NOT NULL,
            ProcessingStage VARCHAR(100) NOT NULL,
            ErrorTimestamp DATETIME NOT NULL DEFAULT GETDATE(),
            RawData TEXT NULL,
            RejectionTimestamp DATETIME NULL -- Added for logging compatibility
        )
    ')
END

-- Create CurrencyConversionLog table for tracking currency conversions
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'CurrencyConversionLog')
BEGIN
    EXEC('
        CREATE TABLE CurrencyConversionLog (
            LogID INT IDENTITY(1,1) PRIMARY KEY,
            FromCurrency VARCHAR(3) NOT NULL,
            ToCurrency VARCHAR(3) NOT NULL,
            ExchangeRate DECIMAL(10,6) NOT NULL,
            ConversionTimestamp DATETIME NOT NULL,
            Source VARCHAR(50) NOT NULL,
            RecordCount INT NOT NULL
        )
    ')
END

-- Create indexes for better query performance
-- SalesData indexes
CREATE NONCLUSTERED INDEX IX_SalesData_OrderDate ON SalesData (OrderDate);
CREATE NONCLUSTERED INDEX IX_SalesData_Region ON SalesData (Region);
CREATE NONCLUSTERED INDEX IX_SalesData_ProductID ON SalesData (ProductID);
CREATE NONCLUSTERED INDEX IX_SalesData_Category ON SalesData (Category);
CREATE NONCLUSTERED INDEX IX_SalesData_CustomerID ON SalesData (CustomerID);
CREATE NONCLUSTERED INDEX IX_SalesData_ProcessingTimestamp ON SalesData (ProcessingTimestamp);

-- ErrorLogging indexes
CREATE NONCLUSTERED INDEX IX_ErrorLogging_Timestamp ON ErrorLogging (ErrorTimestamp);
CREATE NONCLUSTERED INDEX IX_ErrorLogging_Type ON ErrorLogging (ErrorType);
CREATE NONCLUSTERED INDEX IX_ErrorLogging_Stage ON ErrorLogging (ProcessingStage);

-- CurrencyConversionLog indexes
CREATE NONCLUSTERED INDEX IX_CurrencyLog_Timestamp ON CurrencyConversionLog (ConversionTimestamp);
CREATE NONCLUSTERED INDEX IX_CurrencyLog_FromCurrency ON CurrencyConversionLog (FromCurrency);
CREATE NONCLUSTERED INDEX IX_CurrencyLog_ToCurrency ON CurrencyConversionLog (ToCurrency);

-- Create view for sales summary
CREATE VIEW vw_SalesSummary AS
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
GROUP BY Region, Category;

-- Create view for error summary
CREATE VIEW vw_ErrorSummary AS
SELECT 
    ProcessingStage,
    ErrorType,
    COUNT(*) as ErrorCount,
    MIN(ErrorTimestamp) as FirstError,
    MAX(ErrorTimestamp) as LastError
FROM ErrorLogging
GROUP BY ProcessingStage, ErrorType;

-- Create view for currency conversion summary
CREATE VIEW vw_CurrencyConversionSummary AS
SELECT 
    FromCurrency,
    ToCurrency,
    COUNT(*) as ConversionCount,
    AVG(ExchangeRate) as AverageRate,
    MIN(ConversionTimestamp) as FirstConversion,
    MAX(ConversionTimestamp) as LastConversion
FROM CurrencyConversionLog
GROUP BY FromCurrency, ToCurrency;

-- Add comments to tables
EXEC sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Main table containing cleaned and enriched sales data',
    @level0type = N'SCHEMA',
    @level0name = N'dbo',
    @level1type = N'TABLE',
    @level1name = N'SalesData';

EXEC sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Table for tracking rejected records and validation errors',
    @level0type = N'SCHEMA',
    @level0name = N'dbo',
    @level1type = N'TABLE',
    @level1name = N'ErrorLogging';

EXEC sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Table for tracking currency conversion operations',
    @level0type = N'SCHEMA',
    @level0name = N'dbo',
    @level1type = N'TABLE',
    @level1name = N'CurrencyConversionLog';

IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'ErrorLogging' AND COLUMN_NAME = 'RejectionTimestamp')
BEGIN
    ALTER TABLE ErrorLogging ADD RejectionTimestamp DATETIME NULL;
END

PRINT 'Database schema created successfully!';
PRINT 'Tables created: SalesData, ErrorLogging, CurrencyConversionLog';
PRINT 'Indexes created for optimal query performance';
PRINT 'Views created: vw_SalesSummary, vw_ErrorSummary, vw_CurrencyConversionSummary'; 