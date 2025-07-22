-- Sample Analytical Queries for Sales ETL Pipeline
-- These queries demonstrate how to analyze the processed data

-- 1. Sales Performance by Region and Category
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
ORDER BY TotalSales DESC;

-- 2. Monthly Sales Trend
SELECT 
    YEAR(OrderDate) as Year,
    MONTH(OrderDate) as Month,
    COUNT(*) as TotalOrders,
    SUM(SaleAmount) as TotalSales,
    AVG(SaleAmount) as AverageOrderValue
FROM SalesData
WHERE SaleAmount IS NOT NULL
GROUP BY YEAR(OrderDate), MONTH(OrderDate)
ORDER BY Year, Month;

-- 3. Top Products by Sales
SELECT 
    ProductID,
    ProductName,
    Category,
    COUNT(*) as TotalOrders,
    SUM(SaleAmount) as TotalSales,
    AVG(SaleAmount) as AverageOrderValue
FROM SalesData
WHERE SaleAmount IS NOT NULL AND ProductName IS NOT NULL
GROUP BY ProductID, ProductName, Category
ORDER BY TotalSales DESC;

-- 4. Currency Conversion Analysis
SELECT 
    OriginalCurrency,
    COUNT(*) as RecordsConverted,
    AVG(ExchangeRate) as AverageExchangeRate,
    MIN(ExchangeRate) as MinExchangeRate,
    MAX(ExchangeRate) as MaxExchangeRate,
    SUM(OriginalSaleAmount) as TotalOriginalAmount,
    SUM(SaleAmount) as TotalConvertedAmount
FROM SalesData
WHERE OriginalCurrency IS NOT NULL AND OriginalCurrency != 'USD'
GROUP BY OriginalCurrency
ORDER BY RecordsConverted DESC;

-- 5. Error Analysis by Processing Stage
SELECT 
    ProcessingStage,
    ErrorType,
    COUNT(*) as ErrorCount,
    MIN(ErrorTimestamp) as FirstError,
    MAX(ErrorTimestamp) as LastError
FROM ErrorLogging
GROUP BY ProcessingStage, ErrorType
ORDER BY ErrorCount DESC;

-- 6. Data Quality Metrics
SELECT 
    'Total Records' as Metric,
    COUNT(*) as Value
FROM SalesData
UNION ALL
SELECT 
    'Records with SaleAmount' as Metric,
    COUNT(*) as Value
FROM SalesData
WHERE SaleAmount IS NOT NULL
UNION ALL
SELECT 
    'Records with Product Info' as Metric,
    COUNT(*) as Value
FROM SalesData
WHERE ProductName IS NOT NULL
UNION ALL
SELECT 
    'Records with Customer Info' as Metric,
    COUNT(*) as Value
FROM SalesData
WHERE CustomerID IS NOT NULL AND CustomerID != 'UNKNOWN';

-- 7. Regional Performance Comparison
SELECT 
    Region,
    COUNT(*) as TotalOrders,
    SUM(SaleAmount) as TotalSales,
    AVG(SaleAmount) as AverageOrderValue,
    SUM(SaleAmount) / SUM(COUNT(*)) OVER() * 100 as SalesPercentage
FROM SalesData
WHERE SaleAmount IS NOT NULL
GROUP BY Region
ORDER BY TotalSales DESC;

-- 8. Discount Analysis
SELECT 
    CASE 
        WHEN Discount = 0 THEN 'No Discount'
        WHEN Discount <= 0.1 THEN 'Low Discount (0-10%)'
        WHEN Discount <= 0.2 THEN 'Medium Discount (10-20%)'
        ELSE 'High Discount (>20%)'
    END as DiscountCategory,
    COUNT(*) as OrderCount,
    SUM(SaleAmount) as TotalSales,
    AVG(SaleAmount) as AverageOrderValue,
    AVG(Discount) as AverageDiscount
FROM SalesData
WHERE SaleAmount IS NOT NULL AND Discount IS NOT NULL
GROUP BY 
    CASE 
        WHEN Discount = 0 THEN 'No Discount'
        WHEN Discount <= 0.1 THEN 'Low Discount (0-10%)'
        WHEN Discount <= 0.2 THEN 'Medium Discount (10-20%)'
        ELSE 'High Discount (>20%)'
    END
ORDER BY TotalSales DESC;

-- 9. Customer Analysis
SELECT 
    CustomerID,
    COUNT(*) as OrderCount,
    SUM(SaleAmount) as TotalSpent,
    AVG(SaleAmount) as AverageOrderValue,
    MIN(OrderDate) as FirstOrder,
    MAX(OrderDate) as LastOrder,
    DATEDIFF(day, MIN(OrderDate), MAX(OrderDate)) as CustomerLifetime
FROM SalesData
WHERE SaleAmount IS NOT NULL AND CustomerID IS NOT NULL AND CustomerID != 'UNKNOWN'
GROUP BY CustomerID
HAVING COUNT(*) > 1
ORDER BY TotalSpent DESC;

-- 10. Data Processing Timeline
SELECT 
    CAST(ProcessingTimestamp AS DATE) as ProcessingDate,
    COUNT(*) as RecordsProcessed,
    MIN(ProcessingTimestamp) as FirstProcessed,
    MAX(ProcessingTimestamp) as LastProcessed
FROM SalesData
GROUP BY CAST(ProcessingTimestamp AS DATE)
ORDER BY ProcessingDate DESC;

-- 11. Currency Conversion Summary
SELECT 
    FromCurrency,
    ToCurrency,
    COUNT(*) as ConversionCount,
    AVG(ExchangeRate) as AverageRate,
    MIN(ConversionTimestamp) as FirstConversion,
    MAX(ConversionTimestamp) as LastConversion
FROM CurrencyConversionLog
GROUP BY FromCurrency, ToCurrency
ORDER BY ConversionCount DESC;

-- 12. Data Validation Summary
SELECT 
    'Total Records Processed' as Metric,
    (SELECT COUNT(*) FROM SalesData) as Value
UNION ALL
SELECT 
    'Records with Errors' as Metric,
    (SELECT COUNT(*) FROM ErrorLogging) as Value
UNION ALL
SELECT 
    'Error Rate (%)' as Metric,
    CAST(
        (SELECT COUNT(*) FROM ErrorLogging) * 100.0 / 
        (SELECT COUNT(*) FROM SalesData) as DECIMAL(5,2)
    ) as Value
UNION ALL
SELECT 
    'Currency Conversions' as Metric,
    (SELECT COUNT(*) FROM CurrencyConversionLog) as Value;

-- 13. Recent Activity (Last 7 days)
SELECT 
    'Recent Sales' as ActivityType,
    COUNT(*) as Count,
    SUM(SaleAmount) as TotalValue
FROM SalesData
WHERE OrderDate >= DATEADD(day, -7, GETDATE())
UNION ALL
SELECT 
    'Recent Errors' as ActivityType,
    COUNT(*) as Count,
    0 as TotalValue
FROM ErrorLogging
WHERE ErrorTimestamp >= DATEADD(day, -7, GETDATE())
UNION ALL
SELECT 
    'Recent Conversions' as ActivityType,
    COUNT(*) as Count,
    0 as TotalValue
FROM CurrencyConversionLog
WHERE ConversionTimestamp >= DATEADD(day, -7, GETDATE());

-- 14. Product Category Performance
SELECT 
    Category,
    COUNT(*) as TotalOrders,
    SUM(SaleAmount) as TotalSales,
    AVG(SaleAmount) as AverageOrderValue,
    COUNT(DISTINCT CustomerID) as UniqueCustomers,
    COUNT(DISTINCT Region) as RegionsServed
FROM SalesData
WHERE SaleAmount IS NOT NULL AND Category IS NOT NULL
GROUP BY Category
ORDER BY TotalSales DESC;

-- 15. Data Completeness Analysis
SELECT 
    'Complete Records' as Completeness,
    COUNT(*) as RecordCount
FROM SalesData
WHERE SaleAmount IS NOT NULL 
    AND ProductName IS NOT NULL 
    AND CustomerID IS NOT NULL 
    AND CustomerID != 'UNKNOWN'
UNION ALL
SELECT 
    'Partial Records' as Completeness,
    COUNT(*) as RecordCount
FROM SalesData
WHERE SaleAmount IS NOT NULL 
    AND (ProductName IS NULL OR CustomerID IS NULL OR CustomerID = 'UNKNOWN')
UNION ALL
SELECT 
    'Incomplete Records' as Completeness,
    COUNT(*) as RecordCount
FROM SalesData
WHERE SaleAmount IS NULL; 