"""
Data schemas for the ETL pipeline.
Defines the structure and validation rules for input and output data.
"""

from datetime import datetime
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field, validator
from enum import Enum


class Currency(str, Enum):
    """Supported currencies."""
    USD = "USD"
    EUR = "EUR"
    GBP = "GBP"


class Region(str, Enum):
    """Valid regions."""
    EAST = "East"
    WEST = "West"
    NORTH = "North"
    SOUTH = "South"


class Category(str, Enum):
    """Product categories."""
    ELECTRONICS = "Electronics"
    ACCESSORIES = "Accessories"
    OFFICE_SUPPLIES = "Office Supplies"


class RawSalesRecord(BaseModel):
    """Schema for raw sales data records."""
    
    OrderID: str = Field(..., description="Unique order identifier")
    ProductID: str = Field(..., description="Product identifier")
    SaleAmount: Optional[float] = Field(None, description="Sale amount")
    OrderDate: Optional[str] = Field(None, description="Order date in various formats")
    Region: Optional[str] = Field(None, description="Sales region")
    CustomerID: Optional[str] = Field(None, description="Customer identifier")
    Discount: Optional[float] = Field(None, description="Discount percentage")
    Currency: Optional[str] = Field(None, description="Currency code")
    
    @validator('SaleAmount')
    def validate_sale_amount(cls, v):
        """Validate sale amount is within reasonable bounds."""
        if v is not None:
            if v < -1000 or v > 10000:
                raise ValueError('Sale amount must be between -1000 and 10000')
        return v
    
    @validator('Discount')
    def validate_discount(cls, v):
        """Validate discount is between 0 and 1."""
        if v is not None:
            if v < 0 or v > 1:
                raise ValueError('Discount must be between 0 and 1')
        return v
    
    @validator('Currency')
    def validate_currency(cls, v):
        """Validate currency code."""
        if v is not None and v not in [c.value for c in Currency]:
            raise ValueError(f'Invalid currency: {v}')
        return v
    
    @validator('Region')
    def validate_region(cls, v):
        """Validate region."""
        if v is not None and v not in [r.value for r in Region]:
            raise ValueError(f'Invalid region: {v}')
        return v


class ProductReference(BaseModel):
    """Schema for product reference data."""
    
    ProductID: str = Field(..., description="Product identifier")
    ProductName: str = Field(..., description="Product name")
    Category: str = Field(..., description="Product category")
    
    @validator('Category')
    def validate_category(cls, v):
        """Validate product category."""
        if v not in [c.value for c in Category]:
            raise ValueError(f'Invalid category: {v}')
        return v


class CleanedSalesRecord(BaseModel):
    """Schema for cleaned and enriched sales data."""
    
    OrderID: str = Field(..., description="Unique order identifier")
    ProductID: str = Field(..., description="Product identifier")
    ProductName: Optional[str] = Field(None, description="Product name from reference data")
    Category: Optional[str] = Field(None, description="Product category from reference data")
    SaleAmount: float = Field(..., description="Sale amount in USD")
    OriginalSaleAmount: Optional[float] = Field(None, description="Original sale amount")
    OriginalCurrency: Optional[str] = Field(None, description="Original currency")
    ExchangeRate: Optional[float] = Field(None, description="Exchange rate used for conversion")
    OrderDate: datetime = Field(..., description="Parsed order date")
    Region: str = Field(..., description="Sales region")
    CustomerID: Optional[str] = Field(None, description="Customer identifier")
    Discount: Optional[float] = Field(None, description="Discount percentage")
    Currency: str = Field(default="USD", description="Currency (always USD after conversion)")
    ProcessingTimestamp: datetime = Field(default_factory=datetime.now, description="When record was processed")
    
    @validator('SaleAmount')
    def validate_sale_amount(cls, v):
        """Validate sale amount is within reasonable bounds."""
        if v < -1000 or v > 10000:
            raise ValueError('Sale amount must be between -1000 and 10000')
        return v
    
    @validator('Discount')
    def validate_discount(cls, v):
        """Validate discount is between 0 and 1."""
        if v is not None:
            if v < 0 or v > 1:
                raise ValueError('Discount must be between 0 and 1')
        return v


class RejectedRecord(BaseModel):
    """Schema for rejected records with error details."""
    
    OrderID: Optional[str] = Field(None, description="Order identifier if available")
    ProductID: Optional[str] = Field(None, description="Product identifier if available")
    RawData: Dict[str, Any] = Field(..., description="Original raw data")
    ErrorType: str = Field(..., description="Type of validation error")
    ErrorMessage: str = Field(..., description="Detailed error message")
    RejectionTimestamp: datetime = Field(default_factory=datetime.now, description="When record was rejected")
    ProcessingStage: str = Field(..., description="Pipeline stage where rejection occurred")


class CurrencyConversionLog(BaseModel):
    """Schema for currency conversion logging."""
    
    FromCurrency: str = Field(..., description="Source currency")
    ToCurrency: str = Field(..., description="Target currency")
    ExchangeRate: float = Field(..., description="Exchange rate used")
    ConversionTimestamp: datetime = Field(default_factory=datetime.now, description="When conversion occurred")
    Source: str = Field(..., description="Source of exchange rate (API/cache/fallback)")
    RecordCount: int = Field(..., description="Number of records converted with this rate")


class PipelineMetrics(BaseModel):
    """Schema for pipeline execution metrics."""
    
    TotalRecords: int = Field(..., description="Total records processed")
    ValidRecords: int = Field(..., description="Number of valid records")
    RejectedRecords: int = Field(..., description="Number of rejected records")
    ErrorRate: float = Field(..., description="Error rate as percentage")
    ProcessingTimeSeconds: float = Field(..., description="Total processing time")
    CurrencyConversions: int = Field(..., description="Number of currency conversions performed")
    DuplicatesRemoved: int = Field(..., description="Number of duplicate records removed")
    StartTimestamp: datetime = Field(..., description="Pipeline start time")
    EndTimestamp: datetime = Field(..., description="Pipeline end time")


class ValidationRule(BaseModel):
    """Schema for data validation rules."""
    
    FieldName: str = Field(..., description="Field to validate")
    RuleType: str = Field(..., description="Type of validation rule")
    RuleValue: Any = Field(..., description="Value or parameters for the rule")
    ErrorMessage: str = Field(..., description="Error message for failed validation")
    IsRequired: bool = Field(default=True, description="Whether field is required")


class DatabaseSchema(BaseModel):
    """Schema for database table definitions."""
    
    TableName: str = Field(..., description="Database table name")
    Columns: List[Dict[str, Any]] = Field(..., description="Column definitions")
    PrimaryKey: Optional[str] = Field(None, description="Primary key column")
    Indexes: List[Dict[str, Any]] = Field(default=[], description="Index definitions")
    Constraints: List[Dict[str, Any]] = Field(default=[], description="Constraint definitions")


# Predefined validation rules
DEFAULT_VALIDATION_RULES = [
    ValidationRule(
        FieldName="OrderID",
        RuleType="required",
        RuleValue=True,
        ErrorMessage="OrderID is required",
        IsRequired=True
    ),
    ValidationRule(
        FieldName="ProductID",
        RuleType="required",
        RuleValue=True,
        ErrorMessage="ProductID is required",
        IsRequired=True
    ),
    ValidationRule(
        FieldName="SaleAmount",
        RuleType="range",
        RuleValue={"min": -1000, "max": 10000},
        ErrorMessage="SaleAmount must be between -1000 and 10000",
        IsRequired=False
    ),
    ValidationRule(
        FieldName="OrderDate",
        RuleType="date_format",
        RuleValue=["%d/%m/%Y", "%d-%m-%Y", "%Y-%d-%m"],
        ErrorMessage="OrderDate must be in valid date format",
        IsRequired=False
    ),
    ValidationRule(
        FieldName="Region",
        RuleType="enum",
        RuleValue=[r.value for r in Region],
        ErrorMessage="Region must be one of: East, West, North, South",
        IsRequired=False
    ),
    ValidationRule(
        FieldName="Currency",
        RuleType="enum",
        RuleValue=[c.value for c in Currency],
        ErrorMessage="Currency must be one of: USD, EUR, GBP",
        IsRequired=False
    ),
    ValidationRule(
        FieldName="Discount",
        RuleType="range",
        RuleValue={"min": 0, "max": 1},
        ErrorMessage="Discount must be between 0 and 1",
        IsRequired=False
    )
] 