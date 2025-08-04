# Data Pipeline Technical Assessment

## Solution Overview

This data pipeline solution processes three CSV files containing product and sales order data, applying transformations and analysis as specified in the technical requirements.

## Technical Implementation Summary

### Data Processing Technologies
- **PySpark**: Primary framework for distributed data processing with Spark SQL for complex transformations
- **Pandas**: Alternative implementation for single-node processing with DataFrame operations
- **Parquet Format**: Columnar storage format for optimized data access and compression

### Key Technical Techniques Applied

**Data Engineering Patterns:**
- **Bronze-Silver-Gold Architecture**: Implemented with `raw_`, `store_`, and `publish_` prefixes
- **Schema Enforcement**: Explicit data type assignments and primary/foreign key identification
- **Data Quality Checks**: NULL value handling and data validation procedures

**Business Logic Implementation:**
- **Date Calculations**: Business day logic excluding weekends using date arithmetic
- **Category Mapping**: Conditional logic for product category enhancement based on subcategories
- **Revenue Calculations**: Aggregated sales analysis with proper grouping and filtering

**Performance Optimizations:**
- **Adaptive Query Execution**: Spark configurations for dynamic partition pruning
- **Columnar Storage**: Parquet format for efficient analytical queries
- **Memory Management**: Optimized DataFrame operations and caching strategies

**Code Architecture:**
- **Object-Oriented Design**: Modular classes with clear separation of concerns
- **Configuration Management**: External config files for environment-specific settings
- **Error Handling**: Comprehensive logging and exception management
- **Testing Framework**: Unit tests for data transformations and business logic

## Architecture

The solution implements a PySpark-based data pipeline with the following components:

- **Data Loading**: Loads raw CSV files with appropriate prefixes
- **Data Type Assignment**: Assigns proper data types and identifies primary/foreign keys
- **Data Transformation**: Applies business logic transformations to products and sales orders
- **Data Storage**: Saves transformed data in Parquet format
- **Data Analysis**: Performs analytical queries on the transformed data

## Data Flow

1. **Raw Data Loading**: Three CSV files loaded with `raw_` prefix
2. **Data Type Assignment**: Proper schemas applied with `store_` prefix
3. **Product Transformations**: 
   - NULL Color values replaced with "N/A"
   - ProductCategoryName enhanced based on ProductSubCategoryName logic
4. **Sales Order Transformations**:
   - SalesOrderDetail joined with SalesOrderHeader
   - LeadTimeInBusinessDays calculated (excluding weekends)
   - TotalLineExtendedPrice calculated
5. **Data Storage**: Transformed data saved with `publish_` prefix
6. **Analysis**: Revenue analysis by color/year and lead time analysis by category

## Key Features

- **Business Logic Implementation**: Complete implementation of product category enhancement and business day calculations
- **Data Quality**: Proper handling of NULL values and data type conversions
- **Performance Optimization**: Spark configurations for adaptive query execution
- **Error Handling**: Comprehensive logging and exception handling
- **Modular Design**: Object-oriented approach with clear separation of concerns

## Output Structure

- `store_products`: Products with assigned data types
- `store_sales_order_header`: Sales order headers with proper schemas
- `store_sales_order_detail`: Sales order details with proper schemas
- `publish_product`: Transformed products with enhanced categories
- `publish_orders`: Joined and calculated sales order data
- `analysis_top_color_by_year`: Revenue analysis results
- `analysis_avg_lead_time_by_category`: Lead time analysis results

## Running the Pipeline

### Primary Implementation - PySpark Version
The main pipeline is implemented in `data_pipeline.py` using PySpark for distributed processing:

```bash
# Install required dependencies
python3 -m pip install pyspark pandas numpy

# Run the main PySpark pipeline
python3 data_pipeline.py
```

### Alternative Implementation - Pandas Version
For environments without Java/Spark setup, use `data_pipeline_pandas.py` as an alternative:

```bash
# Run the pandas-based alternative
python3 data_pipeline_pandas.py
```

### Quick Demo
For a complete demonstration of the pipeline:

```bash
./demo_pipeline.sh
```

## Results

The pipeline successfully processes all requirements and generates comprehensive analysis results:

- **Product Data Enhancement**: Complete product categorization with NULL value handling
- **Sales Order Processing**: Business day calculations and revenue analysis
- **Analytical Insights**: Revenue trends by color/year and lead time analysis by category
- **Data Quality**: Proper schema enforcement and data type validation

All transformations are applied according to business specifications and output in optimized Parquet format for efficient data access and analysis.