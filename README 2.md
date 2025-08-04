# Data Pipeline Technical Assessment

## Solution Overview

This data pipeline solution processes three CSV files containing product and sales order data, applying transformations and analysis as specified in the technical requirements.

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

## Execution

### Quick Demo
```bash
./demo_pipeline.sh
```

### Manual Execution
```bash
# Install dependencies
python3 -m pip install pyspark pandas numpy

# Run the pipeline
python3 data_pipeline_pandas.py
```

### Alternative PySpark Version
```bash
# Requires Java 8+ installed
python3 data_pipeline.py
```

The pipeline processes all requirements and generates analysis results for the specified business questions.