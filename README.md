# Data Pipeline Technical Assessment

## Solution Overview

I implemented a comprehensive data pipeline solution that processes three CSV files containing product and sales order data. The solution applies all required transformations and analysis as specified in the technical requirements.

**Main Implementation**: `data_pipeline.py` (PySpark)  
**Alternative Implementation**: `data_pipeline_pandas.py` (for environments without Java/Spark setup)

<img width="560" height="549" alt="image" src="https://github.com/user-attachments/assets/b7bb85f8-f542-4a48-9166-5bcf31b62cf2" />


## Data Architecture by Layer

### Layer 1: RAW DATA (Original CSV files)
I loaded the three provided CSV files with appropriate raw_ prefixes as requested:
- **Products**: 303 records, 15 columns
- **Sales Order Header**: 31,465 records, 8 columns  
- **Sales Order Detail**: 121,317 records, 6 columns
- **Total Raw Records**: 152,685

### Layer 2: STORE DATA (Typed and validated data)
I assigned appropriate data types and identified primary/foreign keys for each table:
- **store_products**: 303 records with proper data types
- **store_sales_order_header**: 31,465 records with proper schemas
- **store_sales_order_detail**: 121,317 records with proper schemas
- **Key Features**: Primary/Foreign key identification, data type enforcement

### Layer 3: PUBLISH DATA (Transformed business data)
I implemented all required business transformations:
- **publish_product**: 303 records with business transformations
  - NULL Color values replaced with "N/A" (50 products processed)
  - ProductCategoryName enhanced using the specified logic (144 assignments made)
  - Final Categories: Components (145), Clothing (53), Bikes (32), Accessories (27)
- **publish_orders**: 121,317 records with calculated fields
  - LeadTimeInBusinessDays calculated excluding weekends
  - TotalLineExtendedPrice calculated using the formula: OrderQty * (UnitPrice - UnitPriceDiscount)
  - Total Revenue Processed: $110,230,153.63

### Layer 4: ANALYSIS DATA (Business insights)
I performed the required analysis questions and generated insights:
- **analysis_top_color_by_year**: Revenue analysis by color/year
  - 2021: Red ($6,019,614.02)
  - 2022: Black ($14,005,242.98)
  - 2023: Black ($15,047,694.37)
  - 2024: Yellow ($6,480,746.07)
- **analysis_avg_lead_time_by_category**: Lead time analysis by category
  - Bikes: 5.67 days
  - Components: 5.67 days
  - Accessories: 5.70 days
  - Clothing: 5.71 days

## Implementation Approach

I structured the pipeline following a layered architecture approach:

1. **Raw Data Loading**: Loaded three CSV files with `raw_` prefix as specified
2. **Data Type Assignment**: Applied proper schemas with `store_` prefix
3. **Product Transformations**: Implemented NULL handling and category enhancement logic
4. **Sales Order Transformations**: Performed joins and business day calculations
5. **Data Storage**: Saved transformed data with `publish_` prefix
6. **Analysis**: Generated revenue and lead time analysis as requested

## Running the Pipeline

### PySpark Version (Primary Implementation)
```bash
python3 data_pipeline.py
```

### Pandas Version (Alternative Implementation)
```bash
python3 data_pipeline_pandas.py
```

### Quick Demo
```bash
./demo_pipeline.sh
```

## Output Structure

### Store Layer
- `store_products.parquet`: Products with assigned data types
- `store_sales_order_header.parquet`: Sales order headers with proper schemas
- `store_sales_order_detail.parquet`: Sales order details with proper schemas

### Publish Layer
- `publish_product.parquet`: Transformed products with enhanced categories
- `publish_orders.parquet`: Joined and calculated sales order data

### Analysis Layer
- `analysis_top_color_by_year.parquet`: Revenue analysis results
- `analysis_avg_lead_time_by_category.parquet`: Lead time analysis results

## Technical Performance

- **Processing Time**: 43 seconds for complete pipeline execution
- **Data Quality**: 100% successful processing with comprehensive validation
- **Storage Format**: Parquet format for optimized analytical queries
- **Error Handling**: Comprehensive logging and exception management

## Requirements Compliance

**Data Loading**: 3 CSV files loaded with `raw_` prefix  
**Data Review**: Proper data types assigned, primary/foreign keys identified  
**Data Storage**: Transformed data saved with `store_` prefix  
**Product Transformations**: NULL handling and category enhancement implemented  
**Sales Order Transformations**: Joins and business day calculations completed  
**Analysis Questions**: Revenue by color/year and lead time by category analysis  
**Output**: All data saved in optimized Parquet format
