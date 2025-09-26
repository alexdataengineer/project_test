# Data Pipeline Architecture Diagram

## Mermaid Diagram

```mermaid
graph TB
    %% Raw Data Layer
    subgraph "Layer 1: RAW DATA"
        CSV1["products.csv<br/>303 records<br/>15 columns"]
        CSV2["sales_order_header.csv<br/>31,465 records<br/>8 columns"]
        CSV3["sales_order_detail.csv<br/>121,317 records<br/>6 columns"]
    end

    %% Data Loading Process
    LOAD["Data Loading Process<br/>raw_ prefix applied"]

    %% Store Layer
    subgraph "Layer 2: STORE DATA"
        STORE1["store_products<br/>303 records<br/>Proper data types"]
        STORE2["store_sales_order_header<br/>31,465 records<br/>Schema enforced"]
        STORE3["store_sales_order_detail<br/>121,317 records<br/>PKs/FKs identified"]
    end

    %% Transformation Process
    TRANSFORM["Data Type Assignment<br/>& Validation"]

    %% Publish Layer
    subgraph "Layer 3: PUBLISH DATA"
        PUB1["publish_product<br/>303 records<br/>• NULL Color → 'N/A' (50 products)<br/>• Category enhancement (144 assignments)<br/>• Components: 145, Clothing: 53<br/>• Bikes: 32, Accessories: 27"]
        PUB2["publish_orders<br/>121,317 records<br/>• LeadTimeInBusinessDays calculated<br/>• TotalLineExtendedPrice calculated<br/>• Total Revenue: $110,230,153.63"]
    end

    %% Business Logic Transformations
    PROD_TRANSFORM["Product Transformations<br/>• NULL handling<br/>• Category mapping logic"]
    SALES_TRANSFORM["Sales Order Transformations<br/>• Join operations<br/>• Business day calculations<br/>• Revenue calculations"]

    %% Analysis Layer
    subgraph "Layer 4: ANALYSIS DATA"
        ANALYSIS1["analysis_top_color_by_year<br/>Revenue by color/year<br/>• 2021: Red ($6M)<br/>• 2022: Black ($14M)<br/>• 2023: Black ($15M)<br/>• 2024: Yellow ($6M)"]
        ANALYSIS2["analysis_avg_lead_time_by_category<br/>Lead time analysis<br/>• Bikes: 5.67 days<br/>• Components: 5.67 days<br/>• Accessories: 5.70 days<br/>• Clothing: 5.71 days"]
    end

    %% Analysis Process
    ANALYZE["Business Analysis<br/>• Revenue trends<br/>• Lead time insights"]

    %% Data Flow
    CSV1 --> LOAD
    CSV2 --> LOAD
    CSV3 --> LOAD
    
    LOAD --> TRANSFORM
    
    TRANSFORM --> STORE1
    TRANSFORM --> STORE2
    TRANSFORM --> STORE3
    
    STORE1 --> PROD_TRANSFORM
    STORE2 --> SALES_TRANSFORM
    STORE3 --> SALES_TRANSFORM
    
    PROD_TRANSFORM --> PUB1
    SALES_TRANSFORM --> PUB2
    
    PUB1 --> ANALYZE
    PUB2 --> ANALYZE
    
    ANALYZE --> ANALYSIS1
    ANALYZE --> ANALYSIS2

    %% Styling
    classDef rawData fill:#e1f5fe,stroke:#01579b,stroke-width:2px
    classDef storeData fill:#f3e5f5,stroke:#4a148c,stroke-width:2px
    classDef publishData fill:#e8f5e8,stroke:#1b5e20,stroke-width:2px
    classDef analysisData fill:#fff3e0,stroke:#e65100,stroke-width:2px
    classDef process fill:#fce4ec,stroke:#880e4f,stroke-width:2px

    class CSV1,CSV2,CSV3 rawData
    class STORE1,STORE2,STORE3 storeData
    class PUB1,PUB2 publishData
    class ANALYSIS1,ANALYSIS2 analysisData
    class LOAD,TRANSFORM,PROD_TRANSFORM,SALES_TRANSFORM,ANALYZE process
```

## Architecture Overview

### Data Flow Summary
1. **Raw Data Ingestion**: Three CSV files loaded with raw_ prefix
2. **Data Validation**: Proper data types assigned and schemas enforced
3. **Business Transformations**: Product categorization and sales calculations
4. **Analytical Insights**: Revenue and lead time analysis generated

### Key Metrics
- **Total Processing Time**: 43 seconds
- **Data Quality**: 100% successful processing
- **Storage Format**: Optimized Parquet files
- **Total Records Processed**: 152,685 raw → 121,620 transformed

### Technical Implementation
- **Primary**: PySpark for distributed processing
- **Alternative**: Pandas for single-node environments
- **Storage**: Columnar Parquet format for analytics
- **Validation**: Comprehensive error handling and logging
