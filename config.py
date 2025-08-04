"""
Configuration settings for the data pipeline
"""

# Data file paths
DATA_PATHS = {
    "products": "tables/products (1).csv",
    "sales_order_header": "tables/sales_order_header (1).csv", 
    "sales_order_detail": "tables/sales_order_detail (1).csv"
}

# Output paths
OUTPUT_PATHS = {
    "store_products": "output/store_products",
    "store_sales_order_header": "output/store_sales_order_header",
    "store_sales_order_detail": "output/store_sales_order_detail",
    "publish_product": "output/publish_product",
    "publish_orders": "output/publish_orders",
    "analysis_top_color_by_year": "output/analysis_top_color_by_year",
    "analysis_avg_lead_time_by_category": "output/analysis_avg_lead_time_by_category"
}

# Spark configuration
SPARK_CONFIG = {
    "app_name": "DataPipeline",
    "adaptive_enabled": "true",
    "adaptive_coalesce_partitions": "true"
}

# Product category mapping
PRODUCT_CATEGORY_MAPPING = {
    "clothing": ["Gloves", "Shorts", "Socks", "Tights", "Vests"],
    "accessories": ["Locks", "Lights", "Headsets", "Helmets", "Pedals", "Pumps"],
    "components": ["Wheels", "Saddles"]
}

# Date format for business day calculations
DATE_FORMAT = "%Y-%m-%d"

# Null replacement values
NULL_REPLACEMENTS = {
    "color": "N/A"
} 