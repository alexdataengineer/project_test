from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging
from datetime import datetime, timedelta
import os

class DataPipeline:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("DataPipeline") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
        
        self.setup_logging()
        
    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        
    def load_raw_data(self):
        """Load the three CSV files with raw_ prefix"""
        self.logger.info("Loading raw data files...")
        
        # Load products data
        self.raw_products = self.spark.read \
            .option("header", "true") \
            .option("inferSchema", "false") \
            .csv("tables/products (1).csv")
            
        # Load sales order header data
        self.raw_sales_order_header = self.spark.read \
            .option("header", "true") \
            .option("inferSchema", "false") \
            .csv("tables/sales_order_header (1).csv")
            
        # Load sales order detail data
        self.raw_sales_order_detail = self.spark.read \
            .option("header", "true") \
            .option("inferSchema", "false") \
            .csv("tables/sales_order_detail (1).csv")
            
        self.logger.info("Raw data loaded successfully")
        
    def assign_data_types(self):
        """Assign appropriate data types to all tables"""
        self.logger.info("Assigning data types to raw tables...")
        
        # Products table schema
        products_schema = StructType([
            StructField("ProductID", IntegerType(), False),
            StructField("ProductDesc", StringType(), True),
            StructField("ProductNumber", StringType(), True),
            StructField("MakeFlag", BooleanType(), True),
            StructField("Color", StringType(), True),
            StructField("SafetyStockLevel", IntegerType(), True),
            StructField("ReorderPoint", IntegerType(), True),
            StructField("StandardCost", DecimalType(10, 4), True),
            StructField("ListPrice", DecimalType(10, 4), True),
            StructField("Size", StringType(), True),
            StructField("SizeUnitMeasureCode", StringType(), True),
            StructField("Weight", DecimalType(8, 2), True),
            StructField("WeightUnitMeasureCode", StringType(), True),
            StructField("ProductCategoryName", StringType(), True),
            StructField("ProductSubCategoryName", StringType(), True)
        ])
        
        # Sales order header schema
        header_schema = StructType([
            StructField("SalesOrderID", IntegerType(), False),
            StructField("OrderDate", StringType(), True),
            StructField("ShipDate", StringType(), True),
            StructField("OnlineOrderFlag", BooleanType(), True),
            StructField("AccountNumber", StringType(), True),
            StructField("CustomerID", IntegerType(), True),
            StructField("SalesPersonID", StringType(), True),
            StructField("Freight", DecimalType(10, 4), True)
        ])
        
        # Sales order detail schema
        detail_schema = StructType([
            StructField("SalesOrderID", IntegerType(), False),
            StructField("SalesOrderDetailID", IntegerType(), False),
            StructField("OrderQty", IntegerType(), True),
            StructField("ProductID", IntegerType(), True),
            StructField("UnitPrice", DecimalType(10, 4), True),
            StructField("UnitPriceDiscount", DecimalType(10, 4), True)
        ])
        
        # Apply schemas
        self.store_products = self.raw_products.select(
            col("ProductID").cast("int"),
            col("ProductDesc").cast("string"),
            col("ProductNumber").cast("string"),
            col("MakeFlag").cast("boolean"),
            col("Color").cast("string"),
            col("SafetyStockLevel").cast("int"),
            col("ReorderPoint").cast("int"),
            col("StandardCost").cast("decimal(10,4)"),
            col("ListPrice").cast("decimal(10,4)"),
            col("Size").cast("string"),
            col("SizeUnitMeasureCode").cast("string"),
            col("Weight").cast("decimal(8,2)"),
            col("WeightUnitMeasureCode").cast("string"),
            col("ProductCategoryName").cast("string"),
            col("ProductSubCategoryName").cast("string")
        )
        
        self.store_sales_order_header = self.raw_sales_order_header.select(
            col("SalesOrderID").cast("int"),
            col("OrderDate").cast("string"),
            col("ShipDate").cast("string"),
            col("OnlineOrderFlag").cast("boolean"),
            col("AccountNumber").cast("string"),
            col("CustomerID").cast("int"),
            col("SalesPersonID").cast("string"),
            col("Freight").cast("decimal(10,4)")
        )
        
        self.store_sales_order_detail = self.raw_sales_order_detail.select(
            col("SalesOrderID").cast("int"),
            col("SalesOrderDetailID").cast("int"),
            col("OrderQty").cast("int"),
            col("ProductID").cast("int"),
            col("UnitPrice").cast("decimal(10,4)"),
            col("UnitPriceDiscount").cast("decimal(10,4)")
        )
        
        self.logger.info("Data types assigned successfully")
        
    def transform_products(self):
        """Transform products data according to requirements"""
        self.logger.info("Transforming products data...")
        
        # Replace NULL values in Color field with N/A
        # Enhance ProductCategoryName based on ProductSubCategoryName
        self.publish_product = self.store_products.withColumn(
            "Color", 
            when(col("Color").isNull() | (col("Color") == ""), "N/A").otherwise(col("Color"))
        ).withColumn(
            "ProductCategoryName",
            when(col("ProductCategoryName").isNull() | (col("ProductCategoryName") == ""),
                when(col("ProductSubCategoryName").isin("Gloves", "Shorts", "Socks", "Tights", "Vests"), "Clothing")
                .when(col("ProductSubCategoryName").isin("Locks", "Lights", "Headsets", "Helmets", "Pedals", "Pumps"), "Accessories")
                .when(col("ProductSubCategoryName").contains("Frames") | col("ProductSubCategoryName").isin("Wheels", "Saddles"), "Components")
                .otherwise(col("ProductCategoryName"))
            ).otherwise(col("ProductCategoryName"))
        )
        
        self.logger.info("Products transformation completed")
        
    def transform_sales_orders(self):
        """Transform sales orders data according to requirements"""
        self.logger.info("Transforming sales orders data...")
        
        # Join SalesOrderDetail with SalesOrderHeader
        joined_orders = self.store_sales_order_detail.join(
            self.store_sales_order_header,
            "SalesOrderID",
            "inner"
        )
        
        # Calculate LeadTimeInBusinessDays (excluding weekends)
        def calculate_business_days(order_date, ship_date):
            if order_date is None or ship_date is None:
                return None
            
            try:
                order_dt = datetime.strptime(order_date, "%Y-%m-%d")
                ship_dt = datetime.strptime(ship_date, "%Y-%m-%d")
                
                business_days = 0
                current_date = order_dt
                
                while current_date <= ship_dt:
                    if current_date.weekday() < 5:  # Monday = 0, Friday = 4
                        business_days += 1
                    current_date += timedelta(days=1)
                
                return business_days
            except:
                return None
        
        business_days_udf = udf(calculate_business_days, IntegerType())
        
        # Calculate TotalLineExtendedPrice
        self.publish_orders = joined_orders.withColumn(
            "LeadTimeInBusinessDays",
            business_days_udf(col("OrderDate"), col("ShipDate"))
        ).withColumn(
            "TotalLineExtendedPrice",
            col("OrderQty") * (col("UnitPrice") - col("UnitPriceDiscount"))
        ).select(
            # All fields from SalesOrderDetail
            col("SalesOrderID"),
            col("SalesOrderDetailID"),
            col("OrderQty"),
            col("ProductID"),
            col("UnitPrice"),
            col("UnitPriceDiscount"),
            # All fields from SalesOrderHeader except SalesOrderID, rename Freight
            col("OrderDate"),
            col("ShipDate"),
            col("OnlineOrderFlag"),
            col("AccountNumber"),
            col("CustomerID"),
            col("SalesPersonID"),
            col("Freight").alias("TotalOrderFreight"),
            # Calculated fields
            col("LeadTimeInBusinessDays"),
            col("TotalLineExtendedPrice")
        )
        
        self.logger.info("Sales orders transformation completed")
        
    def save_transformed_data(self):
        """Save all transformed data"""
        self.logger.info("Saving transformed data...")
        
        # Save store_ tables
        self.store_products.write.mode("overwrite").parquet("output/store_products")
        self.store_sales_order_header.write.mode("overwrite").parquet("output/store_sales_order_header")
        self.store_sales_order_detail.write.mode("overwrite").parquet("output/store_sales_order_detail")
        
        # Save publish_ tables
        self.publish_product.write.mode("overwrite").parquet("output/publish_product")
        self.publish_orders.write.mode("overwrite").parquet("output/publish_orders")
        
        self.logger.info("Transformed data saved successfully")
        
    def analyze_data(self):
        """Perform analysis questions"""
        self.logger.info("Performing data analysis...")
        
        # Join orders with products to get color information
        orders_with_products = self.publish_orders.join(
            self.publish_product,
            "ProductID",
            "inner"
        )
        
        # Question 1: Which color generated the highest revenue each year?
        revenue_by_color_year = orders_with_products.groupBy(
            year(col("OrderDate")).alias("Year"),
            col("Color")
        ).agg(
            sum(col("TotalLineExtendedPrice")).alias("TotalRevenue")
        ).orderBy("Year", col("TotalRevenue").desc())
        
        # Get the highest revenue color for each year
        window_spec = Window.partitionBy("Year").orderBy(col("TotalRevenue").desc())
        top_color_by_year = revenue_by_color_year.withColumn(
            "rank", rank().over(window_spec)
        ).filter(col("rank") == 1).drop("rank")
        
        # Question 2: Average LeadTimeInBusinessDays by ProductCategoryName
        avg_lead_time_by_category = orders_with_products.groupBy(
            col("ProductCategoryName")
        ).agg(
            avg(col("LeadTimeInBusinessDays")).alias("AverageLeadTimeInBusinessDays")
        ).orderBy("AverageLeadTimeInBusinessDays")
        
        # Save analysis results
        top_color_by_year.write.mode("overwrite").parquet("output/analysis_top_color_by_year")
        avg_lead_time_by_category.write.mode("overwrite").parquet("output/analysis_avg_lead_time_by_category")
        
        # Display results
        self.logger.info("Analysis Results:")
        self.logger.info("Top revenue color by year:")
        top_color_by_year.show()
        
        self.logger.info("Average lead time by product category:")
        avg_lead_time_by_category.show()
        
        self.logger.info("Analysis completed")
        
    def run_pipeline(self):
        """Execute the complete data pipeline"""
        try:
            self.logger.info("Starting data pipeline execution...")
            
            # Create output directory
            os.makedirs("output", exist_ok=True)
            
            # Execute pipeline steps
            self.load_raw_data()
            self.assign_data_types()
            self.transform_products()
            self.transform_sales_orders()
            self.save_transformed_data()
            self.analyze_data()
            
            self.logger.info("Data pipeline completed successfully")
            
        except Exception as e:
            self.logger.error(f"Pipeline failed: {str(e)}")
            raise
        finally:
            self.spark.stop()

if __name__ == "__main__":
    pipeline = DataPipeline()
    pipeline.run_pipeline() 