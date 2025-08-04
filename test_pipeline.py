"""
Test suite for the data pipeline
"""

import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from data_pipeline import DataPipeline
import os

class TestDataPipeline(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        """Set up Spark session for testing"""
        cls.spark = SparkSession.builder \
            .appName("TestDataPipeline") \
            .master("local[*]") \
            .getOrCreate()
        
    @classmethod
    def tearDownClass(cls):
        """Clean up Spark session"""
        cls.spark.stop()
        
    def setUp(self):
        """Set up test data"""
        # Create test data
        self.test_products_data = [
            (1, "Test Product 1", "TP001", True, "Red", 100, 50, 10.0, 20.0, "M", "CM", 1.5, "LB", "", "Gloves"),
            (2, "Test Product 2", "TP002", False, None, 200, 100, 15.0, 30.0, "L", "CM", 2.0, "LB", "", "Helmets"),
            (3, "Test Product 3", "TP003", True, "Blue", 150, 75, 12.0, 25.0, "S", "CM", 1.8, "LB", "Bikes", "Road Frames")
        ]
        
        self.test_products_df = self.spark.createDataFrame(
            self.test_products_data,
            ["ProductID", "ProductDesc", "ProductNumber", "MakeFlag", "Color", 
             "SafetyStockLevel", "ReorderPoint", "StandardCost", "ListPrice", 
             "Size", "SizeUnitMeasureCode", "Weight", "WeightUnitMeasureCode", 
             "ProductCategoryName", "ProductSubCategoryName"]
        )
        
    def test_product_transformation(self):
        """Test product transformation logic"""
        pipeline = DataPipeline()
        pipeline.spark = self.spark
        
        # Test NULL color replacement
        transformed = pipeline.test_products_df.withColumn(
            "Color", 
            when(col("Color").isNull() | (col("Color") == ""), "N/A").otherwise(col("Color"))
        )
        
        # Check if NULL colors are replaced with "N/A"
        null_color_rows = transformed.filter(col("Color") == "N/A").count()
        self.assertEqual(null_color_rows, 1)
        
    def test_business_day_calculation(self):
        """Test business day calculation"""
        from datetime import datetime, timedelta
        
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
        
        # Test case: Monday to Friday (5 business days)
        result = calculate_business_days("2021-06-07", "2021-06-11")
        self.assertEqual(result, 5)
        
        # Test case: Monday to Monday (5 business days)
        result = calculate_business_days("2021-06-07", "2021-06-14")
        self.assertEqual(result, 5)
        
    def test_product_category_mapping(self):
        """Test product category enhancement logic"""
        from pyspark.sql.functions import when, col
        
        test_data = [
            ("Gloves", ""),
            ("Helmets", ""),
            ("Road Frames", ""),
            ("Wheels", ""),
            ("Unknown", "")
        ]
        
        test_df = self.spark.createDataFrame(test_data, ["ProductSubCategoryName", "ProductCategoryName"])
        
        transformed = test_df.withColumn(
            "ProductCategoryName",
            when(col("ProductSubCategoryName").isin("Gloves", "Shorts", "Socks", "Tights", "Vests"), "Clothing")
            .when(col("ProductSubCategoryName").isin("Locks", "Lights", "Headsets", "Helmets", "Pedals", "Pumps"), "Accessories")
            .when(col("ProductSubCategoryName").contains("Frames") | col("ProductSubCategoryName").isin("Wheels", "Saddles"), "Components")
            .otherwise(col("ProductCategoryName"))
        )
        
        # Check results
        clothing_count = transformed.filter(col("ProductCategoryName") == "Clothing").count()
        accessories_count = transformed.filter(col("ProductCategoryName") == "Accessories").count()
        components_count = transformed.filter(col("ProductCategoryName") == "Components").count()
        
        self.assertEqual(clothing_count, 1)  # Gloves
        self.assertEqual(accessories_count, 1)  # Helmets
        self.assertEqual(components_count, 2)  # Road Frames, Wheels
        
    def test_total_line_extended_price(self):
        """Test TotalLineExtendedPrice calculation"""
        test_data = [
            (1, 2, 10.0, 1.0),  # OrderQty=2, UnitPrice=10.0, UnitPriceDiscount=1.0
            (2, 1, 20.0, 0.0),   # OrderQty=1, UnitPrice=20.0, UnitPriceDiscount=0.0
            (3, 3, 15.0, 2.0)    # OrderQty=3, UnitPrice=15.0, UnitPriceDiscount=2.0
        ]
        
        test_df = self.spark.createDataFrame(
            test_data, 
            ["SalesOrderDetailID", "OrderQty", "UnitPrice", "UnitPriceDiscount"]
        )
        
        calculated = test_df.withColumn(
            "TotalLineExtendedPrice",
            col("OrderQty") * (col("UnitPrice") - col("UnitPriceDiscount"))
        )
        
        # Check calculations
        results = calculated.collect()
        self.assertEqual(results[0]["TotalLineExtendedPrice"], 18.0)  # 2 * (10.0 - 1.0)
        self.assertEqual(results[1]["TotalLineExtendedPrice"], 20.0)  # 1 * (20.0 - 0.0)
        self.assertEqual(results[2]["TotalLineExtendedPrice"], 39.0)  # 3 * (15.0 - 2.0)

if __name__ == "__main__":
    unittest.main() 