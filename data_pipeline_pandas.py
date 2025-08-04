import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
import os

class DataPipelinePandas:
    def __init__(self):
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
        self.raw_products = pd.read_csv("tables/products (1).csv")
            
        # Load sales order header data
        self.raw_sales_order_header = pd.read_csv("tables/sales_order_header (1).csv")
            
        # Load sales order detail data
        self.raw_sales_order_detail = pd.read_csv("tables/sales_order_detail (1).csv")
            
        self.logger.info("Raw data loaded successfully")
        
    def assign_data_types(self):
        """Assign appropriate data types to all tables"""
        self.logger.info("Assigning data types to raw tables...")
        
        # Products table - assign data types
        self.store_products = self.raw_products.copy()
        self.store_products['ProductID'] = pd.to_numeric(self.store_products['ProductID'], errors='coerce')
        self.store_products['SafetyStockLevel'] = pd.to_numeric(self.store_products['SafetyStockLevel'], errors='coerce')
        self.store_products['ReorderPoint'] = pd.to_numeric(self.store_products['ReorderPoint'], errors='coerce')
        self.store_products['StandardCost'] = pd.to_numeric(self.store_products['StandardCost'], errors='coerce')
        self.store_products['ListPrice'] = pd.to_numeric(self.store_products['ListPrice'], errors='coerce')
        self.store_products['Weight'] = pd.to_numeric(self.store_products['Weight'], errors='coerce')
        self.store_products['MakeFlag'] = self.store_products['MakeFlag'].map({'True': True, 'False': False})
        
        # Sales order header - assign data types
        self.store_sales_order_header = self.raw_sales_order_header.copy()
        self.store_sales_order_header['SalesOrderID'] = pd.to_numeric(self.store_sales_order_header['SalesOrderID'], errors='coerce')
        self.store_sales_order_header['CustomerID'] = pd.to_numeric(self.store_sales_order_header['CustomerID'], errors='coerce')
        self.store_sales_order_header['Freight'] = pd.to_numeric(self.store_sales_order_header['Freight'], errors='coerce')
        self.store_sales_order_header['OnlineOrderFlag'] = self.store_sales_order_header['OnlineOrderFlag'].map({'True': True, 'False': False})
        
        # Sales order detail - assign data types
        self.store_sales_order_detail = self.raw_sales_order_detail.copy()
        self.store_sales_order_detail['SalesOrderID'] = pd.to_numeric(self.store_sales_order_detail['SalesOrderID'], errors='coerce')
        self.store_sales_order_detail['SalesOrderDetailID'] = pd.to_numeric(self.store_sales_order_detail['SalesOrderDetailID'], errors='coerce')
        self.store_sales_order_detail['OrderQty'] = pd.to_numeric(self.store_sales_order_detail['OrderQty'], errors='coerce')
        self.store_sales_order_detail['ProductID'] = pd.to_numeric(self.store_sales_order_detail['ProductID'], errors='coerce')
        self.store_sales_order_detail['UnitPrice'] = pd.to_numeric(self.store_sales_order_detail['UnitPrice'], errors='coerce')
        self.store_sales_order_detail['UnitPriceDiscount'] = pd.to_numeric(self.store_sales_order_detail['UnitPriceDiscount'], errors='coerce')
        
        self.logger.info("Data types assigned successfully")
        
    def transform_products(self):
        """Transform products data according to requirements"""
        self.logger.info("Transforming products data...")
        
        self.publish_product = self.store_products.copy()
        
        # Replace NULL values in Color field with N/A
        self.publish_product['Color'] = self.publish_product['Color'].fillna('N/A')
        self.publish_product.loc[self.publish_product['Color'] == '', 'Color'] = 'N/A'
        
        # Enhance ProductCategoryName based on ProductSubCategoryName
        clothing_items = ['Gloves', 'Shorts', 'Socks', 'Tights', 'Vests']
        accessories_items = ['Locks', 'Lights', 'Headsets', 'Helmets', 'Pedals', 'Pumps']
        components_items = ['Wheels', 'Saddles']
        
        # Apply category mapping logic
        mask_null_category = (self.publish_product['ProductCategoryName'].isna()) | (self.publish_product['ProductCategoryName'] == '')
        
        # Clothing category
        clothing_mask = self.publish_product['ProductSubCategoryName'].isin(clothing_items)
        self.publish_product.loc[mask_null_category & clothing_mask, 'ProductCategoryName'] = 'Clothing'
        
        # Accessories category
        accessories_mask = self.publish_product['ProductSubCategoryName'].isin(accessories_items)
        self.publish_product.loc[mask_null_category & accessories_mask, 'ProductCategoryName'] = 'Accessories'
        
        # Components category
        frames_mask = self.publish_product['ProductSubCategoryName'].str.contains('Frames', na=False)
        components_mask = self.publish_product['ProductSubCategoryName'].isin(components_items)
        self.publish_product.loc[mask_null_category & (frames_mask | components_mask), 'ProductCategoryName'] = 'Components'
        
        self.logger.info("Products transformation completed")
        
    def calculate_business_days(self, order_date, ship_date):
        """Calculate business days between two dates (excluding weekends)"""
        if pd.isna(order_date) or pd.isna(ship_date):
            return None
        
        try:
            # Handle incomplete dates by adding day 01 if needed
            if len(str(order_date)) == 7:  # YYYY-MM format
                order_date = str(order_date) + '-01'
            if len(str(ship_date)) == 7:  # YYYY-MM format
                ship_date = str(ship_date) + '-01'
                
            order_dt = pd.to_datetime(order_date)
            ship_dt = pd.to_datetime(ship_date)
            
            business_days = 0
            current_date = order_dt
            
            while current_date <= ship_dt:
                if current_date.weekday() < 5:  # Monday = 0, Friday = 4
                    business_days += 1
                current_date += timedelta(days=1)
            
            return business_days
        except:
            return None
        
    def transform_sales_orders(self):
        """Transform sales orders data according to requirements"""
        self.logger.info("Transforming sales orders data...")
        
        # Join SalesOrderDetail with SalesOrderHeader
        joined_orders = self.store_sales_order_detail.merge(
            self.store_sales_order_header,
            on='SalesOrderID',
            how='inner'
        )
        
        # Calculate LeadTimeInBusinessDays
        joined_orders['LeadTimeInBusinessDays'] = joined_orders.apply(
            lambda row: self.calculate_business_days(row['OrderDate'], row['ShipDate']), 
            axis=1
        )
        
        # Calculate TotalLineExtendedPrice
        joined_orders['TotalLineExtendedPrice'] = joined_orders['OrderQty'] * (joined_orders['UnitPrice'] - joined_orders['UnitPriceDiscount'])
        
        # Select and rename columns as required
        self.publish_orders = joined_orders[[
            'SalesOrderID', 'SalesOrderDetailID', 'OrderQty', 'ProductID', 
            'UnitPrice', 'UnitPriceDiscount', 'OrderDate', 'ShipDate', 
            'OnlineOrderFlag', 'AccountNumber', 'CustomerID', 'SalesPersonID', 
            'Freight', 'LeadTimeInBusinessDays', 'TotalLineExtendedPrice'
        ]].copy()
        
        # Rename Freight to TotalOrderFreight
        self.publish_orders = self.publish_orders.rename(columns={'Freight': 'TotalOrderFreight'})
        
        self.logger.info("Sales orders transformation completed")
        
    def save_transformed_data(self):
        """Save all transformed data"""
        self.logger.info("Saving transformed data...")
        
        # Create output directory
        os.makedirs("output", exist_ok=True)
        
        # Save store_ tables
        self.store_products.to_parquet("output/store_products.parquet", index=False)
        self.store_sales_order_header.to_parquet("output/store_sales_order_header.parquet", index=False)
        self.store_sales_order_detail.to_parquet("output/store_sales_order_detail.parquet", index=False)
        
        # Save publish_ tables
        self.publish_product.to_parquet("output/publish_product.parquet", index=False)
        self.publish_orders.to_parquet("output/publish_orders.parquet", index=False)
        
        self.logger.info("Transformed data saved successfully")
        
    def analyze_data(self):
        """Perform analysis questions"""
        self.logger.info("Performing data analysis...")
        
        # Join orders with products to get color information
        orders_with_products = self.publish_orders.merge(
            self.publish_product[['ProductID', 'Color', 'ProductCategoryName']],
            on='ProductID',
            how='inner'
        )
        
        # Convert OrderDate to datetime for year extraction
        # Handle incomplete dates (YYYY-MM format) by adding day 01
        orders_with_products['OrderDate'] = pd.to_datetime(orders_with_products['OrderDate'] + '-01', errors='coerce')
        orders_with_products['Year'] = orders_with_products['OrderDate'].dt.year
        
        # Question 1: Which color generated the highest revenue each year?
        revenue_by_color_year = orders_with_products.groupby(['Year', 'Color'])['TotalLineExtendedPrice'].sum().reset_index()
        revenue_by_color_year = revenue_by_color_year.sort_values(['Year', 'TotalLineExtendedPrice'], ascending=[True, False])
        
        # Get the highest revenue color for each year
        top_color_by_year = revenue_by_color_year.groupby('Year').first().reset_index()
        top_color_by_year = top_color_by_year[['Year', 'Color', 'TotalLineExtendedPrice']]
        top_color_by_year = top_color_by_year.rename(columns={'TotalLineExtendedPrice': 'TotalRevenue'})
        
        # Question 2: Average LeadTimeInBusinessDays by ProductCategoryName
        avg_lead_time_by_category = orders_with_products.groupby('ProductCategoryName')['LeadTimeInBusinessDays'].mean().reset_index()
        avg_lead_time_by_category = avg_lead_time_by_category.rename(columns={'LeadTimeInBusinessDays': 'AverageLeadTimeInBusinessDays'})
        avg_lead_time_by_category = avg_lead_time_by_category.sort_values('AverageLeadTimeInBusinessDays')
        
        # Save analysis results
        top_color_by_year.to_parquet("output/analysis_top_color_by_year.parquet", index=False)
        avg_lead_time_by_category.to_parquet("output/analysis_avg_lead_time_by_category.parquet", index=False)
        
        # Display results
        self.logger.info("Analysis Results:")
        self.logger.info("Top revenue color by year:")
        print(top_color_by_year.to_string(index=False))
        
        self.logger.info("\nAverage lead time by product category:")
        print(avg_lead_time_by_category.to_string(index=False))
        
        self.logger.info("Analysis completed")
        
    def run_pipeline(self):
        """Execute the complete data pipeline"""
        try:
            self.logger.info("Starting data pipeline execution...")
            
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

if __name__ == "__main__":
    pipeline = DataPipelinePandas()
    pipeline.run_pipeline() 