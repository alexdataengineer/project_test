import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
import os
import sys
from pathlib import Path
from typing import Optional, Dict, Any, List
import json
from dataclasses import dataclass, field
import warnings

@dataclass
class PipelineConfig:
    """Configuration class for the data pipeline"""
    # Input paths
    products_file: str = "tables/products (1).csv"
    sales_order_header_file: str = "tables/sales_order_header (1).csv"
    sales_order_detail_file: str = "tables/sales_order_detail (1).csv"
    
    # Output paths
    output_dir: str = "output"
    
    # Processing options
    error_handling_mode: str = "strict"  # "strict", "lenient", "skip"
    date_format_fallback: str = "%Y-%m"
    max_memory_usage_gb: float = 4.0
    
    # Validation thresholds
    min_expected_rows: Dict[str, int] = field(default_factory=lambda: {
        "products": 100,
        "sales_order_header": 1000,
        "sales_order_detail": 5000
    })
    
    # Category mappings
    category_mappings: Dict[str, List[str]] = field(default_factory=lambda: {
        'Clothing': ['Gloves', 'Shorts', 'Socks', 'Tights', 'Vests'],
        'Accessories': ['Locks', 'Lights', 'Headsets', 'Helmets', 'Pedals', 'Pumps'],
        'Components': ['Wheels', 'Saddles']
    })

class DataValidationError(Exception):
    """Custom exception for data validation errors"""
    pass

class PipelineError(Exception):
    """Custom exception for pipeline errors"""
    pass

class RobustDataPipeline:
    def __init__(self, config: Optional[PipelineConfig] = None, log_level: str = "INFO"):
        self.config = config or PipelineConfig()
        self.setup_logging(log_level)
        self.validation_results = {}
        self.processing_stats = {}
        
    def setup_logging(self, log_level: str = "INFO"):
        """Setup comprehensive logging with file and console handlers"""
        # Create logs directory
        Path("logs").mkdir(exist_ok=True)
        
        # Configure logging
        log_format = '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
        
        # Clear any existing handlers
        logging.getLogger().handlers.clear()
        
        # Setup logger
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(getattr(logging, log_level.upper()))
        
        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(console_formatter)
        
        # File handler
        file_handler = logging.FileHandler(f"logs/pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
        file_handler.setLevel(logging.DEBUG)
        file_formatter = logging.Formatter(log_format)
        file_handler.setFormatter(file_formatter)
        
        self.logger.addHandler(console_handler)
        self.logger.addHandler(file_handler)
        
        self.logger.info("Logging setup completed")
        
    def validate_file_exists(self, filepath: str, file_description: str) -> None:
        """Validate that a file exists and is readable"""
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"{file_description} not found at {filepath}")
        
        if not os.access(filepath, os.R_OK):
            raise PermissionError(f"Cannot read {file_description} at {filepath}")
        
        file_size = os.path.getsize(filepath)
        if file_size == 0:
            raise DataValidationError(f"{file_description} is empty")
        
        self.logger.debug(f"File validation passed for {filepath} (size: {file_size:,} bytes)")
        
    def validate_dataframe(self, df: pd.DataFrame, name: str, required_columns: Optional[List[str]] = None) -> None:
        """Validate DataFrame structure and content"""
        if df is None or df.empty:
            raise DataValidationError(f"{name} DataFrame is empty")
        
        # Check minimum row count
        min_rows = self.config.min_expected_rows.get(name.lower().replace('raw_', ''), 0)
        if len(df) < min_rows:
            warning_msg = f"{name} has only {len(df)} rows, expected at least {min_rows}"
            if self.config.error_handling_mode == "strict":
                raise DataValidationError(warning_msg)
            else:
                self.logger.warning(warning_msg)
        
        # Check for required columns
        if required_columns:
            missing_cols = set(required_columns) - set(df.columns)
            if missing_cols:
                raise DataValidationError(f"{name} missing required columns: {missing_cols}")
        
        # Check for completely null columns
        null_columns = df.columns[df.isnull().all()].tolist()
        if null_columns:
            self.logger.warning(f"{name} has completely null columns: {null_columns}")
        
        # Memory usage check
        memory_usage_mb = df.memory_usage(deep=True).sum() / 1024 / 1024
        if memory_usage_mb > self.config.max_memory_usage_gb * 1024:
            self.logger.warning(f"{name} using {memory_usage_mb:.2f}MB memory, consider optimization")
        
        self.validation_results[name] = {
            'rows': len(df),
            'columns': len(df.columns),
            'memory_usage_mb': memory_usage_mb,
            'null_columns': null_columns
        }
        
        self.logger.info(f"{name} validation passed: {len(df)} rows, {len(df.columns)} columns")
        
    def safe_numeric_conversion(self, series: pd.Series, column_name: str) -> pd.Series:
        """Safely convert series to numeric with error handling"""
        try:
            converted = pd.to_numeric(series, errors='coerce')
            
            # Count conversion failures
            null_count_before = series.isnull().sum()
            null_count_after = converted.isnull().sum()
            failed_conversions = null_count_after - null_count_before
            
            if failed_conversions > 0:
                failure_rate = failed_conversions / len(series) * 100
                message = f"{column_name}: {failed_conversions} values ({failure_rate:.1f}%) failed numeric conversion"
                
                if failure_rate > 10 and self.config.error_handling_mode == "strict":
                    raise DataValidationError(f"High failure rate for {message}")
                else:
                    self.logger.warning(message)
            
            return converted
        except Exception as e:
            self.logger.error(f"Failed to convert {column_name} to numeric: {str(e)}")
            if self.config.error_handling_mode == "strict":
                raise
            return series
        
    def safe_boolean_conversion(self, series: pd.Series, column_name: str) -> pd.Series:
        """Safely convert series to boolean with error handling"""
        try:
            # Handle various boolean representations
            bool_map = {'True': True, 'False': False, 'true': True, 'false': False, 
                       '1': True, '0': False, 1: True, 0: False}
            
            converted = series.map(bool_map)
            
            # Check for unmapped values
            unmapped_mask = converted.isnull() & series.notnull()
            if unmapped_mask.any():
                unmapped_values = series[unmapped_mask].unique()
                self.logger.warning(f"{column_name}: Unmapped boolean values: {unmapped_values}")
                
            return converted
        except Exception as e:
            self.logger.error(f"Failed to convert {column_name} to boolean: {str(e)}")
            if self.config.error_handling_mode == "strict":
                raise
            return series
        
    def load_raw_data(self) -> None:
        """Load CSV files with comprehensive error handling"""
        self.logger.info("Starting raw data loading...")
        
        try:
            # Validate input files
            self.validate_file_exists(self.config.products_file, "Products file")
            self.validate_file_exists(self.config.sales_order_header_file, "Sales order header file")
            self.validate_file_exists(self.config.sales_order_detail_file, "Sales order detail file")
            
            # Load with error handling
            load_kwargs = {
                'encoding': 'utf-8',
                'na_values': ['', 'NULL', 'null', 'None', 'N/A'],
                'keep_default_na': True
            }
            
            self.logger.info("Loading products data...")
            self.raw_products = pd.read_csv(self.config.products_file, **load_kwargs)
            self.validate_dataframe(self.raw_products, "raw_products", 
                                  ['ProductID', 'ProductSubCategoryName'])
            
            self.logger.info("Loading sales order header data...")
            self.raw_sales_order_header = pd.read_csv(self.config.sales_order_header_file, **load_kwargs)
            self.validate_dataframe(self.raw_sales_order_header, "raw_sales_order_header", 
                                  ['SalesOrderID', 'OrderDate'])
            
            self.logger.info("Loading sales order detail data...")
            self.raw_sales_order_detail = pd.read_csv(self.config.sales_order_detail_file, **load_kwargs)
            self.validate_dataframe(self.raw_sales_order_detail, "raw_sales_order_detail", 
                                  ['SalesOrderID', 'ProductID'])
            
            self.logger.info("Raw data loaded successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to load raw data: {str(e)}")
            raise PipelineError(f"Data loading failed: {str(e)}")
        
    def assign_data_types(self) -> None:
        """Assign data types with robust error handling"""
        self.logger.info("Starting data type assignment...")
        
        try:
            # Products table
            self.logger.info("Processing products data types...")
            self.store_products = self.raw_products.copy()
            
            # Numeric conversions with error handling
            numeric_columns = ['ProductID', 'SafetyStockLevel', 'ReorderPoint', 
                             'StandardCost', 'ListPrice', 'Weight']
            
            for col in numeric_columns:
                if col in self.store_products.columns:
                    self.store_products[col] = self.safe_numeric_conversion(
                        self.store_products[col], f"products.{col}")
            
            # Boolean conversion
            if 'MakeFlag' in self.store_products.columns:
                self.store_products['MakeFlag'] = self.safe_boolean_conversion(
                    self.store_products['MakeFlag'], "products.MakeFlag")
            
            # Sales order header
            self.logger.info("Processing sales order header data types...")
            self.store_sales_order_header = self.raw_sales_order_header.copy()
            
            header_numeric_cols = ['SalesOrderID', 'CustomerID', 'Freight']
            for col in header_numeric_cols:
                if col in self.store_sales_order_header.columns:
                    self.store_sales_order_header[col] = self.safe_numeric_conversion(
                        self.store_sales_order_header[col], f"sales_header.{col}")
            
            if 'OnlineOrderFlag' in self.store_sales_order_header.columns:
                self.store_sales_order_header['OnlineOrderFlag'] = self.safe_boolean_conversion(
                    self.store_sales_order_header['OnlineOrderFlag'], "sales_header.OnlineOrderFlag")
            
            # Sales order detail
            self.logger.info("Processing sales order detail data types...")
            self.store_sales_order_detail = self.raw_sales_order_detail.copy()
            
            detail_numeric_cols = ['SalesOrderID', 'SalesOrderDetailID', 'OrderQty', 
                                 'ProductID', 'UnitPrice', 'UnitPriceDiscount']
            
            for col in detail_numeric_cols:
                if col in self.store_sales_order_detail.columns:
                    self.store_sales_order_detail[col] = self.safe_numeric_conversion(
                        self.store_sales_order_detail[col], f"sales_detail.{col}")
            
            self.logger.info("Data type assignment completed successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to assign data types: {str(e)}")
            raise PipelineError(f"Data type assignment failed: {str(e)}")
        
    def transform_products(self) -> None:
        """Transform products data with enhanced error handling"""
        self.logger.info("Starting products transformation...")
        
        try:
            self.publish_product = self.store_products.copy()
            
            # Handle Color field with multiple null representations
            if 'Color' in self.publish_product.columns:
                color_nulls_before = self.publish_product['Color'].isnull().sum()
                self.publish_product['Color'] = self.publish_product['Color'].fillna('N/A')
                self.publish_product.loc[self.publish_product['Color'].isin(['', ' ', 'NULL']), 'Color'] = 'N/A'
                
                color_nulls_after = (self.publish_product['Color'] == 'N/A').sum()
                self.logger.info(f"Replaced {color_nulls_after} null/empty color values with 'N/A'")
            
            # Enhanced category mapping with validation
            if all(col in self.publish_product.columns for col in ['ProductCategoryName', 'ProductSubCategoryName']):
                
                # Track category assignments
                assignments_made = 0
                
                mask_null_category = (
                    (self.publish_product['ProductCategoryName'].isna()) | 
                    (self.publish_product['ProductCategoryName'].isin(['', ' ', 'NULL']))
                )
                
                null_categories_count = mask_null_category.sum()
                self.logger.info(f"Found {null_categories_count} products with missing categories")
                
                # Apply category mappings
                for category, subcategories in self.config.category_mappings.items():
                    category_mask = self.publish_product['ProductSubCategoryName'].isin(subcategories)
                    assignment_mask = mask_null_category & category_mask
                    
                    assigned_count = assignment_mask.sum()
                    if assigned_count > 0:
                        self.publish_product.loc[assignment_mask, 'ProductCategoryName'] = category
                        assignments_made += assigned_count
                        self.logger.info(f"Assigned {assigned_count} products to '{category}' category")
                
                # Handle special case for Frames
                frames_mask = self.publish_product['ProductSubCategoryName'].str.contains('Frames', na=False)
                frames_assignment_mask = mask_null_category & frames_mask
                frames_assigned = frames_assignment_mask.sum()
                
                if frames_assigned > 0:
                    self.publish_product.loc[frames_assignment_mask, 'ProductCategoryName'] = 'Components'
                    assignments_made += frames_assigned
                    self.logger.info(f"Assigned {frames_assigned} frame products to 'Components' category")
                
                # Report remaining unassigned categories
                still_null = (
                    (self.publish_product['ProductCategoryName'].isna()) | 
                    (self.publish_product['ProductCategoryName'].isin(['', ' ', 'NULL']))
                ).sum()
                
                if still_null > 0:
                    self.logger.warning(f"Still have {still_null} products without assigned categories")
                
                self.logger.info(f"Category assignment completed: {assignments_made} total assignments made")
            
            self.logger.info("Products transformation completed successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to transform products: {str(e)}")
            raise PipelineError(f"Products transformation failed: {str(e)}")
        
    def calculate_business_days(self, order_date: Any, ship_date: Any) -> Optional[int]:
        """Calculate business days with robust date handling"""
        try:
            if pd.isna(order_date) or pd.isna(ship_date):
                return None
            
            # Convert to string for processing
            order_str = str(order_date).strip()
            ship_str = str(ship_date).strip()
            
            # Handle incomplete dates (YYYY-MM format)
            if len(order_str) == 7 and '-' in order_str:
                order_str += '-01'
            if len(ship_str) == 7 and '-' in ship_str:
                ship_str += '-01'
            
            # Parse dates with multiple format attempts
            order_dt = pd.to_datetime(order_str, errors='coerce')
            ship_dt = pd.to_datetime(ship_str, errors='coerce')
            
            if pd.isna(order_dt) or pd.isna(ship_dt):
                return None
            
            # Validate date logic
            if ship_dt < order_dt:
                self.logger.warning(f"Ship date {ship_dt} is before order date {order_dt}")
                return None
            
            # Calculate business days
            business_days = 0
            current_date = order_dt
            
            # Limit calculation to avoid infinite loops
            max_days = 365  # Maximum 1 year difference
            days_calculated = 0
            
            while current_date <= ship_dt and days_calculated < max_days:
                if current_date.weekday() < 5:  # Monday = 0, Friday = 4
                    business_days += 1
                current_date += timedelta(days=1)
                days_calculated += 1
            
            if days_calculated >= max_days:
                self.logger.warning(f"Business days calculation exceeded maximum limit for dates {order_date} to {ship_date}")
                return None
            
            return business_days
            
        except Exception as e:
            self.logger.debug(f"Business days calculation failed for {order_date} to {ship_date}: {str(e)}")
            return None
        
    def transform_sales_orders(self) -> None:
        """Transform sales orders with comprehensive validation"""
        self.logger.info("Starting sales orders transformation...")
        
        try:
            # Validate join keys exist
            detail_ids = set(self.store_sales_order_detail['SalesOrderID'].dropna())
            header_ids = set(self.store_sales_order_header['SalesOrderID'].dropna())
            
            # Check for orphaned records
            orphaned_details = detail_ids - header_ids
            orphaned_headers = header_ids - detail_ids
            
            if orphaned_details:
                self.logger.warning(f"Found {len(orphaned_details)} sales order details without headers")
            if orphaned_headers:
                self.logger.warning(f"Found {len(orphaned_headers)} sales order headers without details")
            
            # Perform join with validation
            self.logger.info("Joining sales order detail with header...")
            joined_orders = self.store_sales_order_detail.merge(
                self.store_sales_order_header,
                on='SalesOrderID',
                how='inner',
                validate='many_to_one'  # Validate join relationship
            )
            
            join_loss = len(self.store_sales_order_detail) - len(joined_orders)
            if join_loss > 0:
                self.logger.warning(f"Lost {join_loss} records in join operation")
            
            self.logger.info(f"Join completed: {len(joined_orders)} records")
            
            # Calculate LeadTimeInBusinessDays with progress tracking
            self.logger.info("Calculating lead times...")
            
            if len(joined_orders) > 10000:
                # Show progress for large datasets
                chunk_size = len(joined_orders) // 10
                lead_times = []
                
                for i in range(0, len(joined_orders), chunk_size):
                    chunk = joined_orders.iloc[i:i+chunk_size]
                    chunk_lead_times = chunk.apply(
                        lambda row: self.calculate_business_days(row.get('OrderDate'), row.get('ShipDate')), 
                        axis=1
                    )
                    lead_times.extend(chunk_lead_times)
                    
                    progress = min(100, (i + chunk_size) / len(joined_orders) * 100)
                    self.logger.info(f"Lead time calculation progress: {progress:.1f}%")
                
                joined_orders['LeadTimeInBusinessDays'] = lead_times
            else:
                joined_orders['LeadTimeInBusinessDays'] = joined_orders.apply(
                    lambda row: self.calculate_business_days(row.get('OrderDate'), row.get('ShipDate')), 
                    axis=1
                )
            
            # Validate lead time calculations
            valid_lead_times = joined_orders['LeadTimeInBusinessDays'].notna().sum()
            total_records = len(joined_orders)
            success_rate = valid_lead_times / total_records * 100
            
            self.logger.info(f"Lead time calculation: {valid_lead_times}/{total_records} successful ({success_rate:.1f}%)")
            
            # Calculate TotalLineExtendedPrice with validation
            self.logger.info("Calculating extended prices...")
            
            # Validate required columns for calculation
            required_price_cols = ['OrderQty', 'UnitPrice', 'UnitPriceDiscount']
            missing_price_cols = [col for col in required_price_cols if col not in joined_orders.columns]
            
            if missing_price_cols:
                raise DataValidationError(f"Missing required columns for price calculation: {missing_price_cols}")
            
            # Handle potential calculation errors
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", RuntimeWarning)
                
                joined_orders['TotalLineExtendedPrice'] = (
                    joined_orders['OrderQty'].fillna(0) * 
                    (joined_orders['UnitPrice'].fillna(0) - joined_orders['UnitPriceDiscount'].fillna(0))
                )
            
            # Validate calculated prices
            negative_prices = (joined_orders['TotalLineExtendedPrice'] < 0).sum()
            if negative_prices > 0:
                self.logger.warning(f"Found {negative_prices} records with negative extended prices")
            
            zero_prices = (joined_orders['TotalLineExtendedPrice'] == 0).sum()
            if zero_prices > 0:
                self.logger.warning(f"Found {zero_prices} records with zero extended prices")
            
            # Select required columns with existence validation
            required_columns = [
                'SalesOrderID', 'SalesOrderDetailID', 'OrderQty', 'ProductID', 
                'UnitPrice', 'UnitPriceDiscount', 'OrderDate', 'ShipDate', 
                'OnlineOrderFlag', 'AccountNumber', 'CustomerID', 'SalesPersonID', 
                'Freight', 'LeadTimeInBusinessDays', 'TotalLineExtendedPrice'
            ]
            
            available_columns = [col for col in required_columns if col in joined_orders.columns]
            missing_columns = set(required_columns) - set(available_columns)
            
            if missing_columns:
                self.logger.warning(f"Missing columns in final output: {missing_columns}")
            
            self.publish_orders = joined_orders[available_columns].copy()
            
            # Rename Freight to TotalOrderFreight if it exists
            if 'Freight' in self.publish_orders.columns:
                self.publish_orders = self.publish_orders.rename(columns={'Freight': 'TotalOrderFreight'})
            
            # Final validation
            self.validate_dataframe(self.publish_orders, "publish_orders")
            
            self.logger.info(f"Sales orders transformation completed: {len(self.publish_orders)} records")
            
        except Exception as e:
            self.logger.error(f"Failed to transform sales orders: {str(e)}")
            raise PipelineError(f"Sales orders transformation failed: {str(e)}")
        
    def save_transformed_data(self) -> None:
        """Save transformed data with comprehensive error handling"""
        self.logger.info("Starting data save operation...")
        
        try:
            # Create output directory with proper permissions
            output_path = Path(self.config.output_dir)
            output_path.mkdir(parents=True, exist_ok=True)
            
            # Check write permissions
            if not os.access(output_path, os.W_OK):
                raise PermissionError(f"No write permission for output directory: {output_path}")
            
            # Save with compression and error handling
            save_kwargs = {
                'index': False,
                'compression': 'snappy'  # Better compression and speed
            }
            
            tables_to_save = [
                (self.store_products, "store_products.parquet"),
                (self.store_sales_order_header, "store_sales_order_header.parquet"),
                (self.store_sales_order_detail, "store_sales_order_detail.parquet"),
                (self.publish_product, "publish_product.parquet"),
                (self.publish_orders, "publish_orders.parquet")
            ]
            
            for df, filename in tables_to_save:
                if df is not None and not df.empty:
                    filepath = output_path / filename
                    
                    try:
                        df.to_parquet(filepath, **save_kwargs)
                        file_size = filepath.stat().st_size
                        self.logger.info(f"Saved {filename}: {len(df):,} rows, {file_size:,} bytes")
                        
                        # Store stats
                        self.processing_stats[filename] = {
                            'rows': len(df),
                            'columns': len(df.columns),
                            'file_size_bytes': file_size
                        }
                        
                    except Exception as e:
                        self.logger.error(f"Failed to save {filename}: {str(e)}")
                        if self.config.error_handling_mode == "strict":
                            raise
                else:
                    self.logger.warning(f"Skipping save for {filename}: DataFrame is None or empty")
            
            # Save processing metadata
            metadata = {
                'pipeline_run_time': datetime.now().isoformat(),
                'config': self.config.__dict__,
                'validation_results': self.validation_results,
                'processing_stats': self.processing_stats
            }
            
            metadata_path = output_path / "pipeline_metadata.json"
            with open(metadata_path, 'w') as f:
                json.dump(metadata, f, indent=2, default=str)
            
            self.logger.info(f"All data saved successfully to {output_path}")
            
        except Exception as e:
            self.logger.error(f"Failed to save transformed data: {str(e)}")
            raise PipelineError(f"Data save operation failed: {str(e)}")
        
    def analyze_data(self) -> None:
        """Perform analysis with enhanced error handling"""
        self.logger.info("Starting data analysis...")
        
        try:
            # Validate input data for analysis
            if not hasattr(self, 'publish_orders') or self.publish_orders.empty:
                raise DataValidationError("No orders data available for analysis")
            
            if not hasattr(self, 'publish_product') or self.publish_product.empty:
                raise DataValidationError("No product data available for analysis")
            
            # Join orders with products
            self.logger.info("Joining orders with products for analysis...")
            
            # Validate join keys
            order_products = set(self.publish_orders['ProductID'].dropna())
            available_products = set(self.publish_product['ProductID'].dropna())
            missing_products = order_products - available_products
            
            if missing_products:
                self.logger.warning(f"Found {len(missing_products)} product IDs in orders but not in products")
            
            orders_with_products = self.publish_orders.merge(
                self.publish_product[['ProductID', 'Color', 'ProductCategoryName']].dropna(subset=['ProductID']),
                on='ProductID',
                how='left'
            )
            
            # Handle date parsing with robust error handling
            self.logger.info("Processing order dates...")
            
            def safe_date_conversion(date_str):
                try:
                    if pd.isna(date_str):
                        return pd.NaT
                    
                    date_str = str(date_str).strip()
                    
                    # Handle YYYY-MM format
                    if len(date_str) == 7 and '-' in date_str:
                        date_str += '-01'
                    
                    return pd.to_datetime(date_str, errors='coerce')
                except:
                    return pd.NaT
            
            orders_with_products['OrderDate_parsed'] = orders_with_products['OrderDate'].apply(safe_date_conversion)
            orders_with_products['Year'] = orders_with_products['OrderDate_parsed'].dt.year
            
            # Validate date parsing success
            valid_dates = orders_with_products['OrderDate_parsed'].notna().sum()
            total_orders = len(orders_with_products)
            date_success_rate = valid_dates / total_orders * 100
            
            self.logger.info(f"Date parsing: {valid_dates}/{total_orders} successful ({date_success_rate:.1f}%)")
            
            if date_success_rate < 50:
                self.logger.warning("Low date parsing success rate - analysis results may be incomplete")
            
            # Analysis 1: Revenue by color and year
            self.logger.info("Analyzing revenue by color and year...")
            
            analysis_data = orders_with_products.dropna(subset=['Year', 'Color', 'TotalLineExtendedPrice'])
            
            if analysis_data.empty:
                self.logger.warning("No valid data for color revenue analysis")
                top_color_by_year = pd.DataFrame(columns=['Year', 'Color', 'TotalRevenue'])
            else:
                revenue_by_color_year = analysis_data.groupby(['Year', 'Color'])['TotalLineExtendedPrice'].sum().reset_index()
                revenue_by_color_year = revenue_by_color_year.sort_values(['Year', 'TotalLineExtendedPrice'], ascending=[True, False])
                
                # Get the highest revenue color for each year
                top_color_by_year = revenue_by_color_year.groupby('Year').first().reset_index()
                top_color_by_year = top_color_by_year[['Year', 'Color', 'TotalLineExtendedPrice']]
                top_color_by_year = top_color_by_year.rename(columns={'TotalLineExtendedPrice': 'TotalRevenue'})
                
                self.logger.info(f"Color revenue analysis completed for {len(top_color_by_year)} years")
            
            # Analysis 2: Average lead time by product category
            self.logger.info("Analyzing lead time by product category...")
            
            lead_time_data = orders_with_products.dropna(subset=['ProductCategoryName', 'LeadTimeInBusinessDays'])
            
            if lead_time_data.empty:
                self.logger.warning("No valid data for lead time analysis")
                avg_lead_time_by_category = pd.DataFrame(columns=['ProductCategoryName', 'AverageLeadTimeInBusinessDays'])
            else:
                avg_lead_time_by_category = lead_time_data.groupby('ProductCategoryName')['LeadTimeInBusinessDays'].agg([
                    'mean', 'count', 'std', 'min', 'max'
                ]).reset_index()
                
                avg_lead_time_by_category = avg_lead_time_by_category.rename(columns={
                    'mean': 'AverageLeadTimeInBusinessDays',
                    'count': 'SampleSize',
                    'std': 'StandardDeviation',
                    'min': 'MinLeadTime',
                    'max': 'MaxLeadTime'
                })
                
                # Round to reasonable precision
                numeric_cols = ['AverageLeadTimeInBusinessDays', 'StandardDeviation']
                for col in numeric_cols:
                    if col in avg_lead_time_by_category.columns:
                        avg_lead_time_by_category[col] = avg_lead_time_by_category[col].round(2)
                
                avg_lead_time_by_category = avg_lead_time_by_category.sort_values('AverageLeadTimeInBusinessDays')
                
                self.logger.info(f"Lead time analysis completed for {len(avg_lead_time_by_category)} categories")
            
            # Save analysis results with error handling
            try:
                output_path = Path(self.config.output_dir)
                
                if not top_color_by_year.empty:
                    color_analysis_path = output_path / "analysis_top_color_by_year.parquet"
                    top_color_by_year.to_parquet(color_analysis_path, index=False)
                    self.logger.info(f"Saved color revenue analysis: {len(top_color_by_year)} records")
                
                if not avg_lead_time_by_category.empty:
                    lead_time_analysis_path = output_path / "analysis_avg_lead_time_by_category.parquet"
                    avg_lead_time_by_category.to_parquet(lead_time_analysis_path, index=False)
                    self.logger.info(f"Saved lead time analysis: {len(avg_lead_time_by_category)} records")
                
                # Save summary analysis report
                analysis_summary = {
                    'analysis_timestamp': datetime.now().isoformat(),
                    'total_orders_analyzed': len(orders_with_products),
                    'valid_orders_for_color_analysis': len(analysis_data) if not analysis_data.empty else 0,
                    'valid_orders_for_leadtime_analysis': len(lead_time_data) if not lead_time_data.empty else 0,
                    'years_analyzed': top_color_by_year['Year'].tolist() if not top_color_by_year.empty else [],
                    'categories_analyzed': avg_lead_time_by_category['ProductCategoryName'].tolist() if not avg_lead_time_by_category.empty else []
                }
                
                summary_path = output_path / "analysis_summary.json"
                with open(summary_path, 'w') as f:
                    json.dump(analysis_summary, f, indent=2, default=str)
                
            except Exception as e:
                self.logger.error(f"Failed to save analysis results: {str(e)}")
                if self.config.error_handling_mode == "strict":
                    raise
            
            # Display results with formatting
            self.logger.info("=== ANALYSIS RESULTS ===")
            
            if not top_color_by_year.empty:
                self.logger.info("Top revenue color by year:")
                print("\n" + top_color_by_year.to_string(index=False, float_format='${:,.2f}'.format))
            else:
                self.logger.warning("No color revenue analysis results to display")
            
            if not avg_lead_time_by_category.empty:
                self.logger.info("\nAverage lead time by product category:")
                print("\n" + avg_lead_time_by_category.to_string(index=False, float_format='{:.2f}'.format))
            else:
                self.logger.warning("No lead time analysis results to display")
            
            self.logger.info("Analysis completed successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to perform data analysis: {str(e)}")
            raise PipelineError(f"Data analysis failed: {str(e)}")
    
    def generate_pipeline_report(self) -> Dict[str, Any]:
        """Generate comprehensive pipeline execution report"""
        try:
            report = {
                'pipeline_metadata': {
                    'execution_timestamp': datetime.now().isoformat(),
                    'pipeline_version': '2.0.0',
                    'configuration': self.config.__dict__
                },
                'data_validation': self.validation_results,
                'processing_statistics': self.processing_stats,
                'execution_summary': {
                    'total_files_processed': len(self.processing_stats),
                    'total_rows_processed': sum(stats.get('rows', 0) for stats in self.processing_stats.values()),
                    'total_storage_size_mb': sum(stats.get('file_size_bytes', 0) for stats in self.processing_stats.values()) / 1024 / 1024
                }
            }
            
            # Save comprehensive report
            report_path = Path(self.config.output_dir) / "pipeline_execution_report.json"
            with open(report_path, 'w') as f:
                json.dump(report, f, indent=2, default=str)
            
            self.logger.info(f"Pipeline execution report saved to {report_path}")
            return report
            
        except Exception as e:
            self.logger.error(f"Failed to generate pipeline report: {str(e)}")
            return {}
    
    def run_pipeline(self, generate_report: bool = True) -> bool:
        """Execute the complete data pipeline with comprehensive error handling"""
        pipeline_start_time = datetime.now()
        
        try:
            self.logger.info("="*60)
            self.logger.info("STARTING ROBUST DATA PIPELINE EXECUTION")
            self.logger.info("="*60)
            self.logger.info(f"Start time: {pipeline_start_time}")
            self.logger.info(f"Configuration: {self.config.error_handling_mode} mode")
            
            # Pipeline execution steps
            steps = [
                ("Loading raw data", self.load_raw_data),
                ("Assigning data types", self.assign_data_types),
                ("Transforming products", self.transform_products),
                ("Transforming sales orders", self.transform_sales_orders),
                ("Saving transformed data", self.save_transformed_data),
                ("Performing data analysis", self.analyze_data)
            ]
            
            completed_steps = 0
            
            for step_name, step_function in steps:
                try:
                    self.logger.info(f"Starting: {step_name}...")
                    step_start = datetime.now()
                    
                    step_function()
                    
                    step_duration = datetime.now() - step_start
                    completed_steps += 1
                    
                    self.logger.info(f"Completed: {step_name} (Duration: {step_duration})")
                    
                except Exception as e:
                    self.logger.error(f"Failed at step '{step_name}': {str(e)}")
                    
                    if self.config.error_handling_mode == "strict":
                        raise PipelineError(f"Pipeline failed at step '{step_name}': {str(e)}")
                    elif self.config.error_handling_mode == "skip":
                        self.logger.warning(f"Skipping failed step '{step_name}' and continuing...")
                        continue
                    else:  # lenient mode
                        self.logger.warning(f"Error in step '{step_name}', attempting to continue...")
                        continue
            
            # Calculate total execution time
            total_duration = datetime.now() - pipeline_start_time
            
            # Generate final report
            if generate_report:
                self.generate_pipeline_report()
            
            # Final status
            success_rate = completed_steps / len(steps) * 100
            
            self.logger.info("="*60)
            self.logger.info("PIPELINE EXECUTION COMPLETED")
            self.logger.info("="*60)
            self.logger.info(f"End time: {datetime.now()}")
            self.logger.info(f"Total duration: {total_duration}")
            self.logger.info(f"Steps completed: {completed_steps}/{len(steps)} ({success_rate:.1f}%)")
            
            if completed_steps == len(steps):
                self.logger.info("✅ All pipeline steps completed successfully!")
                return True
            else:
                self.logger.warning(f"⚠️  Pipeline completed with {len(steps) - completed_steps} failed steps")
                return False
            
        except Exception as e:
            total_duration = datetime.now() - pipeline_start_time
            self.logger.error("="*60)
            self.logger.error("PIPELINE EXECUTION FAILED")
            self.logger.error("="*60)
            self.logger.error(f"Error: {str(e)}")
            self.logger.error(f"Duration before failure: {total_duration}")
            self.logger.error(f"Steps completed: {completed_steps}/{len(steps)}")
            
            # Still try to generate a report for debugging
            if generate_report:
                try:
                    self.generate_pipeline_report()
                except:
                    pass
            
            return False

def main():
    """Main execution function with command line argument support"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Robust Data Pipeline')
    parser.add_argument('--config', type=str, help='Path to configuration JSON file')
    parser.add_argument('--mode', choices=['strict', 'lenient', 'skip'], default='lenient',
                        help='Error handling mode')
    parser.add_argument('--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], default='INFO',
                        help='Logging level')
    parser.add_argument('--output-dir', type=str, default='output',
                        help='Output directory for processed files')
    
    args = parser.parse_args()
    
    try:
        # Load configuration
        if args.config and os.path.exists(args.config):
            with open(args.config, 'r') as f:
                config_dict = json.load(f)
            config = PipelineConfig(**config_dict)
        else:
            config = PipelineConfig()
        
        # Override with command line arguments
        config.error_handling_mode = args.mode
        config.output_dir = args.output_dir
        
        # Create and run pipeline
        pipeline = RobustDataPipeline(config=config, log_level=args.log_level)
        success = pipeline.run_pipeline()
        
        # Exit with appropriate code
        sys.exit(0 if success else 1)
        
    except Exception as e:
        print(f"Fatal error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()