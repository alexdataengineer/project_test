# Data Files

This directory should contain the following CSV files for the data pipeline to work:

- `products (1).csv` - Product master data
- `sales_order_header (1).csv` - Sales order header information
- `sales_order_detail (1).csv` - Sales order detail information

## File Structure

The files should have the following structure:

### products (1).csv
```
ProductID,ProductDesc,ProductNumber,MakeFlag,Color,SafetyStockLevel,ReorderPoint,StandardCost,ListPrice,Size,SizeUnitMeasureCode,Weight,WeightUnitMeasureCode,ProductCategoryName,ProductSubCategoryName
```

### sales_order_header (1).csv
```
SalesOrderID,OrderDate,ShipDate,OnlineOrderFlag,AccountNumber,CustomerID,SalesPersonID,Freight
```

### sales_order_detail (1).csv
```
SalesOrderID,SalesOrderDetailID,OrderQty,ProductID,UnitPrice,UnitPriceDiscount
```

## Note

These files are not included in the repository due to their large size. Please ensure you have the correct data files in this directory before running the pipeline. 