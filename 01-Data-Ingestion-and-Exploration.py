# Databricks notebook source
# MAGIC %md
# MAGIC # 📥 Data Ingestion and Exploration
# MAGIC
# MAGIC **Level**: Beginner
# MAGIC **Duration**: 30 minutes
# MAGIC **Prerequisites**: Complete notebook 00-Getting-Started
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Learning Objectives
# MAGIC
# MAGIC By the end of this notebook, you will:
# MAGIC - ✅ Load JSON files into Spark DataFrames
# MAGIC - ✅ Understand schema inference vs. explicit schemas
# MAGIC - ✅ Perform data quality checks (nulls, duplicates, data types)
# MAGIC - ✅ Apply basic transformations (filter, select, aggregate)
# MAGIC - ✅ Create visualizations with the `display()` function
# MAGIC - ✅ Follow best practices for data ingestion
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📖 Documentation Links
# MAGIC
# MAGIC | Resource | Link |
# MAGIC |----------|------|
# MAGIC | 📘 Reading Data | [Databricks Data Sources](https://docs.databricks.com/en/data/data-sources/) |
# MAGIC | 🐍 PySpark DataFrame API | [DataFrame Guide](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html) |
# MAGIC | 📊 Data Exploration | [Exploratory Data Analysis](https://docs.databricks.com/en/exploratory-data-analysis/) |
# MAGIC | 🔍 Data Quality | [Data Quality Best Practices](https://docs.databricks.com/en/lakehouse/data-quality.html) |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔧 Setup and Configuration
# MAGIC
# MAGIC Let's configure our environment for optimal demo performance:

# COMMAND ----------

# DBTITLE 1,Configure Spark for Demo
# Reduce shuffle partitions for faster demos
spark.conf.set("spark.sql.shuffle.partitions", "1")

# Show more rows in display() output
spark.conf.set("spark.sql.repl.eagerEval.maxNumRows", "20")

print("✅ Spark configured for demo environment")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Why JSON?
# MAGIC
# MAGIC **JSON** is a flexible, human-readable format for semi-structured data:
# MAGIC
# MAGIC | Feature | Benefit |
# MAGIC |---------|---------|
# MAGIC | 📝 **Human Readable** | Easy to inspect and understand data structure |
# MAGIC | 🔄 **Flexible Schema** | Handles nested objects and arrays naturally |
# MAGIC | 🌐 **Universal Format** | Widely supported across all platforms |
# MAGIC | 📊 **Self-Describing** | Field names included in every record |
# MAGIC | 🔢 **Type Inference** | Spark automatically detects data types |
# MAGIC
# MAGIC ### JSON vs. CSV
# MAGIC
# MAGIC ```
# MAGIC CSV:  Flat structure only, requires header row, limited nesting
# MAGIC       ❌ Not ideal for complex data
# MAGIC
# MAGIC JSON: Supports nested objects, arrays, flexible schema
# MAGIC       ✅ Perfect for real-world business data
# MAGIC ```
# MAGIC
# MAGIC 📖 **Learn More**: [JSON Documentation](https://www.json.org/)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📂 Loading Data from JSON
# MAGIC
# MAGIC ### Method 1: Schema Inference (Recommended for Exploration)

# COMMAND ----------

# DBTITLE 1,Load JSON with Schema Inference
# Define the dataset path
dataset_path = "/databricks-datasets/retail-org/sales_orders/"

# Load the JSON files (schema is automatically inferred)
df = spark.read.format("json").load(dataset_path)

# Alternative shorthand syntax:
# df = spark.read.json(dataset_path)

print(f"✅ Loaded {df.count():,} records")
print(f"✅ Schema inferred from JSON data")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Method 2: Explicit Schema (Recommended for Production)
# MAGIC
# MAGIC 💡 **Why explicit schemas?**
# MAGIC - ✅ Faster (no schema inference overhead)
# MAGIC - ✅ Safer (reject unexpected data)
# MAGIC - ✅ Documented (schema is code)

# COMMAND ----------

# DBTITLE 1,Define Explicit Schema
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    DoubleType,
    StringType,
    ArrayType,
    TimestampType,
)

# Define the schema explicitly for retail orders with nested products
orders_schema = StructType(
    [
        StructField("order_number", IntegerType(), True),
        StructField("order_datetime", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("customer_name", StringType(), True),
        StructField(
            "ordered_products",
            ArrayType(
                StructType(
                    [
                        StructField("name", StringType(), True),
                        StructField("price", DoubleType(), True),
                        StructField("qty", IntegerType(), True),
                    ]
                )
            ),
            True,
        ),
    ]
)

# Load with explicit schema
df_explicit = spark.read.format("json").schema(orders_schema).load(dataset_path)

print("✅ Loaded with explicit schema")
print(f"   Records: {df_explicit.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ⚠️ **Best Practice**: Use schema inference for exploration, explicit schemas for production pipelines.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔍 Schema Exploration
# MAGIC
# MAGIC Understanding your data structure is the first step in any data engineering workflow.

# COMMAND ----------

# DBTITLE 1,Display Schema Tree
# Print the schema in a readable tree format
df.printSchema()

# COMMAND ----------

# DBTITLE 1,Get Column Names and Types
# Extract column names and data types
columns_info = [
    (field.name, field.dataType.simpleString()) for field in df.schema.fields
]

# Display as a DataFrame for better visualization
from pyspark.sql import Row

schema_df = spark.createDataFrame(
    [Row(column=name, data_type=dtype) for name, dtype in columns_info]
)
display(schema_df)

# COMMAND ----------

# DBTITLE 1,Count Columns by Data Type
from pyspark.sql.functions import lit, count

# Group columns by data type
type_counts = {}
for field in df.schema.fields:
    dtype = field.dataType.simpleString()
    type_counts[dtype] = type_counts.get(dtype, 0) + 1

print("📊 Column Distribution by Type:")
for dtype, count in sorted(type_counts.items()):
    print(f"   {dtype}: {count} columns")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Data Exploration
# MAGIC
# MAGIC ### Preview Sample Records

# COMMAND ----------

# DBTITLE 1,Display First 10 Records
# Use display() for interactive table with sorting and filtering
display(df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC 💡 **Pro Tip**: Click column headers to sort, use the filter icon to search, and click the chart icons to visualize!

# COMMAND ----------

# DBTITLE 1,Show Random Sample
# Get a random sample (useful for large datasets)
sample_df = df.sample(fraction=0.01, seed=42)
display(sample_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Basic Statistics

# COMMAND ----------

# DBTITLE 1,Summary Statistics for All Columns
# Get count, mean, stddev, min, max for numeric columns
display(df.summary())

# COMMAND ----------

# DBTITLE 1,Explore Nested Structure
# First, let's understand the nested ordered_products array
print("Schema of the data:")
df.printSchema()

print("\nSample order with products:")
display(df.select("order_number", "customer_name", "ordered_products").limit(3))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Record Counts

# COMMAND ----------

# DBTITLE 1,Total Record Count
total_records = df.count()
print(f"📊 Total Records: {total_records:,}")

# COMMAND ----------

# DBTITLE 1,Count by Categorical Columns
from pyspark.sql.functions import count, col

# Count orders by state
state_counts = (
    df.groupBy("state")
    .agg(count("*").alias("order_count"))
    .orderBy(col("order_count").desc())
)

display(state_counts)

# COMMAND ----------

# MAGIC %md
# MAGIC 💡 **Visualization**: Click the bar chart icon above to see which states have the most orders!

# COMMAND ----------

# DBTITLE 1,Count by Product
product_counts = (
    df.groupBy("product_id").agg(count("*").alias("count")).orderBy(col("count").desc())
)

display(product_counts)

# COMMAND ----------

# DBTITLE 1,Count by City
city_counts = (
    df.groupBy("city").agg(count("*").alias("count")).orderBy(col("count").desc())
)

display(city_counts)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧹 Data Quality Checks
# MAGIC
# MAGIC ### Why Data Quality Matters
# MAGIC
# MAGIC ⚠️ **Bad data leads to**:
# MAGIC - Incorrect analytics and reports
# MAGIC - Failed ML model training
# MAGIC - Production pipeline failures
# MAGIC - Lost business value
# MAGIC
# MAGIC ✅ **Good data quality ensures**:
# MAGIC - Trustworthy insights
# MAGIC - Reliable predictions
# MAGIC - Smooth operations

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check for Null Values

# COMMAND ----------

# DBTITLE 1,Count Nulls in Each Column
from pyspark.sql.functions import col, sum as spark_sum, count, when

# Count nulls for each column
null_counts = df.select(
    [spark_sum(when(col(c).isNull(), 1).otherwise(0)).alias(c) for c in df.columns]
)

# Transpose for better readability
null_df = null_counts.first().asDict()
null_summary = spark.createDataFrame(
    [
        Row(column=k, null_count=v, null_percentage=round(v / total_records * 100, 2))
        for k, v in null_df.items()
    ]
).orderBy(col("null_count").desc())

display(null_summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check for Duplicates

# COMMAND ----------

# DBTITLE 1,Check for Duplicate Records
# Count total records
total = df.count()

# Count distinct records (all columns)
distinct = df.distinct().count()

# Calculate duplicates
duplicates = total - distinct

print(f"📊 Total Records: {total:,}")
print(f"📊 Distinct Records: {distinct:,}")
print(f"📊 Duplicate Records: {duplicates:,}")

if duplicates > 0:
    print(f"⚠️  Warning: {duplicates} duplicate records found!")
else:
    print("✅ No duplicates found")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Type Validation

# COMMAND ----------

# DBTITLE 1,Check for Invalid Numeric Values
from pyspark.sql.functions import isnan, isnull, size

# Check for null values in top-level columns
top_level_columns = ["order_number", "customer_id", "ordered_products"]

for col_name in top_level_columns:
    null_count = df.filter(isnull(col(col_name))).count()

    if null_count > 0:
        print(f"⚠️  {col_name}: {null_count} NULL values")
    else:
        print(f"✅ {col_name}: No NULL values")

# Check for empty product arrays
empty_arrays = df.filter(size(col("ordered_products")) == 0).count()
if empty_arrays > 0:
    print(f"⚠️  {empty_arrays} orders with no products")
else:
    print(f"✅ All orders have at least one product")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Value Range Validation

# COMMAND ----------

# DBTITLE 1,Check for Invalid Product Prices
from pyspark.sql.functions import explode

# Explode products to check individual prices
products_df = df.select("order_number", explode("ordered_products").alias("product"))

# Check for negative or zero prices
invalid_prices = products_df.filter(col("product.price") <= 0).count()
invalid_qty = products_df.filter(col("product.qty") <= 0).count()

if invalid_prices > 0:
    print(f"⚠️  Warning: {invalid_prices} products with invalid prices!")
else:
    print("✅ All product prices are positive")

if invalid_qty > 0:
    print(f"⚠️  Warning: {invalid_qty} products with invalid quantities!")
else:
    print("✅ All product quantities are positive")

# COMMAND ----------

# DBTITLE 1,Check for Unrealistic Values
# Product prices should be reasonable (e.g., $1 to $10,000)
unrealistic_prices = products_df.filter(
    (col("product.price") < 1) | (col("product.price") > 10000)
).count()

if unrealistic_prices > 0:
    print(f"⚠️  Warning: {unrealistic_prices} products with unrealistic prices!")
    display(
        products_df.filter(
            (col("product.price") < 1) | (col("product.price") > 10000)
        ).limit(5)
    )
else:
    print("✅ All product prices are realistic")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔄 Basic Transformations
# MAGIC
# MAGIC ### Selecting Columns

# COMMAND ----------

# DBTITLE 1,Working with Nested Data - Explode Products
# Explode the ordered_products array to get one row per product
from pyspark.sql.functions import explode

products_expanded = df.select(
    "order_number",
    "order_datetime",
    "customer_id",
    "customer_name",
    explode("ordered_products").alias("product"),
)

products_with_total = products_expanded.select(
    "order_number",
    "customer_name",
    col("product.name").alias("product_name"),
    col("product.price").alias("price"),
    col("product.qty").alias("quantity"),
    (col("product.price") * col("product.qty")).alias("line_total"),
)

display(products_with_total.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filtering Data

# COMMAND ----------

# DBTITLE 1,Filter Orders by Customer
# Get orders from a specific customer
customer_orders = df.filter(col("customer_id") == "C00001")

print(f"📊 Orders for customer C00001: {customer_orders.count():,}")
display(
    customer_orders.select("order_number", "customer_name", "ordered_products").limit(
        10
    )
)

# COMMAND ----------

# DBTITLE 1,Filter Orders with Specific Products
# Find orders containing products with price > $50
from pyspark.sql.functions import exists

expensive_product_orders = df.filter(
    exists(col("ordered_products"), lambda p: p.price > 50)
)

print(f"📊 Orders with products priced > $50: {expensive_product_orders.count():,}")
display(
    expensive_product_orders.select(
        "order_number", "customer_name", "ordered_products"
    ).limit(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Adding Calculated Columns

# COMMAND ----------

# DBTITLE 1,Calculate Order Totals from Products
from pyspark.sql.functions import round as spark_round, sum as spark_sum

# Calculate total order value by summing all product line totals
order_totals = products_with_total.groupBy("order_number", "customer_name").agg(
    spark_sum("line_total").alias("order_total"), count("*").alias("num_products")
)

display(order_totals.limit(10))

display(
    df_with_margin.select("total", "discount", "tax", "profit_margin_pct").limit(10)
)

# COMMAND ----------

# DBTITLE 1,Categorize Orders by Value
from pyspark.sql.functions import when

order_totals_with_category = order_totals.withColumn(
    "order_category",
    when(col("order_total") < 100, "Small")
    .when((col("order_total") >= 100) & (col("order_total") < 500), "Medium")
    .otherwise("Large"),
)

category_counts = (
    order_totals_with_category.groupBy("order_category")
    .agg(count("*").alias("count"))
    .orderBy("order_category")
)

display(category_counts)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Aggregations

# COMMAND ----------

# DBTITLE 1,Aggregate by Product
from pyspark.sql.functions import (
    avg,
    sum as spark_sum,
    min as spark_min,
    max as spark_max,
)

product_stats = (
    products_with_total.groupBy("product_name")
    .agg(
        count("*").alias("times_ordered"),
        spark_round(avg("price"), 2).alias("avg_price"),
        spark_round(spark_sum("line_total"), 2).alias("total_revenue"),
        spark_round(avg("quantity"), 2).alias("avg_qty"),
    )
    .orderBy(col("total_revenue").desc())
)

display(product_stats.limit(10))

# COMMAND ----------

# DBTITLE 1,Aggregate by Customer
customer_stats = (
    df.groupBy("customer_id", "customer_name")
    .agg(
        count("*").alias("num_orders"),
        spark_round(avg(size(col("ordered_products"))), 2).alias(
            "avg_products_per_order"
        ),
    )
    .orderBy(col("num_orders").desc())
)

display(customer_stats.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Data Visualizations
# MAGIC
# MAGIC ### Distribution Analysis

# COMMAND ----------

# DBTITLE 1,Order Value Distribution
from pyspark.sql.functions import floor

order_distribution = (
    order_totals.withColumn(
        "value_bin", (floor(col("order_total") / 100) * 100).cast("int")
    )
    .groupBy("value_bin")
    .agg(count("*").alias("count"))
    .orderBy("value_bin")
)

display(order_distribution)

# COMMAND ----------

# MAGIC %md
# MAGIC 💡 **Try this**: Click the histogram icon to see the distribution visually!

# COMMAND ----------

# DBTITLE 1,Product Price vs Quantity Ordered
price_qty = products_with_total.select(
    "product_name", "price", "quantity", "line_total"
).filter((col("price") > 0) & (col("quantity") > 0))

display(price_qty.limit(1000))

# COMMAND ----------

# MAGIC %md
# MAGIC 💡 **Scatter Plot**: Click the scatter plot icon to see the correlation!

# COMMAND ----------

# DBTITLE 1,Top 10 Products by Revenue
top_products = product_stats.orderBy(col("total_revenue").desc()).limit(10)

display(top_products)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Best Practices for Data Ingestion
# MAGIC
# MAGIC ### 1. Schema Management
# MAGIC
# MAGIC | Scenario | Recommendation |
# MAGIC |----------|----------------|
# MAGIC | 🔍 Exploration | Use schema inference for speed |
# MAGIC | 🏭 Production | Use explicit schemas for safety |
# MAGIC | 📊 Analytics | Schema inference is fine |
# MAGIC | 🔄 ETL Pipelines | Always use explicit schemas |
# MAGIC
# MAGIC ### 2. Data Quality Checks
# MAGIC
# MAGIC ✅ **Always check**:
# MAGIC - Null counts and percentages
# MAGIC - Duplicate records
# MAGIC - Value ranges (min, max)
# MAGIC - Data type consistency
# MAGIC - Referential integrity
# MAGIC
# MAGIC ### 3. Performance Optimization
# MAGIC
# MAGIC ```python
# MAGIC # ❌ Bad: Read entire dataset then filter
# MAGIC df = spark.read.json(path)
# MAGIC filtered = df.filter(col("state") == "CA")
# MAGIC
# MAGIC # ✅ Good: Use predicate pushdown
# MAGIC df = spark.read.json(path).filter(col("state") == "CA")
# MAGIC ```
# MAGIC
# MAGIC ### 4. Memory Management
# MAGIC
# MAGIC ```python
# MAGIC # ❌ Bad: Load everything into memory
# MAGIC df.collect()  # Don't do this on large datasets!
# MAGIC
# MAGIC # ✅ Good: Use display() or limit()
# MAGIC display(df.limit(100))
# MAGIC ```
# MAGIC
# MAGIC ### 5. Documentation
# MAGIC
# MAGIC ✅ **Document**:
# MAGIC - Data source and update frequency
# MAGIC - Schema definitions
# MAGIC - Known data quality issues
# MAGIC - Transformation logic

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📝 Summary
# MAGIC
# MAGIC ### What We Covered
# MAGIC
# MAGIC ✅ **Data Ingestion**
# MAGIC - Loaded JSON files with nested structures (arrays of structs)
# MAGIC - Defined explicit schemas for complex nested data
# MAGIC
# MAGIC ✅ **Schema Exploration**
# MAGIC - Inspected nested data types and array structures
# MAGIC - Understood how to work with complex schemas
# MAGIC
# MAGIC ✅ **Data Quality**
# MAGIC - Checked for nulls and empty arrays
# MAGIC - Validated nested field values (prices, quantities)
# MAGIC
# MAGIC ✅ **Transformations**
# MAGIC - Used `explode()` to flatten nested arrays
# MAGIC - Calculated derived values (line totals from price × qty)
# MAGIC - Aggregated across nested structures
# MAGIC
# MAGIC ✅ **Visualizations**
# MAGIC - Analyzed product-level and order-level metrics
# MAGIC - Explored customer and product statistics
# MAGIC
# MAGIC ### Key Takeaways
# MAGIC
# MAGIC 💡 **Nested JSON** = Real-world data with arrays and structs (orders → products)
# MAGIC 💡 **explode()** = Flatten arrays to work with individual elements
# MAGIC 💡 **Explicit schemas** = Essential for complex nested structures
# MAGIC 💡 **Derived calculations** = Compute values from nested fields (price × qty)
# MAGIC
# MAGIC ### Next Steps
# MAGIC
# MAGIC 🎯 **Ready for Delta Lake?** Open **02-Delta-Lake-Fundamentals.py** to learn about:
# MAGIC - Creating Delta tables
# MAGIC - CRUD operations (INSERT, UPDATE, DELETE, MERGE)
# MAGIC - Unity Catalog integration
# MAGIC - Table optimization
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Great job! 🚀 You're now ready to work with Delta Lake!**
