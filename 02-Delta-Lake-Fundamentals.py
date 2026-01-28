# Databricks notebook source
# MAGIC %md
# MAGIC # 🔷 Delta Lake Fundamentals
# MAGIC
# MAGIC **Level**: Beginner to Intermediate
# MAGIC **Duration**: 40 minutes
# MAGIC **Prerequisites**: Complete notebooks 00 and 01
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Learning Objectives
# MAGIC
# MAGIC By the end of this notebook, you will:
# MAGIC - ✅ Understand what Delta Lake is and why it's essential
# MAGIC - ✅ Create Delta tables from DataFrames
# MAGIC - ✅ Perform CRUD operations (INSERT, UPDATE, DELETE, MERGE)
# MAGIC - ✅ Register tables in Unity Catalog
# MAGIC - ✅ Query Delta table history and metadata
# MAGIC - ✅ Optimize Delta tables for performance
# MAGIC - ✅ Follow Delta Lake best practices
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📖 Documentation Links
# MAGIC
# MAGIC | Resource | Link |
# MAGIC |----------|------|
# MAGIC | 📘 Delta Lake Guide | [Databricks Delta Tutorial](https://docs.databricks.com/en/delta/tutorial.html) |
# MAGIC | 🔷 Delta Lake Docs | [delta.io](https://delta.io/) |
# MAGIC | 📊 Table Operations | [Delta Lake Operations](https://docs.databricks.com/en/delta/index.html) |
# MAGIC | 🎯 Best Practices | [Delta Lake Best Practices](https://docs.databricks.com/en/delta/best-practices.html) |
# MAGIC | 🔧 Optimization | [Optimize Performance](https://docs.databricks.com/en/delta/optimize.html) |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔧 Setup and Configuration

# COMMAND ----------

# DBTITLE 1,Configure Spark for Demo
# Reduce shuffle partitions for faster demos
spark.conf.set("spark.sql.shuffle.partitions", "1")

print("✅ Spark configured for demo environment")

# COMMAND ----------

# DBTITLE 1,Import Required Libraries
from pyspark.sql.functions import *
from delta.tables import DeltaTable
import shutil
import os

print("✅ Libraries imported")

# COMMAND ----------

# DBTITLE 1,Setup: Load and Prepare Data for This Tutorial
# This creates a clean, easy-to-use table for teaching concepts

from pyspark.sql.functions import explode, col, from_unixtime, sum as _sum

# Load sales orders
sales_raw = spark.read.json("/databricks-datasets/retail-org/sales_orders/")

# Load customers
customers = (
    spark.read.format("csv")
    .option("header", "true")
    .load("/databricks-datasets/retail-org/customers/")
)

# Join and create clean view
sales_with_customers = (
    sales_raw.join(customers, on=["customer_id", "customer_name"], how="left")
    .withColumn(
        "order_datetime_ts",
        from_unixtime(col("order_datetime").cast("long")).cast("timestamp"),
    )
    .withColumn(
        "order_date", from_unixtime(col("order_datetime").cast("long")).cast("date")
    )
)

# Create flattened product view for easy analysis
orders_exploded = sales_with_customers.select(
    "order_number",
    "customer_id",
    "customer_name",
    "order_datetime_ts",
    "order_date",
    "state",
    "city",
    "loyalty_segment",
    explode("ordered_products").alias("product"),
).select(
    "*",
    col("product.name").alias("product_name"),
    col("product.price").alias("price"),
    col("product.qty").alias("quantity"),
    (col("product.price") * col("product.qty")).alias("line_total"),
)

print("✅ Data loaded and prepared")
print(f"📊 Total orders: {sales_with_customers.count():,}")
display(orders_exploded.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔷 What is Delta Lake?
# MAGIC
# MAGIC ### The Evolution of Data Storage
# MAGIC
# MAGIC ```
# MAGIC Traditional Data Lake (Parquet/CSV)
# MAGIC     ↓
# MAGIC     ❌ No ACID transactions
# MAGIC     ❌ No schema enforcement
# MAGIC     ❌ No time travel
# MAGIC     ❌ Difficult updates/deletes
# MAGIC     ↓
# MAGIC Delta Lake = Parquet + Transaction Log
# MAGIC     ↓
# MAGIC     ✅ ACID transactions
# MAGIC     ✅ Schema enforcement & evolution
# MAGIC     ✅ Time travel (version history)
# MAGIC     ✅ Efficient upserts & deletes
# MAGIC     ✅ Audit trail
# MAGIC ```
# MAGIC
# MAGIC ### Key Features Comparison
# MAGIC
# MAGIC | Feature | Parquet | Delta Lake |
# MAGIC |---------|---------|------------|
# MAGIC | **Storage Format** | Columnar | Columnar (Parquet) |
# MAGIC | **ACID Transactions** | ❌ No | ✅ Yes |
# MAGIC | **Schema Enforcement** | ❌ No | ✅ Yes |
# MAGIC | **Time Travel** | ❌ No | ✅ Yes (30 days default) |
# MAGIC | **Updates/Deletes** | ❌ Rewrite entire file | ✅ Efficient row-level |
# MAGIC | **Concurrent Writes** | ❌ Data corruption risk | ✅ Safe with optimistic concurrency |
# MAGIC | **Metadata Operations** | ❌ Slow (list files) | ✅ Fast (transaction log) |
# MAGIC | **Data Quality** | ❌ Manual checks | ✅ Built-in constraints |
# MAGIC
# MAGIC ### How Delta Lake Works
# MAGIC
# MAGIC ```
# MAGIC Delta Table Directory:
# MAGIC ├── _delta_log/
# MAGIC │   ├── 00000000000000000000.json  ← Transaction log (version 0)
# MAGIC │   ├── 00000000000000000001.json  ← Transaction log (version 1)
# MAGIC │   └── 00000000000000000002.json  ← Transaction log (version 2)
# MAGIC ├── part-00000.snappy.parquet      ← Data files
# MAGIC ├── part-00001.snappy.parquet
# MAGIC └── part-00002.snappy.parquet
# MAGIC ```
# MAGIC
# MAGIC 💡 **The transaction log** is the secret sauce – it tracks every change, enabling time travel and ACID guarantees!
# MAGIC
# MAGIC 📖 **Learn More**: [Delta Lake Architecture](https://delta.io/learn/delta-lake-architecture/)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📥 Using Our Prepared Data
# MAGIC
# MAGIC We've already loaded and prepared the retail orders data in the setup cell above. Let's use `orders_exploded` for all our examples:

# COMMAND ----------

# DBTITLE 1,Preview Prepared Data
print(f"✅ Loaded {orders_exploded.count():,} records")
print(f"✅ Columns: {len(orders_exploded.columns)}")
display(orders_exploded.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🏗️ Creating Delta Tables
# MAGIC
# MAGIC ### Method 1: Write DataFrame as Delta Table

# COMMAND ----------

# DBTITLE 1,Create Delta Table from DataFrame
# Define the Delta table path
delta_path = "/tmp/delta/orders_data"

# Remove existing data if present (for demo purposes)
dbutils.fs.rm(delta_path, recurse=True)

# Write DataFrame as Delta table
orders_exploded.write.format("delta").mode("overwrite").save(delta_path)

print(f"✅ Delta table created at: {delta_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Method 2: Create Managed Table (Recommended)
# MAGIC
# MAGIC 💡 **Managed tables** are stored in Unity Catalog and provide:
# MAGIC - Centralized governance
# MAGIC - Access control
# MAGIC - Data lineage
# MAGIC - Automatic cleanup

# COMMAND ----------

# DBTITLE 1,Create Managed Delta Table
# Create a managed table (stored in Unity Catalog)
table_name = "orders_data_delta"

# Drop table if exists (for demo purposes)
spark.sql(f"DROP TABLE IF EXISTS {table_name}")

# Create managed Delta table
orders_exploded.write.format("delta").mode("overwrite").saveAsTable(table_name)

print(f"✅ Managed Delta table created: {table_name}")

# COMMAND ----------

# DBTITLE 1,Verify Table Creation
# Show table information
spark.sql(f"DESCRIBE EXTENDED {table_name}").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Reading Delta Tables
# MAGIC
# MAGIC ### Read from Path

# COMMAND ----------

# DBTITLE 1,Read Delta Table from Path
# Read Delta table using path
delta_df = spark.read.format("delta").load(delta_path)

print(f"✅ Read {delta_df.count():,} records from Delta table")
display(delta_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read from Table Name

# COMMAND ----------

# DBTITLE 1,Read Delta Table by Name
# Read using table name (simpler!)
delta_df_table = spark.table(table_name)

print(f"✅ Read {delta_df_table.count():,} records from managed table")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query with SQL

# COMMAND ----------

# DBTITLE 1,Query Delta Table with SQL
# Use SQL to query the Delta table
result = spark.sql(f"""
    SELECT 
        state,
        COUNT(*) as order_count,
        ROUND(AVG(line_total), 2) as avg_order_value,
        ROUND(SUM(line_total), 2) as total_revenue
    FROM {table_name}
    GROUP BY state
    ORDER BY total_revenue DESC
    LIMIT 10
""")

display(result)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ➕ INSERT Operations
# MAGIC
# MAGIC ### Append New Records

# COMMAND ----------

# DBTITLE 1,Create New Records to Insert
# Create sample new order records
from pyspark.sql import Row
from datetime import datetime

new_orders = spark.createDataFrame(
    [
        Row(
            order_number=999001,
            customer_id=1001,
            customer_name="John Smith",
            order_datetime_ts=datetime.now(),
            order_date=datetime.now().date(),
            state="NY",
            city="New York",
            loyalty_segment="Gold",
            product_name="Laptop",
            price=1200.0,
            quantity=1,
            line_total=1200.0,
        ),
        Row(
            order_number=999002,
            customer_id=1002,
            customer_name="Jane Doe",
            order_datetime_ts=datetime.now(),
            order_date=datetime.now().date(),
            state="CA",
            city="San Francisco",
            loyalty_segment="Platinum",
            product_name="Monitor",
            price=350.0,
            quantity=2,
            line_total=700.0,
        ),
        Row(
            order_number=999003,
            customer_id=1003,
            customer_name="Bob Johnson",
            order_datetime_ts=datetime.now(),
            order_date=datetime.now().date(),
            state="TX",
            city="Austin",
            loyalty_segment="Silver",
            product_name="Keyboard",
            price=85.0,
            quantity=1,
            line_total=85.0,
        ),
    ]
)

print(f"✅ Created {new_orders.count()} new order records")
display(new_orders)

# COMMAND ----------

# DBTITLE 1,Insert Records into Delta Table
# Get count before insert
count_before = spark.table(table_name).count()

# Append new records
new_orders.write.format("delta").mode("append").saveAsTable(table_name)

# Get count after insert
count_after = spark.table(table_name).count()

print(f"✅ Records before: {count_before:,}")
print(f"✅ Records after: {count_after:,}")
print(f"✅ Records inserted: {count_after - count_before}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Insert with SQL

# COMMAND ----------

# DBTITLE 1,Insert Using SQL
# Insert records using SQL syntax
spark.sql(f"""
    INSERT INTO {table_name}
    VALUES (
        999004, 1004, 'Alice Brown', current_timestamp(), current_date(), 'FL', 'Miami', 'Gold',
        'Mouse', 45.0, 1, 45.0
    )
""")

print("✅ Record inserted via SQL")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔄 UPDATE Operations
# MAGIC
# MAGIC ### Update with DeltaTable API

# COMMAND ----------

# DBTITLE 1,Update Records Using DeltaTable API
# Load Delta table for updates
delta_table = DeltaTable.forName(spark, table_name)

# Update line_total for orders from California (apply 10% discount)
delta_table.update(
    condition="state = 'CA'",
    set={"line_total": "line_total * 0.9"},
)

print("✅ Updated CA orders with 10% discount")

# COMMAND ----------

# DBTITLE 1,Verify Updates
# Check updated records
updated_records = spark.sql(f"""
    SELECT state, product_name, price, quantity, line_total
    FROM {table_name}
    WHERE state = 'CA'
    LIMIT 10
""")

display(updated_records)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Update with SQL

# COMMAND ----------

# DBTITLE 1,Update Using SQL
# Update using SQL syntax
spark.sql(f"""
    UPDATE {table_name}
    SET line_total = line_total * 0.95
    WHERE state = 'NY'
""")

print("✅ Updated NY orders with 5% discount")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ❌ DELETE Operations
# MAGIC
# MAGIC ### Delete with DeltaTable API

# COMMAND ----------

# DBTITLE 1,Delete Records Using DeltaTable API
# Count before delete
count_before = spark.table(table_name).count()

# Delete orders with very low line totals (< $10)
delta_table.delete("line_total < 10")

# Count after delete
count_after = spark.table(table_name).count()

print(f"✅ Records before: {count_before:,}")
print(f"✅ Records after: {count_after:,}")
print(f"✅ Records deleted: {count_before - count_after}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Delete with SQL

# COMMAND ----------

# DBTITLE 1,Delete Using SQL
# Delete using SQL syntax
spark.sql(f"""
    DELETE FROM {table_name}
    WHERE price < 20
""")

print("✅ Deleted products under $20")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔀 MERGE Operations (Upsert)
# MAGIC
# MAGIC ### Why MERGE Matters
# MAGIC
# MAGIC **MERGE** (also called UPSERT) is essential for:
# MAGIC - 📊 Change Data Capture (CDC)
# MAGIC - 🔄 Slowly Changing Dimensions (SCD)
# MAGIC - 🔁 Incremental data loads
# MAGIC - 🎯 Deduplication
# MAGIC
# MAGIC ### MERGE Logic
# MAGIC
# MAGIC ```
# MAGIC IF record exists (matched):
# MAGIC     → UPDATE with new values
# MAGIC ELSE (not matched):
# MAGIC     → INSERT new record
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Create Update Dataset
# Create a dataset with updates and new records
updates = spark.createDataFrame(
    [
        Row(
            order_number=999001,
            customer_id=1001,
            customer_name="John Smith",
            order_datetime_ts=datetime.now(),
            order_date=datetime.now().date(),
            state="NY",
            city="New York",
            loyalty_segment="Platinum",
            product_name="Laptop",
            price=1100.0,
            quantity=1,
            line_total=1100.0,
        ),
        Row(
            order_number=999005,
            customer_id=1005,
            customer_name="Carol White",
            order_datetime_ts=datetime.now(),
            order_date=datetime.now().date(),
            state="WA",
            city="Seattle",
            loyalty_segment="Gold",
            product_name="Tablet",
            price=500.0,
            quantity=1,
            line_total=500.0,
        ),
    ]
)

print(f"✅ Created {updates.count()} records for merge")
display(updates)

# COMMAND ----------

# DBTITLE 1,Perform MERGE Operation
# Count before merge
count_before = spark.table(table_name).count()

# Perform merge (upsert)
delta_table.alias("target").merge(
    updates.alias("source"),
    "target.order_number = source.order_number AND target.state = source.state",
).whenMatchedUpdate(
    set={"loyalty_segment": "source.loyalty_segment", "line_total": "source.line_total"}
).whenNotMatchedInsertAll().execute()

# Count after merge
count_after = spark.table(table_name).count()

print(f"✅ Records before: {count_before:,}")
print(f"✅ Records after: {count_after:,}")
print(f"✅ Records inserted: {count_after - count_before}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### MERGE with SQL

# COMMAND ----------

# DBTITLE 1,MERGE Using SQL
# Create a temporary view for the source data
updates.createOrReplaceTempView("order_updates")

# Perform merge using SQL
spark.sql(f"""
    MERGE INTO {table_name} AS target
    USING order_updates AS source
    ON target.order_number = source.order_number 
       AND target.state = source.state
    WHEN MATCHED THEN
        UPDATE SET target.line_total = source.line_total
    WHEN NOT MATCHED THEN
        INSERT *
""")

print("✅ MERGE completed via SQL")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🕰️ Time Travel
# MAGIC
# MAGIC ### Why Time Travel?
# MAGIC
# MAGIC ✅ **Audit and compliance** – See data at any point in time
# MAGIC ✅ **Rollback mistakes** – Undo bad updates or deletes
# MAGIC ✅ **Reproduce results** – Query historical data for ML models
# MAGIC ✅ **Debug issues** – Compare versions to find when data changed

# COMMAND ----------

# DBTITLE 1,View Table History
# Show all versions of the Delta table
history = spark.sql(f"DESCRIBE HISTORY {table_name}")
display(history)

# COMMAND ----------

# DBTITLE 1,Query Previous Version by Version Number
# Query version 0 (original data)
version_0 = spark.read.format("delta").option("versionAsOf", 0).table(table_name)

print(f"✅ Version 0 record count: {version_0.count():,}")
display(version_0.limit(5))

# COMMAND ----------

# DBTITLE 1,Query Previous Version by Timestamp
# Query data as of a specific timestamp
# Get timestamp from history
first_timestamp = history.select("timestamp").first()[0]

version_by_time = (
    spark.read.format("delta")
    .option("timestampAsOf", first_timestamp)
    .table(table_name)
)

print(f"✅ Record count at {first_timestamp}: {version_by_time.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Restore to Previous Version

# COMMAND ----------

# DBTITLE 1,Restore Table to Previous Version
# Restore to version 0 (undo all changes)
spark.sql(f"RESTORE TABLE {table_name} TO VERSION AS OF 0")

print("✅ Table restored to version 0")
print(f"✅ Current record count: {spark.table(table_name).count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🚀 Optimization
# MAGIC
# MAGIC ### Why Optimize?
# MAGIC
# MAGIC Over time, Delta tables accumulate:
# MAGIC - 📁 Many small files (slow reads)
# MAGIC - 🗑️ Old versions (wasted storage)
# MAGIC - 📊 Unoptimized data layout (slow queries)

# COMMAND ----------

# MAGIC %md
# MAGIC ### OPTIMIZE Command
# MAGIC
# MAGIC **OPTIMIZE** compacts small files into larger ones:
# MAGIC
# MAGIC ```
# MAGIC Before:  [10MB] [5MB] [8MB] [12MB] [7MB] ... (100 files)
# MAGIC After:   [128MB] [128MB] [128MB] ... (10 files)
# MAGIC Result:  ✅ Faster reads, fewer file operations
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Optimize Delta Table
# Compact small files
spark.sql(f"OPTIMIZE {table_name}")

print("✅ Table optimized (small files compacted)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Z-Ordering
# MAGIC
# MAGIC **Z-Ordering** co-locates related data for faster queries:
# MAGIC
# MAGIC ```
# MAGIC Without Z-Order:  Query "WHERE state='CA'" → Read all files
# MAGIC With Z-Order:     Query "WHERE state='CA'" → Read only CA files
# MAGIC Result:           ✅ 10-100x faster queries
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Optimize with Z-Ordering
# Optimize and Z-order by frequently queried column
spark.sql(f"OPTIMIZE {table_name} ZORDER BY (addr_state)")

print("✅ Table optimized with Z-ordering on addr_state")

# COMMAND ----------

# MAGIC %md
# MAGIC ### VACUUM Command
# MAGIC
# MAGIC **VACUUM** removes old data files no longer needed:
# MAGIC
# MAGIC ⚠️ **Warning**: After VACUUM, you can't time travel beyond the retention period!

# COMMAND ----------

# DBTITLE 1,Vacuum Old Files
# Remove files older than 7 days (default is 7 days)
# Note: Set retention to 0 hours for demo (NOT recommended for production!)
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

spark.sql(f"VACUUM {table_name} RETAIN 0 HOURS")

print("✅ Old files vacuumed")
print("⚠️  Time travel is now limited to current version")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Table Metadata and Statistics

# COMMAND ----------

# DBTITLE 1,Show Table Details
# Get detailed table information
spark.sql(f"DESCRIBE DETAIL {table_name}").show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Show Table Properties
# View table properties
spark.sql(f"SHOW TBLPROPERTIES {table_name}").show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Show Table Statistics
# Generate and view statistics
spark.sql(f"ANALYZE TABLE {table_name} COMPUTE STATISTICS")

print("✅ Table statistics computed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Best Practices
# MAGIC
# MAGIC ### 1. Table Design
# MAGIC
# MAGIC | Practice | Recommendation |
# MAGIC |----------|----------------|
# MAGIC | 📁 **File Size** | Target 128MB-1GB per file |
# MAGIC | 🔑 **Partitioning** | Partition by date/region if queries filter on it |
# MAGIC | 🎯 **Z-Ordering** | Z-order by high-cardinality columns (e.g., user_id) |
# MAGIC | 📊 **Schema** | Use explicit schemas in production |
# MAGIC
# MAGIC ### 2. Write Operations
# MAGIC
# MAGIC ```python
# MAGIC # ❌ Bad: Many small writes
# MAGIC for record in records:
# MAGIC     df.write.mode("append").save(path)
# MAGIC
# MAGIC # ✅ Good: Batch writes
# MAGIC batch_df.write.mode("append").save(path)
# MAGIC ```
# MAGIC
# MAGIC ### 3. Optimization Schedule
# MAGIC
# MAGIC | Operation | Frequency | Purpose |
# MAGIC |-----------|-----------|---------|
# MAGIC | **OPTIMIZE** | Daily/Weekly | Compact small files |
# MAGIC | **VACUUM** | Weekly/Monthly | Remove old files |
# MAGIC | **ANALYZE** | After major changes | Update statistics |
# MAGIC
# MAGIC ### 4. Time Travel
# MAGIC
# MAGIC ✅ **Do**:
# MAGIC - Set appropriate retention (default 7 days)
# MAGIC - Use time travel for audits and rollbacks
# MAGIC - Document version numbers for reproducibility
# MAGIC
# MAGIC ❌ **Don't**:
# MAGIC - Rely on time travel for backups
# MAGIC - Set retention too low (< 7 days)
# MAGIC - Forget to VACUUM old versions
# MAGIC
# MAGIC ### 5. Performance Tips
# MAGIC
# MAGIC ```python
# MAGIC # ✅ Use predicate pushdown
# MAGIC df = spark.read.format("delta").load(path).filter("date >= '2024-01-01'")
# MAGIC
# MAGIC # ✅ Use column pruning
# MAGIC df = spark.read.format("delta").load(path).select("id", "name", "amount")
# MAGIC
# MAGIC # ✅ Cache frequently accessed tables
# MAGIC df.cache()
# MAGIC ```
# MAGIC
# MAGIC 📖 **Learn More**: [Delta Lake Best Practices](https://docs.databricks.com/en/delta/best-practices.html)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📝 Summary
# MAGIC
# MAGIC ### What We Covered
# MAGIC
# MAGIC ✅ **Delta Lake Basics**
# MAGIC - Understood Delta Lake architecture (Parquet + transaction log)
# MAGIC - Learned key benefits: ACID, time travel, schema enforcement
# MAGIC
# MAGIC ✅ **Table Creation**
# MAGIC - Created Delta tables from DataFrames
# MAGIC - Registered managed tables in Unity Catalog
# MAGIC
# MAGIC ✅ **CRUD Operations**
# MAGIC - **INSERT**: Added new records with append
# MAGIC - **UPDATE**: Modified existing records
# MAGIC - **DELETE**: Removed unwanted records
# MAGIC - **MERGE**: Performed upserts (update + insert)
# MAGIC
# MAGIC ✅ **Time Travel**
# MAGIC - Queried historical versions by version number and timestamp
# MAGIC - Restored tables to previous versions
# MAGIC
# MAGIC ✅ **Optimization**
# MAGIC - Compacted small files with OPTIMIZE
# MAGIC - Improved query performance with Z-ordering
# MAGIC - Cleaned up old files with VACUUM
# MAGIC
# MAGIC ### Key Takeaways
# MAGIC
# MAGIC 💡 **Delta Lake** = Reliable data lake with ACID guarantees
# MAGIC 💡 **MERGE** = Essential for CDC and incremental loads
# MAGIC 💡 **Time Travel** = Audit trail and rollback capability
# MAGIC 💡 **OPTIMIZE + Z-ORDER** = Fast queries on large datasets
# MAGIC 💡 **VACUUM** = Clean up old files (but limits time travel)
# MAGIC
# MAGIC ### Next Steps
# MAGIC
# MAGIC 🎯 **Ready for advanced features?** The next notebooks will cover:
# MAGIC - **03-Delta-Lake-Advanced**: Schema evolution, constraints, CDC patterns
# MAGIC - **04-Streaming-with-Delta**: Real-time data processing with Structured Streaming
# MAGIC - **05-Production-Patterns**: Data quality, monitoring, performance tuning
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Congratulations! 🎉 You now understand Delta Lake fundamentals!**
