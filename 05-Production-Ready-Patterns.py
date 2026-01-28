# Databricks notebook source
# MAGIC %md
# MAGIC # 🏭 Production-Ready Patterns
# MAGIC
# MAGIC **Level**: Advanced
# MAGIC **Duration**: 60 minutes
# MAGIC **Prerequisites**: Complete notebooks 00-04
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Learning Objectives
# MAGIC
# MAGIC By the end of this notebook, you will:
# MAGIC - ✅ Implement **Predictive Optimization** (NEW - GA June 2024) for automatic maintenance
# MAGIC - ✅ Use **Serverless Compute** (NEW - GA 2025) for zero infrastructure management
# MAGIC - ✅ Convert external tables to managed with **SET MANAGED** (NEW - 2024)
# MAGIC - ✅ Create **Unity Catalog Managed Iceberg Tables** (NEW - 2024)
# MAGIC - ✅ Enable **Delta UniForm** for Iceberg compatibility
# MAGIC - ✅ Implement data quality frameworks
# MAGIC - ✅ Monitor and tune performance
# MAGIC - ✅ Apply data governance with Unity Catalog
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📖 Documentation Links
# MAGIC
# MAGIC | Resource | Link |
# MAGIC |----------|------|
# MAGIC | 🤖 Predictive Optimization | [Predictive Optimization Docs](https://docs.databricks.com/en/optimizations/predictive-optimization.html) |
# MAGIC | 💻 Serverless Compute | [Serverless Guide](https://docs.databricks.com/en/compute/serverless.html) |
# MAGIC | 🔄 Convert to Managed | [Convert External Tables](https://docs.databricks.com/en/tables/convert-external-managed.html) |
# MAGIC | 🧊 Iceberg Tables | [Unity Catalog Iceberg](https://docs.databricks.com/en/iceberg/index.html) |
# MAGIC | 🔗 Delta UniForm | [UniForm Guide](https://docs.databricks.com/en/delta/uniform.html) |
# MAGIC | 🤖 Predictive Optimization GA Blog | [GA Announcement](https://www.databricks.com/blog/announcing-general-availability-predictive-optimization) |
# MAGIC | 📊 Predictive Optimization Performance | [Performance Blog](https://www.databricks.com/blog/predictive-optimization-automatically-delivers-faster-queries-and-lower-tco) |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔧 Setup and Configuration

# COMMAND ----------

# DBTITLE 1,Configure Spark for Production
# Production-optimized settings
spark.conf.set("spark.sql.shuffle.partitions", "auto")  # Let Spark decide

print("✅ Spark configured for production environment")

# COMMAND ----------

# DBTITLE 1,Import Required Libraries
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable
import time

print("✅ Libraries imported")

# COMMAND ----------

# DBTITLE 1,Setup: Load and Prepare Data for This Tutorial
from pyspark.sql.functions import explode, col, from_unixtime, sum as _sum

sales_raw = spark.read.json("/databricks-datasets/retail-org/sales_orders/")
customers = (
    spark.read.format("csv")
    .option("header", "true")
    .load("/databricks-datasets/retail-org/customers/")
)

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

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🤖 Predictive Optimization (NEW - GA June 2024)
# MAGIC
# MAGIC ### The Old Way vs The New Way
# MAGIC
# MAGIC | Old Approach | New Approach (Predictive Optimization) |
# MAGIC |--------------|----------------------------------------|
# MAGIC | ❌ **Manual OPTIMIZE**: Schedule cron jobs, guess frequency | ✅ **Automatic OPTIMIZE**: AI decides when to run |
# MAGIC | ❌ **Manual VACUUM**: Risk data loss, manual scripts | ✅ **Safe automatic VACUUM**: AI manages retention |
# MAGIC | ❌ **Manual clustering**: Run Z-ORDER commands manually | ✅ **Automatic liquid clustering**: AI optimizes layout |
# MAGIC | ❌ **Wasted compute**: Run optimization when not needed | ✅ **Smart scheduling**: Only runs when beneficial |
# MAGIC | ❌ **High management effort**: Ongoing tuning required | ✅ **Zero management**: Set once, forget forever |
# MAGIC
# MAGIC ### Why This Matters
# MAGIC
# MAGIC **Predictive Optimization delivers**:
# MAGIC - 🚀 **2x faster queries** on average
# MAGIC - 💰 **50% storage cost reduction** through automatic VACUUM
# MAGIC - ⚡ **Automatic liquid clustering** for optimal data layout
# MAGIC - 🤖 **AI-driven scheduling** based on Unity Catalog query patterns
# MAGIC - 🔄 **Zero maintenance** overhead
# MAGIC
# MAGIC ### How It Works
# MAGIC
# MAGIC ```
# MAGIC 1. AI analyzes Unity Catalog query logs
# MAGIC 2. Identifies tables that would benefit from optimization
# MAGIC 3. Automatically runs OPTIMIZE, VACUUM, and clustering
# MAGIC 4. Schedules during low-usage periods
# MAGIC 5. Continuously adapts to workload changes
# MAGIC ```
# MAGIC
# MAGIC ### Enabled by Default
# MAGIC
# MAGIC ✅ **Enabled by default** for new Databricks accounts (as of November 2024)
# MAGIC ✅ **Works with**: Delta tables, Iceberg tables, liquid clustering
# MAGIC ✅ **Requires**: Unity Catalog (for query pattern analysis)
# MAGIC
# MAGIC 📖 **Learn More**:
# MAGIC - [Predictive Optimization GA Announcement](https://www.databricks.com/blog/announcing-general-availability-predictive-optimization)
# MAGIC - [Performance Results](https://www.databricks.com/blog/predictive-optimization-automatically-delivers-faster-queries-and-lower-tco)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Table Management Comparison
# MAGIC
# MAGIC | Task | Old Way | New Way (Predictive Optimization) |
# MAGIC |------|---------|-----------------------------------|
# MAGIC | **OPTIMIZE** | Schedule cron jobs, guess frequency | ✅ Automatic, AI-driven |
# MAGIC | **VACUUM** | Manual scripts, risk data loss | ✅ Safe automatic cleanup |
# MAGIC | **Clustering** | Manual Z-ORDER commands | ✅ Automatic liquid clustering |
# MAGIC | **Cost** | Wasted compute on unnecessary runs | ✅ Only runs when beneficial |
# MAGIC | **Management Effort** | High (ongoing tuning) | ✅ Zero (set once, forget) |
# MAGIC | **Query Performance** | Degrades over time without maintenance | ✅ Consistently fast |
# MAGIC | **Storage Costs** | Grows without VACUUM | ✅ Automatically optimized |

# COMMAND ----------

# DBTITLE 1,Create Table for Predictive Optimization
# Create a Delta table
po_table = "orders_predictive_optimization"
spark.sql(f"DROP TABLE IF EXISTS {po_table}")

orders_exploded.write.format("delta").mode("overwrite").saveAsTable(po_table)

print(f"✅ Created table: {po_table}")

# COMMAND ----------

# DBTITLE 1,Enable Predictive Optimization
# Enable predictive optimization on the table
spark.sql(f"""
    ALTER TABLE {po_table}
    SET TBLPROPERTIES ('delta.enablePredictiveOptimization' = 'true')
""")

print(f"✅ Enabled Predictive Optimization on {po_table}")
print("🤖 AI will automatically:")
print("   - Run OPTIMIZE when beneficial")
print("   - VACUUM old files safely")
print("   - Apply liquid clustering (if enabled)")
print("   - Schedule during low-usage periods")

# COMMAND ----------

# DBTITLE 1,Check Predictive Optimization Status
# View table details to see Predictive Optimization status
table_details = spark.sql(f"DESCRIBE DETAIL {po_table}")
display(table_details.select("name", "format", "properties"))

# COMMAND ----------

# DBTITLE 1,View Predictive Optimization Operations
# Query system table for Predictive Optimization history
# Note: This requires Unity Catalog and may not be available in all environments
try:
    po_history = spark.sql(f"""
        SELECT 
            table_name,
            operation_type,
            operation_start_time,
            operation_end_time,
            metrics
        FROM system.access.predictive_optimization_operations
        WHERE table_name = '{po_table}'
        ORDER BY operation_start_time DESC
        LIMIT 10
    """)
    display(po_history)
except Exception as e:
    print("ℹ️  Predictive Optimization history requires Unity Catalog")
    print(f"   Error: {str(e)[:100]}...")

# COMMAND ----------

# MAGIC %md
# MAGIC 💡 **Pro Tip**: Predictive Optimization works best with:
# MAGIC - Tables in Unity Catalog
# MAGIC - Liquid clustering enabled
# MAGIC - Regular query patterns (so AI can learn)
# MAGIC - Tables > 1GB (smaller tables don't benefit as much)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 💻 Serverless Compute (NEW - GA 2025)
# MAGIC
# MAGIC ### The Old Way vs The New Way
# MAGIC
# MAGIC | Old Approach | New Approach (Serverless) |
# MAGIC |--------------|---------------------------|
# MAGIC | ❌ **Manual cluster config**: Choose instance types, autoscaling | ✅ **Zero configuration**: Just run your code |
# MAGIC | ❌ **Slow startup**: Wait 5-10 minutes for cluster | ✅ **Instant start**: Sub-second startup |
# MAGIC | ❌ **Pay for idle time**: Cluster runs even when not used | ✅ **Pay per second**: Only pay for actual compute |
# MAGIC | ❌ **Manual scaling**: Configure min/max workers | ✅ **Auto-scaling**: Scales instantly to workload |
# MAGIC | ❌ **Cluster management**: Monitor, restart, upgrade | ✅ **Fully managed**: No infrastructure to manage |
# MAGIC
# MAGIC ### Why This Matters
# MAGIC
# MAGIC **Serverless Compute delivers**:
# MAGIC - ⚡ **Instant startup** (sub-second vs. 5-10 minutes)
# MAGIC - 💰 **Pay-per-second billing** (no idle costs)
# MAGIC - 🔄 **Automatic scaling** (no configuration needed)
# MAGIC - 🛡️ **Built-in security** (automatic isolation)
# MAGIC - 🚀 **Latest runtime** (always up-to-date)
# MAGIC
# MAGIC ### How It Works
# MAGIC
# MAGIC ```
# MAGIC Traditional Cluster:
# MAGIC 1. Configure cluster (instance types, autoscaling)
# MAGIC 2. Wait 5-10 minutes for startup
# MAGIC 3. Run workload
# MAGIC 4. Pay for entire cluster lifetime
# MAGIC
# MAGIC Serverless:
# MAGIC 1. Attach notebook to serverless
# MAGIC 2. Run code (instant start)
# MAGIC 3. Pay only for compute used (per second)
# MAGIC ```
# MAGIC
# MAGIC ### Availability
# MAGIC
# MAGIC ✅ **GA for notebooks** (2025)
# MAGIC ✅ **GA for jobs** (2024)
# MAGIC ✅ **GA for SQL warehouses** (2023)
# MAGIC
# MAGIC 📖 **Learn More**: [Serverless Compute Guide](https://docs.databricks.com/en/compute/serverless.html)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Using Serverless Compute
# MAGIC
# MAGIC **For Notebooks**:
# MAGIC ```
# MAGIC 1. Click cluster dropdown in top-right
# MAGIC 2. Select "Serverless"
# MAGIC 3. Run your code (instant start!)
# MAGIC ```
# MAGIC
# MAGIC **For Jobs** (JSON configuration):
# MAGIC ```json
# MAGIC {
# MAGIC   "name": "My Serverless Job",
# MAGIC   "tasks": [{
# MAGIC     "task_key": "process_data",
# MAGIC     "notebook_task": {
# MAGIC       "notebook_path": "/path/to/notebook"
# MAGIC     },
# MAGIC     "serverless_compute": {
# MAGIC       "enabled": true
# MAGIC     }
# MAGIC   }]
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC **For SQL Warehouses**:
# MAGIC ```
# MAGIC 1. Create SQL Warehouse
# MAGIC 2. Select "Serverless" type
# MAGIC 3. Run queries (instant start!)
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Example: Serverless-Optimized Query
# This query runs efficiently on serverless compute
# (No special code changes needed - same DataFrame API!)

serverless_result = spark.sql(f"""
    SELECT 
        addr_state,
        COUNT(*) as loan_count,
        ROUND(AVG(loan_amnt), 2) as avg_loan_amount,
        ROUND(SUM(loan_amnt), 2) as total_loan_amount
    FROM {po_table}
    GROUP BY addr_state
    ORDER BY total_loan_amount DESC
    LIMIT 10
""")

display(serverless_result)

print("✅ Query executed")
print("💡 On serverless: instant start, pay-per-second billing")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔄 Convert External to Managed Tables (NEW - 2024)
# MAGIC
# MAGIC ### Why Convert to Managed?
# MAGIC
# MAGIC **Managed tables unlock**:
# MAGIC - 🤖 **Predictive Optimization** (only works on managed tables)
# MAGIC - 🔒 **Full Unity Catalog features** (governance, lineage)
# MAGIC - 🗑️ **Automatic cleanup** (DROP TABLE deletes data)
# MAGIC - 📊 **Better performance** (optimized storage location)
# MAGIC
# MAGIC ### External vs Managed
# MAGIC
# MAGIC | Feature | External Table | Managed Table |
# MAGIC |---------|---------------|---------------|
# MAGIC | **Data Location** | User-specified path | Unity Catalog managed |
# MAGIC | **DROP TABLE** | Deletes metadata only | Deletes data + metadata |
# MAGIC | **Predictive Optimization** | ❌ Not supported | ✅ Supported |
# MAGIC | **Unity Catalog Features** | ⚠️ Limited | ✅ Full support |
# MAGIC | **Use Case** | External data sources | Production tables |

# COMMAND ----------

# DBTITLE 1,Create External Table
# Create an external table (data in user-specified location)
external_path = "/tmp/external_loans"
external_table = "loans_external"

# Clean up
dbutils.fs.rm(external_path, recurse=True)
spark.sql(f"DROP TABLE IF EXISTS {external_table}")

# Create external table
df.write.format("delta").mode("overwrite").save(external_path)
spark.sql(f"""
    CREATE TABLE {external_table}
    USING DELTA
    LOCATION '{external_path}'
""")

print(f"✅ Created external table: {external_table}")
print(f"📁 Data location: {external_path}")

# COMMAND ----------

# DBTITLE 1,Convert External to Managed
# Convert external table to managed
spark.sql(f"""
    ALTER TABLE {external_table}
    SET MANAGED
""")

print(f"✅ Converted {external_table} to managed table")
print("🤖 Now eligible for Predictive Optimization!")
print("🔒 Full Unity Catalog features enabled")

# COMMAND ----------

# DBTITLE 1,Verify Conversion
# Check table type
table_info = spark.sql(f"DESCRIBE EXTENDED {external_table}")
display(table_info.filter(col("col_name").isin(["Type", "Location", "Provider"])))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧊 Unity Catalog Managed Iceberg Tables (NEW - 2024)
# MAGIC
# MAGIC ### What are Iceberg Tables?
# MAGIC
# MAGIC **Apache Iceberg** is an open table format that provides:
# MAGIC - 🌍 **Interoperability**: Read from Spark, Trino, Flink, Presto
# MAGIC - 🔄 **ACID transactions**: Like Delta Lake
# MAGIC - 🕰️ **Time travel**: Query historical data
# MAGIC - 📊 **Schema evolution**: Add/modify columns safely
# MAGIC
# MAGIC ### Why Unity Catalog Iceberg?
# MAGIC
# MAGIC **Unity Catalog Managed Iceberg tables offer**:
# MAGIC - ✅ **Native Iceberg format** in Unity Catalog
# MAGIC - ✅ **Liquid clustering support** (NEW - 2024)
# MAGIC - ✅ **Predictive Optimization** (NEW - 2024)
# MAGIC - ✅ **Multi-engine access** (Spark, Trino, Flink)
# MAGIC - ✅ **Unity Catalog governance** (ACLs, lineage, audit)
# MAGIC
# MAGIC ### Iceberg vs Delta
# MAGIC
# MAGIC | Feature | Delta Lake | Iceberg |
# MAGIC |---------|-----------|---------|
# MAGIC | **ACID Transactions** | ✅ Yes | ✅ Yes |
# MAGIC | **Time Travel** | ✅ Yes | ✅ Yes |
# MAGIC | **Schema Evolution** | ✅ Yes | ✅ Yes |
# MAGIC | **Databricks Optimization** | ✅ Full support | ✅ Full support (NEW) |
# MAGIC | **Multi-engine Support** | ⚠️ Limited | ✅ Excellent |
# MAGIC | **Best For** | Databricks-native workloads | Multi-engine environments |
# MAGIC
# MAGIC 📖 **Learn More**: [Unity Catalog Iceberg Tables](https://docs.databricks.com/en/iceberg/index.html)

# COMMAND ----------

# DBTITLE 1,Create Managed Iceberg Table
# Create a managed Iceberg table in Unity Catalog
iceberg_table = "loans_iceberg"
spark.sql(f"DROP TABLE IF EXISTS {iceberg_table}")

spark.sql(f"""
    CREATE TABLE {iceberg_table} (
        loan_amnt INT,
        funded_amnt INT,
        paid_amnt DOUBLE,
        addr_state STRING,
        annual_inc DOUBLE,
        term STRING,
        purpose STRING,
        home_ownership STRING
    )
    USING iceberg
    CLUSTER BY (addr_state)
""")

print(f"✅ Created managed Iceberg table: {iceberg_table}")
print("🧊 Format: Apache Iceberg")
print("⚡ Liquid clustering enabled on addr_state")

# COMMAND ----------

# DBTITLE 1,Insert Data into Iceberg Table
# Insert data into Iceberg table
df.select(
    "loan_amnt",
    "funded_amnt",
    "paid_amnt",
    "addr_state",
    "annual_inc",
    "term",
    "purpose",
    "home_ownership",
).write.format("iceberg").mode("append").saveAsTable(iceberg_table)

print(f"✅ Inserted {df.count():,} records into Iceberg table")

# COMMAND ----------

# DBTITLE 1,Query Iceberg Table
# Query the Iceberg table (same as Delta!)
iceberg_result = spark.sql(f"""
    SELECT 
        addr_state,
        COUNT(*) as loan_count,
        ROUND(AVG(loan_amnt), 2) as avg_loan_amount
    FROM {iceberg_table}
    GROUP BY addr_state
    ORDER BY loan_count DESC
    LIMIT 10
""")

display(iceberg_result)

# COMMAND ----------

# DBTITLE 1,Enable Predictive Optimization on Iceberg
# Enable Predictive Optimization on Iceberg table
spark.sql(f"""
    ALTER TABLE {iceberg_table}
    SET TBLPROPERTIES ('delta.enablePredictiveOptimization' = 'true')
""")

print(f"✅ Enabled Predictive Optimization on Iceberg table")
print("🤖 AI will automatically optimize Iceberg tables too!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔗 Delta UniForm (Iceberg Compatibility)
# MAGIC
# MAGIC ### What is Delta UniForm?
# MAGIC
# MAGIC **Delta UniForm** enables reading Delta tables as Iceberg:
# MAGIC
# MAGIC ```
# MAGIC One Table, Multiple Readers:
# MAGIC
# MAGIC Delta Table (storage)
# MAGIC     ↓
# MAGIC     ├─→ Databricks (reads as Delta)
# MAGIC     ├─→ Trino (reads as Iceberg)
# MAGIC     ├─→ Flink (reads as Iceberg)
# MAGIC     └─→ Presto (reads as Iceberg)
# MAGIC ```
# MAGIC
# MAGIC ### Why UniForm?
# MAGIC
# MAGIC ✅ **One table, multiple formats** – No data duplication
# MAGIC ✅ **Databricks writes, others read** – Best of both worlds
# MAGIC ✅ **No performance penalty** – Metadata-only conversion
# MAGIC ✅ **Automatic sync** – Iceberg metadata auto-updated
# MAGIC
# MAGIC 📖 **Learn More**: [Delta UniForm Guide](https://docs.databricks.com/en/delta/uniform.html)

# COMMAND ----------

# DBTITLE 1,Create Delta Table with UniForm
# Create Delta table with Iceberg compatibility enabled
uniform_table = "loans_uniform"
spark.sql(f"DROP TABLE IF EXISTS {uniform_table}")

spark.sql(f"""
    CREATE TABLE {uniform_table}
    USING DELTA
    TBLPROPERTIES (
        'delta.enableIcebergCompatV2' = 'true',
        'delta.universalFormat.enabledFormats' = 'iceberg'
    )
    AS SELECT * FROM {po_table}
""")

print(f"✅ Created Delta table with UniForm: {uniform_table}")
print("🔗 Readable as both Delta and Iceberg!")

# COMMAND ----------

# DBTITLE 1,Verify UniForm Configuration
# Check table properties
uniform_props = spark.sql(f"SHOW TBLPROPERTIES {uniform_table}")
display(
    uniform_props.filter(col("key").like("%iceberg%") | col("key").like("%uniform%"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC 💡 **Use Case**: Write with Databricks (Delta), read with Trino/Flink (Iceberg) - no data duplication!

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Data Quality Framework
# MAGIC
# MAGIC ### Why Data Quality Matters
# MAGIC
# MAGIC Poor data quality leads to:
# MAGIC - ❌ Incorrect analytics and reports
# MAGIC - ❌ Failed ML model training
# MAGIC - ❌ Production pipeline failures
# MAGIC - ❌ Lost business value
# MAGIC
# MAGIC ### Data Quality Layers
# MAGIC
# MAGIC ```
# MAGIC Layer 1: Schema Enforcement (Delta Lake built-in)
# MAGIC Layer 2: Constraints (NOT NULL, CHECK)
# MAGIC Layer 3: Expectations (Great Expectations, Deequ)
# MAGIC Layer 4: Monitoring (Alerts, dashboards)
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Create Table with Data Quality Constraints
# Create table with comprehensive constraints
dq_table = "loans_data_quality"
spark.sql(f"DROP TABLE IF EXISTS {dq_table}")

spark.sql(f"""
    CREATE TABLE {dq_table} (
        loan_id BIGINT GENERATED ALWAYS AS IDENTITY,
        loan_amnt INT NOT NULL,
        funded_amnt INT NOT NULL,
        paid_amnt DOUBLE,
        addr_state STRING NOT NULL,
        annual_inc DOUBLE,
        term STRING,
        purpose STRING,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
        updated_at TIMESTAMP,
        CONSTRAINT valid_loan_amount CHECK (loan_amnt > 0 AND loan_amnt <= 50000),
        CONSTRAINT valid_funded_amount CHECK (funded_amnt >= 0 AND funded_amnt <= loan_amnt),
        CONSTRAINT valid_paid_amount CHECK (paid_amnt >= 0),
        CONSTRAINT valid_state CHECK (LENGTH(addr_state) = 2),
        CONSTRAINT valid_income CHECK (annual_inc IS NULL OR annual_inc > 0)
    )
    USING DELTA
    CLUSTER BY (addr_state, term)
""")

print(f"✅ Created table with data quality constraints: {dq_table}")

# COMMAND ----------

# DBTITLE 1,Insert Valid Data
# Insert valid data
valid_data = (
    df.select(
        "loan_amnt",
        "funded_amnt",
        "paid_amnt",
        "addr_state",
        "annual_inc",
        "term",
        "purpose",
    )
    .filter(
        (col("loan_amnt") > 0)
        & (col("loan_amnt") <= 50000)
        & (col("funded_amnt") >= 0)
        & (col("funded_amnt") <= col("loan_amnt"))
        & (col("paid_amnt") >= 0)
        & (length(col("addr_state")) == 2)
    )
    .limit(1000)
)

valid_data.write.format("delta").mode("append").saveAsTable(dq_table)

print(f"✅ Inserted valid data")
print(f"📊 Record count: {spark.table(dq_table).count():,}")

# COMMAND ----------

# DBTITLE 1,Test Constraint Violations
# Try to insert invalid data
try:
    spark.sql(f"""
        INSERT INTO {dq_table} (loan_amnt, funded_amnt, paid_amnt, addr_state, annual_inc, term, purpose)
        VALUES (100000, 10000, 0.0, 'CA', 75000.0, ' 36 months', 'debt_consolidation')
    """)
    print("❌ This should have failed!")
except Exception as e:
    print("✅ Constraint prevented invalid data!")
    print(f"   Violation: loan_amnt > 50000")

# COMMAND ----------

# DBTITLE 1,Data Quality Monitoring Query
# Create a data quality monitoring query
dq_metrics = spark.sql(f"""
    SELECT 
        COUNT(*) as total_records,
        COUNT(DISTINCT addr_state) as unique_states,
        ROUND(AVG(loan_amnt), 2) as avg_loan_amount,
        MIN(loan_amnt) as min_loan_amount,
        MAX(loan_amnt) as max_loan_amount,
        SUM(CASE WHEN paid_amnt = 0 THEN 1 ELSE 0 END) as unpaid_loans,
        SUM(CASE WHEN paid_amnt >= funded_amnt THEN 1 ELSE 0 END) as fully_paid_loans,
        ROUND(AVG(CASE WHEN paid_amnt > 0 THEN paid_amnt / funded_amnt * 100 END), 2) as avg_repayment_pct
    FROM {dq_table}
""")

display(dq_metrics)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🚀 Performance Tuning
# MAGIC
# MAGIC ### Performance Optimization Checklist
# MAGIC
# MAGIC ✅ **Table Design**:
# MAGIC - Use liquid clustering for frequently queried columns
# MAGIC - Enable Predictive Optimization
# MAGIC - Set appropriate file sizes (128MB-1GB)
# MAGIC
# MAGIC ✅ **Query Optimization**:
# MAGIC - Use predicate pushdown (filter early)
# MAGIC - Use column pruning (select only needed columns)
# MAGIC - Enable Adaptive Query Execution (AQE)
# MAGIC
# MAGIC ✅ **Compute**:
# MAGIC - Use serverless for variable workloads
# MAGIC - Use photon for SQL-heavy workloads
# MAGIC - Right-size clusters for batch jobs

# COMMAND ----------

# DBTITLE 1,Analyze Table Statistics
# Compute statistics for query optimization
spark.sql(f"ANALYZE TABLE {dq_table} COMPUTE STATISTICS")

print(f"✅ Computed statistics for {dq_table}")

# View statistics
stats = spark.sql(f"DESCRIBE EXTENDED {dq_table}")
display(
    stats.filter(col("col_name").isin(["Statistics", "Num Files", "Size in Bytes"]))
)

# COMMAND ----------

# DBTITLE 1,Query Performance Comparison
# Compare query performance with and without optimization

# Query 1: Without optimization (full table scan)
import time

start = time.time()
result1 = spark.sql(f"""
    SELECT addr_state, COUNT(*) as count
    FROM {dq_table}
    GROUP BY addr_state
""").collect()
time1 = time.time() - start

print(f"⏱️  Query time: {time1:.3f} seconds")

# COMMAND ----------

# DBTITLE 1,Enable Caching for Frequently Accessed Tables
# Cache table for faster repeated queries
spark.sql(f"CACHE TABLE {dq_table}")

print(f"✅ Cached table: {dq_table}")
print("💡 Subsequent queries will be faster")

# Run query again
start = time.time()
result2 = spark.sql(f"""
    SELECT addr_state, COUNT(*) as count
    FROM {dq_table}
    GROUP BY addr_state
""").collect()
time2 = time.time() - start

print(f"⏱️  Cached query time: {time2:.3f} seconds")
print(f"🚀 Speedup: {time1 / time2:.2f}x faster")

# COMMAND ----------

# DBTITLE 1,Uncache Table
# Uncache when no longer needed
spark.sql(f"UNCACHE TABLE {dq_table}")
print(f"✅ Uncached table: {dq_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔒 Data Governance with Unity Catalog
# MAGIC
# MAGIC ### Unity Catalog Features
# MAGIC
# MAGIC | Feature | Benefit |
# MAGIC |---------|---------|
# MAGIC | 🔐 **Access Control** | Row-level, column-level security |
# MAGIC | 🎭 **Data Masking** | Hide sensitive data (PII) |
# MAGIC | 📊 **Data Lineage** | Track data flow across pipelines |
# MAGIC | 📝 **Audit Logging** | Who accessed what, when |
# MAGIC | 🏷️ **Data Discovery** | Search and tag datasets |
# MAGIC | 🔗 **Cross-workspace** | Share data across workspaces |

# COMMAND ----------

# DBTITLE 1,Add Table Comments and Tags
# Add table-level documentation
spark.sql(f"""
    COMMENT ON TABLE {dq_table} IS 
    'Production loan data with data quality constraints. 
    Updated daily via ETL pipeline. 
    Owner: Data Engineering Team'
""")

# Add column comments
spark.sql(f"""
    ALTER TABLE {dq_table}
    ALTER COLUMN loan_amnt COMMENT 'Loan amount requested by borrower (USD)'
""")

spark.sql(f"""
    ALTER TABLE {dq_table}
    ALTER COLUMN addr_state COMMENT 'Borrower state (2-letter code)'
""")

print(f"✅ Added documentation to {dq_table}")

# COMMAND ----------

# DBTITLE 1,View Table Metadata
# View table with comments
table_desc = spark.sql(f"DESCRIBE EXTENDED {dq_table}")
display(table_desc)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Monitoring and Alerting
# MAGIC
# MAGIC ### Key Metrics to Monitor
# MAGIC
# MAGIC | Metric | What to Track | Alert Threshold |
# MAGIC |--------|---------------|-----------------|
# MAGIC | **Data Freshness** | Last update timestamp | > 24 hours |
# MAGIC | **Record Count** | Daily record count | < 90% of average |
# MAGIC | **Data Quality** | Constraint violations | > 0 |
# MAGIC | **Query Performance** | P95 query latency | > 2x baseline |
# MAGIC | **Storage Growth** | Table size growth | > 50% week-over-week |

# COMMAND ----------

# DBTITLE 1,Create Monitoring View
# Create a monitoring view for dashboards
monitoring_view = "loans_monitoring"
spark.sql(f"DROP VIEW IF EXISTS {monitoring_view}")

spark.sql(f"""
    CREATE VIEW {monitoring_view} AS
    SELECT 
        '{dq_table}' as table_name,
        COUNT(*) as record_count,
        MAX(created_at) as last_updated,
        COUNT(DISTINCT addr_state) as unique_states,
        ROUND(AVG(loan_amnt), 2) as avg_loan_amount,
        SUM(CASE WHEN paid_amnt = 0 THEN 1 ELSE 0 END) as unpaid_count,
        ROUND(SUM(CASE WHEN paid_amnt = 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as unpaid_pct,
        CURRENT_TIMESTAMP() as metrics_timestamp
    FROM {dq_table}
""")

print(f"✅ Created monitoring view: {monitoring_view}")

# COMMAND ----------

# DBTITLE 1,View Monitoring Metrics
# Query monitoring metrics
display(spark.table(monitoring_view))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📝 Summary
# MAGIC
# MAGIC ### What We Covered
# MAGIC
# MAGIC ✅ **🤖 Predictive Optimization (NEW - GA June 2024)**
# MAGIC - Enabled automatic OPTIMIZE, VACUUM, and clustering
# MAGIC - Achieved 2x faster queries and 50% storage cost reduction
# MAGIC - Zero maintenance overhead with AI-driven scheduling
# MAGIC
# MAGIC ✅ **💻 Serverless Compute (NEW - GA 2025)**
# MAGIC - Understood instant startup and pay-per-second billing
# MAGIC - Eliminated cluster configuration and management
# MAGIC - Learned how to use serverless for notebooks and jobs
# MAGIC
# MAGIC ✅ **🔄 Convert External to Managed (NEW - 2024)**
# MAGIC - Converted external tables to managed with SET MANAGED
# MAGIC - Unlocked Predictive Optimization and full Unity Catalog features
# MAGIC
# MAGIC ✅ **🧊 Unity Catalog Managed Iceberg Tables (NEW - 2024)**
# MAGIC - Created native Iceberg tables in Unity Catalog
# MAGIC - Enabled liquid clustering and Predictive Optimization on Iceberg
# MAGIC - Achieved multi-engine interoperability
# MAGIC
# MAGIC ✅ **🔗 Delta UniForm**
# MAGIC - Enabled Iceberg compatibility on Delta tables
# MAGIC - One table, multiple readers (Delta + Iceberg)
# MAGIC - No data duplication or performance penalty
# MAGIC
# MAGIC ✅ **Data Quality Framework**
# MAGIC - Implemented NOT NULL and CHECK constraints
# MAGIC - Created data quality monitoring queries
# MAGIC - Prevented bad data at write time
# MAGIC
# MAGIC ✅ **Performance Tuning**
# MAGIC - Computed table statistics for query optimization
# MAGIC - Used caching for frequently accessed tables
# MAGIC - Compared query performance with optimizations
# MAGIC
# MAGIC ✅ **Data Governance**
# MAGIC - Added table and column documentation
# MAGIC - Understood Unity Catalog features (ACLs, lineage, audit)
# MAGIC - Created monitoring views for dashboards
# MAGIC
# MAGIC ### Key Takeaways
# MAGIC
# MAGIC 💡 **Predictive Optimization** = Set it and forget it. AI handles all table maintenance automatically.
# MAGIC 💡 **Serverless Compute** = Zero infrastructure management. Instant start, pay-per-second billing.
# MAGIC 💡 **Iceberg Tables** = True lakehouse interoperability with full Databricks optimization support.
# MAGIC 💡 **Delta UniForm** = One table, multiple readers. Best of Delta and Iceberg.
# MAGIC 💡 **Data Quality** = Prevent bad data with constraints, monitor with metrics.
# MAGIC 💡 **Unity Catalog** = Centralized governance for all your data assets.
# MAGIC
# MAGIC ### Production Checklist
# MAGIC
# MAGIC Before deploying to production, ensure:
# MAGIC
# MAGIC ✅ **Table Design**:
# MAGIC - [ ] Liquid clustering enabled on frequently queried columns
# MAGIC - [ ] Predictive Optimization enabled
# MAGIC - [ ] Appropriate constraints (NOT NULL, CHECK)
# MAGIC - [ ] Table and column documentation added
# MAGIC
# MAGIC ✅ **Compute**:
# MAGIC - [ ] Serverless enabled for variable workloads
# MAGIC - [ ] Right-sized clusters for batch jobs
# MAGIC - [ ] Photon enabled for SQL-heavy workloads
# MAGIC
# MAGIC ✅ **Governance**:
# MAGIC - [ ] Tables registered in Unity Catalog
# MAGIC - [ ] Access controls configured
# MAGIC - [ ] Audit logging enabled
# MAGIC - [ ] Data lineage documented
# MAGIC
# MAGIC ✅ **Monitoring**:
# MAGIC - [ ] Data quality metrics tracked
# MAGIC - [ ] Alerts configured for anomalies
# MAGIC - [ ] Dashboards created for stakeholders
# MAGIC - [ ] Performance baselines established
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Congratulations! 🎉 You've completed the Databricks Data Engineering tutorial series!**
# MAGIC
# MAGIC You now have the skills to build production-ready data pipelines with:
# MAGIC - ⚡ Liquid Clustering for automatic optimization
# MAGIC - 🤖 Predictive Optimization for zero-maintenance tables
# MAGIC - 💻 Serverless Compute for instant scaling
# MAGIC - 🧊 Iceberg Tables for multi-engine interoperability
# MAGIC - 🔒 Unity Catalog for comprehensive governance
# MAGIC
# MAGIC **Next Steps**:
# MAGIC - 🎓 Explore [Databricks Academy](https://www.databricks.com/learn/training/home) for advanced courses
# MAGIC - 📖 Read the [Delta Lake Best Practices Guide](https://docs.databricks.com/en/delta/best-practices.html)
# MAGIC - 🏆 Get certified: [Databricks Data Engineer Associate](https://www.databricks.com/learn/certification/data-engineer-associate)
# MAGIC
# MAGIC **Happy building! 🚀**
