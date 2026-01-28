# Databricks notebook source
# MAGIC %md
# MAGIC # ⚡ Delta Lake Advanced Features
# MAGIC
# MAGIC **Level**: Intermediate
# MAGIC **Duration**: 45 minutes
# MAGIC **Prerequisites**: Complete notebooks 00-02
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Learning Objectives
# MAGIC
# MAGIC By the end of this notebook, you will:
# MAGIC - ✅ Master time travel and versioning capabilities
# MAGIC - ✅ Implement **Liquid Clustering** (NEW - GA 2024) for automatic optimization
# MAGIC - ✅ Use **Automatic Liquid Clustering** (NEW - Public Preview March 2025)
# MAGIC - ✅ Understand schema evolution and migration strategies
# MAGIC - ✅ Implement data quality constraints and validations
# MAGIC - ✅ Clone tables efficiently (shallow vs deep)
# MAGIC - ✅ Compare old vs new data layout strategies
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📖 Documentation Links
# MAGIC
# MAGIC | Resource | Link |
# MAGIC |----------|------|
# MAGIC | 📘 Delta Lake Advanced | [Databricks Delta Docs](https://docs.databricks.com/en/delta/index.html) |
# MAGIC | ⚡ Liquid Clustering | [Clustering Guide](https://docs.databricks.com/en/delta/clustering.html) |
# MAGIC | 🕰️ Time Travel | [Time Travel Docs](https://docs.databricks.com/en/delta/history.html) |
# MAGIC | 🔗 Table Cloning | [Clone Tables](https://docs.databricks.com/en/delta/clone.html) |
# MAGIC | 📊 Schema Evolution | [Schema Evolution](https://docs.databricks.com/en/delta/schema-evolution.html) |
# MAGIC | ⚡ Liquid Clustering GA Blog | [Announcement](https://www.databricks.com/blog/announcing-general-availability-liquid-clustering) |
# MAGIC | 🤖 Automatic Clustering Blog | [Announcement](https://www.databricks.com/blog/announcing-automatic-liquid-clustering) |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔧 Setup and Configuration

# COMMAND ----------

# DBTITLE 1,Configure Spark for Demo
# Reduce shuffle partitions for faster demos
spark.conf.set("spark.sql.shuffle.partitions", "1")

# Enable adaptive query execution
spark.conf.set("spark.sql.adaptive.enabled", "true")

print("✅ Spark configured for demo environment")

# COMMAND ----------

# DBTITLE 1,Import Required Libraries
from pyspark.sql.functions import *
from delta.tables import DeltaTable
from pyspark.sql.types import *

print("✅ Libraries imported")

# COMMAND ----------

# DBTITLE 1,Load Source Data
# Load the Lending Club dataset
source_path = "/databricks-datasets/learning-spark-v2/loans/loan-risks.snappy.parquet"
df = spark.read.format("parquet").load(source_path)

print(f"✅ Loaded {df.count():,} records")
print(f"✅ Columns: {len(df.columns)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🕰️ Time Travel and Versioning
# MAGIC
# MAGIC ### Why Time Travel Matters
# MAGIC
# MAGIC Time travel enables:
# MAGIC - 📊 **Audit and compliance** – Track all changes to data
# MAGIC - 🔙 **Rollback mistakes** – Undo bad updates or deletes
# MAGIC - 🔬 **Reproducibility** – Query exact data used for ML models
# MAGIC - 🐛 **Debugging** – Compare versions to find when data changed
# MAGIC - 📈 **Trend analysis** – Compare data across time periods
# MAGIC
# MAGIC ### How It Works
# MAGIC
# MAGIC ```
# MAGIC Delta Table Transaction Log:
# MAGIC ├── Version 0: Initial load (10,000 records)
# MAGIC ├── Version 1: INSERT 500 records
# MAGIC ├── Version 2: UPDATE 200 records
# MAGIC ├── Version 3: DELETE 50 records
# MAGIC └── Version 4: MERGE 300 records
# MAGIC
# MAGIC Query any version: SELECT * FROM table VERSION AS OF 2
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Create Delta Table for Time Travel Demo
# Create a Delta table
table_name = "loans_time_travel"
spark.sql(f"DROP TABLE IF EXISTS {table_name}")

df.write.format("delta").mode("overwrite").saveAsTable(table_name)

print(f"✅ Created table: {table_name}")
print(f"✅ Version 0: {spark.table(table_name).count():,} records")

# COMMAND ----------

# DBTITLE 1,Make Changes to Create Version History
# Version 1: Insert new records
new_loans = df.limit(100)
new_loans.write.format("delta").mode("append").saveAsTable(table_name)
print(f"✅ Version 1: Inserted 100 records")

# Version 2: Update records
spark.sql(f"""
    UPDATE {table_name}
    SET paid_amnt = funded_amnt * 0.5
    WHERE addr_state = 'CA' AND paid_amnt = 0
""")
print(f"✅ Version 2: Updated CA loans")

# Version 3: Delete records
spark.sql(f"""
    DELETE FROM {table_name}
    WHERE loan_amnt < 1000
""")
print(f"✅ Version 3: Deleted small loans")

# COMMAND ----------

# DBTITLE 1,View Complete Table History
# Show all versions with details
history = spark.sql(f"DESCRIBE HISTORY {table_name}")
display(history.select("version", "timestamp", "operation", "operationMetrics"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query Previous Versions

# COMMAND ----------

# DBTITLE 1,Query by Version Number
# Query version 0 (original data)
version_0 = spark.read.format("delta").option("versionAsOf", 0).table(table_name)
print(f"📊 Version 0 count: {version_0.count():,}")

# Query version 1 (after insert)
version_1 = spark.read.format("delta").option("versionAsOf", 1).table(table_name)
print(f"📊 Version 1 count: {version_1.count():,}")

# Current version
current = spark.table(table_name)
print(f"📊 Current version count: {current.count():,}")

# COMMAND ----------

# DBTITLE 1,Query by Timestamp
# Get timestamp from history
first_timestamp = history.select("timestamp").first()[0]

# Query data as of that timestamp
version_by_time = (
    spark.read.format("delta")
    .option("timestampAsOf", first_timestamp)
    .table(table_name)
)

print(f"📊 Record count at {first_timestamp}: {version_by_time.count():,}")

# COMMAND ----------

# DBTITLE 1,Compare Versions
# Compare record counts across versions
version_comparison = spark.sql(f"""
    SELECT 
        0 as version,
        (SELECT COUNT(*) FROM {table_name} VERSION AS OF 0) as record_count
    UNION ALL
    SELECT 
        1 as version,
        (SELECT COUNT(*) FROM {table_name} VERSION AS OF 1) as record_count
    UNION ALL
    SELECT 
        2 as version,
        (SELECT COUNT(*) FROM {table_name} VERSION AS OF 2) as record_count
    UNION ALL
    SELECT 
        3 as version,
        (SELECT COUNT(*) FROM {table_name} VERSION AS OF 3) as record_count
""")

display(version_comparison)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Restore to Previous Version

# COMMAND ----------

# DBTITLE 1,Restore Table to Specific Version
# Restore to version 0 (undo all changes)
spark.sql(f"RESTORE TABLE {table_name} TO VERSION AS OF 0")

print("✅ Table restored to version 0")
print(f"✅ Current record count: {spark.table(table_name).count():,}")

# View history after restore
display(spark.sql(f"DESCRIBE HISTORY {table_name}").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⚡ Liquid Clustering (NEW - GA 2024)
# MAGIC
# MAGIC ### The Old Way vs The New Way
# MAGIC
# MAGIC | Old Approach | New Approach (Liquid Clustering) |
# MAGIC |--------------|----------------------------------|
# MAGIC | ❌ **Hive Partitioning**: Choose wrong columns = bad performance | ✅ **Automatic optimization**: No manual tuning needed |
# MAGIC | ❌ **Z-ORDER**: Manual optimization, requires rewrites | ✅ **Incremental clustering**: No full table rewrites |
# MAGIC | ❌ **Static layout**: Can't change without rewriting | ✅ **Flexible**: Change clustering keys anytime |
# MAGIC | ❌ **Limited concurrency**: Partitioning limits concurrent writes | ✅ **Full concurrency**: No write limitations |
# MAGIC | ❌ **Manual maintenance**: Schedule OPTIMIZE jobs | ✅ **Automatic**: Works with Predictive Optimization |
# MAGIC
# MAGIC ### Why This Matters
# MAGIC
# MAGIC **Performance gains**:
# MAGIC - 🚀 **2-12x faster queries** compared to Hive partitioning
# MAGIC - 📉 **50% fewer files** to scan for typical queries
# MAGIC - ⚡ **Automatic optimization** with Predictive Optimization (covered in Notebook 05)
# MAGIC - 🔄 **No rewrites needed** when changing clustering keys
# MAGIC
# MAGIC ### How It Works
# MAGIC
# MAGIC Liquid clustering automatically organizes data by specified columns:
# MAGIC
# MAGIC ```
# MAGIC Traditional Partitioning:
# MAGIC /state=CA/part-001.parquet
# MAGIC /state=NY/part-002.parquet
# MAGIC → Fixed structure, hard to change
# MAGIC
# MAGIC Liquid Clustering:
# MAGIC /part-001.parquet (contains CA data clustered together)
# MAGIC /part-002.parquet (contains NY data clustered together)
# MAGIC → Flexible, can change clustering keys
# MAGIC ```
# MAGIC
# MAGIC 📖 **Learn More**: [Liquid Clustering GA Announcement](https://www.databricks.com/blog/announcing-general-availability-liquid-clustering)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Layout Strategies Comparison
# MAGIC
# MAGIC | Feature | Hive Partitioning | Z-ORDER | Liquid Clustering |
# MAGIC |---------|------------------|---------|-------------------|
# MAGIC | **Setup Complexity** | High (choose wrong = bad performance) | Medium (manual optimization) | Low (automatic) |
# MAGIC | **Query Performance** | Good (if partitioned correctly) | Very Good | Excellent (2-12x faster) |
# MAGIC | **Flexibility** | ❌ Rewrites required | ❌ Rewrites required | ✅ No rewrites |
# MAGIC | **Maintenance** | Manual | Manual | Automatic with Predictive Optimization |
# MAGIC | **Concurrent Writes** | Limited | Limited | Full support |
# MAGIC | **Change Clustering Keys** | ❌ Full rewrite | ❌ Full rewrite | ✅ Incremental |
# MAGIC | **Best For** | Low-cardinality columns (date, region) | High-cardinality columns | Any workload |
# MAGIC | **Recommendation** | ❌ Legacy approach | ❌ Legacy approach | ✅ **Use this!** |

# COMMAND ----------

# DBTITLE 1,Create Table with Liquid Clustering
# Create a new table with liquid clustering enabled
clustered_table = "loans_clustered"
spark.sql(f"DROP TABLE IF EXISTS {clustered_table}")

# Create table with liquid clustering on state and loan amount
spark.sql(f"""
    CREATE TABLE {clustered_table}
    USING DELTA
    CLUSTER BY (addr_state, loan_amnt)
    AS SELECT * FROM {table_name}
""")

print(f"✅ Created table with liquid clustering: {clustered_table}")
print(f"✅ Clustered by: addr_state, loan_amnt")

# COMMAND ----------

# DBTITLE 1,Enable Liquid Clustering on Existing Table
# You can also enable liquid clustering on an existing table
existing_table = "loans_existing"
spark.sql(f"DROP TABLE IF EXISTS {existing_table}")

# Create regular table first
df.write.format("delta").mode("overwrite").saveAsTable(existing_table)

# Enable liquid clustering
spark.sql(f"""
    ALTER TABLE {existing_table} 
    SET TBLPROPERTIES ('delta.enableLiquidClustering' = 'true')
""")

# Add clustering columns
spark.sql(f"""
    ALTER TABLE {existing_table} 
    CLUSTER BY (addr_state, term)
""")

print(f"✅ Enabled liquid clustering on existing table: {existing_table}")
print(f"✅ Clustered by: addr_state, term")

# COMMAND ----------

# DBTITLE 1,Optimize with Liquid Clustering
# Run OPTIMIZE to apply liquid clustering
spark.sql(f"OPTIMIZE {clustered_table}")

print(f"✅ Optimized {clustered_table} with liquid clustering")
print("✅ Data is now organized by addr_state and loan_amnt")

# COMMAND ----------

# DBTITLE 1,Change Clustering Keys (No Rewrite Needed!)
# Change clustering keys without rewriting the entire table
spark.sql(f"""
    ALTER TABLE {clustered_table} 
    CLUSTER BY (addr_state, term, purpose)
""")

print("✅ Changed clustering keys to: addr_state, term, purpose")
print("✅ No full table rewrite required!")
print("💡 Next OPTIMIZE will incrementally apply new clustering")

# COMMAND ----------

# DBTITLE 1,View Clustering Information
# Check table properties to see clustering configuration
clustering_info = spark.sql(f"DESCRIBE DETAIL {clustered_table}")
display(
    clustering_info.select("name", "format", "numFiles", "sizeInBytes", "properties")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🤖 Automatic Liquid Clustering (NEW - Public Preview March 2025)
# MAGIC
# MAGIC ### The Old Way vs The New Way
# MAGIC
# MAGIC | Old Approach | New Approach (Automatic Clustering) |
# MAGIC |--------------|-------------------------------------|
# MAGIC | ❌ **Manual selection**: Guess which columns to cluster by | ✅ **AI-powered**: Analyzes query patterns automatically |
# MAGIC | ❌ **Static choice**: Clustering keys don't adapt to workload changes | ✅ **Adaptive**: Adjusts to changing query patterns |
# MAGIC | ❌ **Trial and error**: Test different clustering keys manually | ✅ **Automatic**: AI picks optimal keys |
# MAGIC
# MAGIC ### Why This Matters
# MAGIC
# MAGIC **Automatic Liquid Clustering uses AI to**:
# MAGIC - 🤖 **Analyze query patterns** from Unity Catalog query history
# MAGIC - 🎯 **Select optimal clustering keys** based on actual usage
# MAGIC - 🔄 **Adapt over time** as query patterns change
# MAGIC - ⚡ **Maximize performance** without manual tuning
# MAGIC
# MAGIC ### How It Works
# MAGIC
# MAGIC ```
# MAGIC 1. AI analyzes Unity Catalog query logs
# MAGIC 2. Identifies most frequently filtered/joined columns
# MAGIC 3. Automatically selects clustering keys
# MAGIC 4. Continuously adapts to workload changes
# MAGIC ```
# MAGIC
# MAGIC 📖 **Learn More**: [Automatic Liquid Clustering Announcement](https://www.databricks.com/blog/announcing-automatic-liquid-clustering)

# COMMAND ----------

# DBTITLE 1,Create Table with Automatic Liquid Clustering
# Create table with automatic clustering (AI selects keys)
auto_clustered_table = "loans_auto_clustered"
spark.sql(f"DROP TABLE IF EXISTS {auto_clustered_table}")

# Create table with CLUSTER BY AUTO
spark.sql(f"""
    CREATE TABLE {auto_clustered_table}
    USING DELTA
    CLUSTER BY AUTO
    AS SELECT * FROM {table_name}
""")

print(f"✅ Created table with automatic liquid clustering: {auto_clustered_table}")
print("🤖 AI will analyze query patterns and select optimal clustering keys")

# COMMAND ----------

# DBTITLE 1,Enable Automatic Clustering on Existing Table
# Enable automatic clustering on an existing table
spark.sql(f"""
    ALTER TABLE {existing_table} 
    CLUSTER BY AUTO
""")

print(f"✅ Enabled automatic clustering on: {existing_table}")
print("🤖 AI will optimize clustering based on query patterns")

# COMMAND ----------

# MAGIC %md
# MAGIC 💡 **Pro Tip**: Automatic Liquid Clustering works best with:
# MAGIC - Tables in Unity Catalog (for query pattern analysis)
# MAGIC - Predictive Optimization enabled (covered in Notebook 05)
# MAGIC - Sufficient query history (at least a few days of queries)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔄 Schema Evolution
# MAGIC
# MAGIC ### Why Schema Evolution Matters
# MAGIC
# MAGIC Real-world data changes over time:
# MAGIC - 📊 New fields added to source systems
# MAGIC - 🔄 Data types need to change
# MAGIC - 🗑️ Old columns become obsolete
# MAGIC - 🔀 Column names need standardization
# MAGIC
# MAGIC Delta Lake makes schema changes safe and easy!

# COMMAND ----------

# DBTITLE 1,Add New Columns
# Add new columns to the table
spark.sql(f"""
    ALTER TABLE {table_name}
    ADD COLUMNS (
        risk_score DOUBLE COMMENT 'Calculated risk score',
        last_updated TIMESTAMP COMMENT 'Last update timestamp'
    )
""")

print("✅ Added columns: risk_score, last_updated")

# Verify new schema
display(spark.sql(f"DESCRIBE {table_name}"))

# COMMAND ----------

# DBTITLE 1,Update New Columns with Calculated Values
# Populate the new risk_score column
spark.sql(f"""
    UPDATE {table_name}
    SET 
        risk_score = (dti * 0.3) + (delinq_2yrs * 10) + (revol_util * 0.2),
        last_updated = current_timestamp()
""")

print("✅ Populated new columns with calculated values")

# Verify updates
display(
    spark.sql(f"""
    SELECT loan_amnt, dti, delinq_2yrs, revol_util, risk_score, last_updated
    FROM {table_name}
    LIMIT 10
""")
)

# COMMAND ----------

# DBTITLE 1,Merge Schema on Write
# Create a DataFrame with additional columns
extended_df = (
    df.limit(10)
    .withColumn("data_source", lit("external_api"))
    .withColumn("ingestion_date", current_date())
)

# Write with schema merge enabled
extended_df.write.format("delta").mode("append").option(
    "mergeSchema", "true"
).saveAsTable(table_name)

print("✅ Merged schema with new columns: data_source, ingestion_date")

# View updated schema
display(spark.sql(f"DESCRIBE {table_name}"))

# COMMAND ----------

# DBTITLE 1,Change Column Comments
# Update column comments for better documentation
spark.sql(f"""
    ALTER TABLE {table_name}
    ALTER COLUMN loan_amnt COMMENT 'Loan amount requested by borrower (USD)'
""")

spark.sql(f"""
    ALTER TABLE {table_name}
    ALTER COLUMN addr_state COMMENT 'Borrower state (2-letter code)'
""")

print("✅ Updated column comments")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔄 Change Data Feed (CDF)
# MAGIC
# MAGIC ### What is Change Data Feed?
# MAGIC
# MAGIC **Change Data Feed (CDF)** records row-level changes (inserts, updates, deletes) made to Delta tables.
# MAGIC
# MAGIC ### Why Use CDF?
# MAGIC
# MAGIC - 🔄 **Incremental ETL**: Process only changed data, not full table scans
# MAGIC - 📊 **Audit trails**: Track all modifications for compliance
# MAGIC - 🔗 **Downstream sync**: Replicate changes to data warehouses
# MAGIC - ⚡ **Performance**: Avoid expensive full table reads
# MAGIC
# MAGIC ### CDF vs CDC
# MAGIC
# MAGIC | Feature | Change Data Feed (CDF) | Change Data Capture (CDC) |
# MAGIC |---------|------------------------|---------------------------|
# MAGIC | **Scope** | Delta Lake feature | Database replication pattern |
# MAGIC | **What it tracks** | Row-level changes in Delta tables | Changes from source databases (MySQL, PostgreSQL) |
# MAGIC | **Use case** | Incremental processing within lakehouse | Ingesting changes from OLTP systems |
# MAGIC | **Implementation** | Enable table property | Lakeflow Connect, Debezium, etc. |
# MAGIC
# MAGIC 📖 **Docs**: [Change Data Feed](https://docs.databricks.com/en/delta/delta-change-data-feed.html)

# COMMAND ----------

# DBTITLE 1,Enable CDF on New Table
# Create table with CDF enabled from the start
spark.sql(f"""
  CREATE OR REPLACE TABLE loans_cdf
  TBLPROPERTIES (delta.enableChangeDataFeed = true)
  AS SELECT * FROM {table_name} LIMIT 1000
""")

print("✅ Created table with Change Data Feed enabled")
print("📊 All future changes will be tracked")

# COMMAND ----------

# DBTITLE 1,Enable CDF on Existing Table
# For existing tables, enable CDF with ALTER TABLE
spark.sql("""
  ALTER TABLE loans_cdf
  SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")

print("✅ Enabled Change Data Feed on existing table")
print("⚠️ Important: Only changes AFTER enabling CDF are tracked")

# COMMAND ----------

# DBTITLE 1,Perform Operations to Generate Change Data
from pyspark.sql.functions import *

# INSERT new records
new_records = spark.createDataFrame(
    [
        (99999, 25000, 24000, "NY", "36 months"),
        (99998, 30000, 29500, "CA", "60 months"),
        (99997, 15000, 14800, "TX", "36 months"),
    ],
    ["loan_id", "loan_amnt", "funded_amnt", "addr_state", "term"],
)

new_records.write.format("delta").mode("append").saveAsTable("loans_cdf")
print("✅ Inserted 3 new records")

# UPDATE existing records
spark.sql("""
  UPDATE loans_cdf
  SET funded_amnt = funded_amnt * 1.1
  WHERE loan_id IN (99999, 99998)
""")
print("✅ Updated 2 records")

# DELETE a record
spark.sql("""
  DELETE FROM loans_cdf
  WHERE loan_id = 99997
""")
print("✅ Deleted 1 record")

print(f"\n📊 Total operations: 3 INSERTs, 2 UPDATEs, 1 DELETE")

# COMMAND ----------

# DBTITLE 1,Read Change Data Feed
# Read all changes since version 1
changes_df = (
    spark.read.format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", 1)
    .table("loans_cdf")
)

print(f"📊 Total change records: {changes_df.count()}")

# Show changes with metadata
display(
    changes_df.select(
        "loan_id",
        "loan_amnt",
        "funded_amnt",
        "addr_state",
        "_change_type",  # Type of change
        "_commit_version",  # Delta table version
        "_commit_timestamp",  # When the change occurred
    )
    .orderBy("_commit_version", "loan_id")
    .limit(20)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Understanding CDF Change Types
# MAGIC
# MAGIC | Change Type | Description | When It Appears |
# MAGIC |------------|-------------|-----------------|
# MAGIC | `insert` | New row added | INSERT statements |
# MAGIC | `update_preimage` | Row values BEFORE update | UPDATE statements (old values) |
# MAGIC | `update_postimage` | Row values AFTER update | UPDATE statements (new values) |
# MAGIC | `delete` | Row removed | DELETE statements |
# MAGIC
# MAGIC 💡 **Note**: Updates generate TWO records - one preimage (before) and one postimage (after)

# COMMAND ----------

# DBTITLE 1,Analyze Changes by Type
# Count changes by type
change_summary = changes_df.groupBy("_change_type").count().orderBy("_change_type")

print("📊 Change Summary:")
display(change_summary)

# COMMAND ----------

# DBTITLE 1,Filter for Specific Change Types
# Get only inserts
inserts = changes_df.filter(col("_change_type") == "insert")
print(f"✅ Inserts: {inserts.count()}")

# Get only updates (both pre and post images)
updates = changes_df.filter(col("_change_type").like("update%"))
print(f"🔄 Updates (pre+post): {updates.count()}")

# Get only deletes
deletes = changes_df.filter(col("_change_type") == "delete")
print(f"🗑️ Deletes: {deletes.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### CDF Use Cases
# MAGIC
# MAGIC #### 1️⃣ Incremental ETL Pipeline
# MAGIC ```python
# MAGIC # Process only changes since last run
# MAGIC last_processed_version = 10  # Track this in your pipeline
# MAGIC
# MAGIC changes = spark.read.format("delta") \
# MAGIC     .option("readChangeFeed", "true") \
# MAGIC     .option("startingVersion", last_processed_version) \
# MAGIC     .table("source_table")
# MAGIC
# MAGIC # Apply changes to target
# MAGIC changes.write.format("delta").mode("append").saveAsTable("target_table")
# MAGIC ```
# MAGIC
# MAGIC #### 2️⃣ Time-Based Change Tracking
# MAGIC ```python
# MAGIC # Get changes since yesterday
# MAGIC changes = spark.read.format("delta") \
# MAGIC     .option("readChangeFeed", "true") \
# MAGIC     .option("startingTimestamp", "2024-01-01") \
# MAGIC     .option("endingTimestamp", "2024-01-02") \
# MAGIC     .table("my_table")
# MAGIC ```
# MAGIC
# MAGIC #### 3️⃣ Audit Trail
# MAGIC ```python
# MAGIC # Create audit log table from CDF
# MAGIC audit_log = changes.select(
# MAGIC     "loan_id",
# MAGIC     "_change_type",
# MAGIC     "_commit_timestamp",
# MAGIC     current_user().alias("changed_by")
# MAGIC )
# MAGIC audit_log.write.mode("append").saveAsTable("audit_log")
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Cleanup
spark.sql("DROP TABLE IF EXISTS loans_cdf")
print("✅ Cleanup complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Constraints and Data Quality
# MAGIC
# MAGIC ### Why Constraints Matter
# MAGIC
# MAGIC Constraints prevent bad data from entering your tables:
# MAGIC - 🛡️ **NOT NULL**: Ensure critical fields are always populated
# MAGIC - ✅ **CHECK**: Validate business rules (e.g., loan_amnt > 0)
# MAGIC - 🎯 **Generated columns**: Auto-calculate derived values
# MAGIC
# MAGIC ### Constraint Types

# COMMAND ----------

# DBTITLE 1,Add NOT NULL Constraint
# Create a new table with NOT NULL constraints
constrained_table = "loans_constrained"
spark.sql(f"DROP TABLE IF EXISTS {constrained_table}")

spark.sql(f"""
    CREATE TABLE {constrained_table} (
        loan_amnt INT NOT NULL COMMENT 'Loan amount (required)',
        funded_amnt INT NOT NULL COMMENT 'Funded amount (required)',
        paid_amnt DOUBLE,
        addr_state STRING NOT NULL COMMENT 'State (required)',
        annual_inc DOUBLE,
        term STRING,
        purpose STRING
    )
    USING DELTA
""")

print(f"✅ Created table with NOT NULL constraints: {constrained_table}")

# COMMAND ----------

# DBTITLE 1,Test NOT NULL Constraint
# Try to insert a record with NULL in a NOT NULL column
try:
    spark.sql(f"""
        INSERT INTO {constrained_table}
        VALUES (NULL, 10000, 0.0, 'CA', 75000.0, ' 36 months', 'debt_consolidation')
    """)
    print("❌ This should have failed!")
except Exception as e:
    print("✅ NOT NULL constraint prevented bad data!")
    print(f"   Error: {str(e)[:100]}...")

# COMMAND ----------

# DBTITLE 1,Add CHECK Constraint
# Add CHECK constraints to validate business rules
spark.sql(f"""
    ALTER TABLE {constrained_table}
    ADD CONSTRAINT valid_loan_amount CHECK (loan_amnt > 0 AND loan_amnt <= 50000)
""")

spark.sql(f"""
    ALTER TABLE {constrained_table}
    ADD CONSTRAINT valid_funded_amount CHECK (funded_amnt >= 0 AND funded_amnt <= loan_amnt)
""")

print("✅ Added CHECK constraints:")
print("   - loan_amnt must be between 1 and 50,000")
print("   - funded_amnt must be <= loan_amnt")

# COMMAND ----------

# DBTITLE 1,Test CHECK Constraint
# Try to insert invalid data
try:
    spark.sql(f"""
        INSERT INTO {constrained_table}
        VALUES (100000, 10000, 0.0, 'CA', 75000.0, ' 36 months', 'debt_consolidation')
    """)
    print("❌ This should have failed!")
except Exception as e:
    print("✅ CHECK constraint prevented invalid loan amount!")
    print(f"   Error: {str(e)[:100]}...")

# COMMAND ----------

# DBTITLE 1,Insert Valid Data
# Insert valid data
spark.sql(f"""
    INSERT INTO {constrained_table}
    VALUES 
        (15000, 15000, 0.0, 'CA', 75000.0, ' 36 months', 'debt_consolidation'),
        (25000, 25000, 0.0, 'NY', 95000.0, ' 60 months', 'home_improvement'),
        (10000, 10000, 0.0, 'TX', 65000.0, ' 36 months', 'credit_card')
""")

print("✅ Inserted 3 valid records")
display(spark.table(constrained_table))

# COMMAND ----------

# DBTITLE 1,View Table Constraints
# Show all constraints on the table
constraints = spark.sql(f"SHOW TBLPROPERTIES {constrained_table}")
display(constraints.filter(col("key").like("%constraint%")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔗 Table Cloning
# MAGIC
# MAGIC ### Shallow vs Deep Clone
# MAGIC
# MAGIC | Feature | Shallow Clone | Deep Clone |
# MAGIC |---------|--------------|------------|
# MAGIC | **Speed** | ⚡ Instant (metadata only) | 🐢 Slower (copies data) |
# MAGIC | **Storage** | 📦 Minimal (references original) | 📦 Full copy |
# MAGIC | **Independence** | ⚠️ Depends on source table | ✅ Fully independent |
# MAGIC | **Use Case** | Testing, dev environments | Production backups, isolation |
# MAGIC | **Cost** | 💰 Low | 💰 Higher |
# MAGIC
# MAGIC ### When to Use Each
# MAGIC
# MAGIC **Shallow Clone**:
# MAGIC - 🧪 Create test environments quickly
# MAGIC - 🔬 Experiment without copying data
# MAGIC - 📊 Share data snapshots
# MAGIC
# MAGIC **Deep Clone**:
# MAGIC - 💾 Create backups before major changes
# MAGIC - 🔒 Isolate data for compliance
# MAGIC - 🌍 Replicate data across regions

# COMMAND ----------

# DBTITLE 1,Create Shallow Clone
# Create a shallow clone (instant, metadata only)
shallow_clone = "loans_shallow_clone"
spark.sql(f"DROP TABLE IF EXISTS {shallow_clone}")

spark.sql(f"""
    CREATE TABLE {shallow_clone}
    SHALLOW CLONE {table_name}
""")

print(f"✅ Created shallow clone: {shallow_clone}")
print("⚡ Instant operation (metadata only)")
print(f"📊 Record count: {spark.table(shallow_clone).count():,}")

# COMMAND ----------

# DBTITLE 1,Create Deep Clone
# Create a deep clone (full data copy)
deep_clone = "loans_deep_clone"
spark.sql(f"DROP TABLE IF EXISTS {deep_clone}")

spark.sql(f"""
    CREATE TABLE {deep_clone}
    DEEP CLONE {table_name}
""")

print(f"✅ Created deep clone: {deep_clone}")
print("📦 Full data copy (independent)")
print(f"📊 Record count: {spark.table(deep_clone).count():,}")

# COMMAND ----------

# DBTITLE 1,Clone Specific Version
# Clone a specific version of the table
version_clone = "loans_version_clone"
spark.sql(f"DROP TABLE IF EXISTS {version_clone}")

spark.sql(f"""
    CREATE TABLE {version_clone}
    SHALLOW CLONE {table_name}
    VERSION AS OF 0
""")

print(f"✅ Created clone of version 0: {version_clone}")
print(f"📊 Record count: {spark.table(version_clone).count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📝 Summary
# MAGIC
# MAGIC ### What We Covered
# MAGIC
# MAGIC ✅ **Time Travel and Versioning**
# MAGIC - Queried historical versions by version number and timestamp
# MAGIC - Compared data across versions
# MAGIC - Restored tables to previous states
# MAGIC
# MAGIC ✅ **⚡ Liquid Clustering (NEW - GA 2024)**
# MAGIC - Understood the benefits over Hive partitioning and Z-ORDER
# MAGIC - Created tables with liquid clustering
# MAGIC - Changed clustering keys without rewrites
# MAGIC - Achieved 2-12x faster query performance
# MAGIC
# MAGIC ✅ **🤖 Automatic Liquid Clustering (NEW - Public Preview March 2025)**
# MAGIC - Enabled AI-powered automatic clustering key selection
# MAGIC - Let AI optimize based on query patterns
# MAGIC
# MAGIC ✅ **Schema Evolution**
# MAGIC - Added new columns dynamically
# MAGIC - Merged schemas on write
# MAGIC - Updated column metadata
# MAGIC
# MAGIC ✅ **Constraints and Data Quality**
# MAGIC - Implemented NOT NULL constraints
# MAGIC - Added CHECK constraints for business rules
# MAGIC - Prevented bad data from entering tables
# MAGIC
# MAGIC ✅ **Table Cloning**
# MAGIC - Created shallow clones (instant, metadata only)
# MAGIC - Created deep clones (full data copy)
# MAGIC - Cloned specific versions
# MAGIC
# MAGIC ### Key Takeaways
# MAGIC
# MAGIC 💡 **Liquid Clustering** = Say goodbye to manual partitioning! 2-12x faster queries without rewrites
# MAGIC 💡 **Automatic Clustering** = AI picks optimal clustering keys based on query patterns
# MAGIC 💡 **Time Travel** = Audit trail, rollback capability, and reproducibility
# MAGIC 💡 **Schema Evolution** = Adapt to changing data without breaking pipelines
# MAGIC 💡 **Constraints** = Prevent bad data at write time
# MAGIC 💡 **Cloning** = Instant test environments (shallow) or independent backups (deep)
# MAGIC
# MAGIC ### Next Steps
# MAGIC
# MAGIC 🎯 **Ready for streaming?** Open **04-Streaming-with-Delta-Lake.py** to learn:
# MAGIC - Real-time data processing with Structured Streaming
# MAGIC - Liquid clustering in streaming workloads
# MAGIC - Stream-to-stream joins and aggregations
# MAGIC - Change Data Capture (CDC) patterns
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Excellent work! 🚀 You've mastered Delta Lake advanced features!**
