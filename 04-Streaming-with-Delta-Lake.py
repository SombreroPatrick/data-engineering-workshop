# Databricks notebook source
# MAGIC %md
# MAGIC # 🌊 Streaming with Delta Lake
# MAGIC
# MAGIC **Level**: Intermediate to Advanced
# MAGIC **Duration**: 50 minutes
# MAGIC **Prerequisites**: Complete notebooks 00-03
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Learning Objectives
# MAGIC
# MAGIC By the end of this notebook, you will:
# MAGIC - ✅ Understand Structured Streaming fundamentals
# MAGIC - ✅ Read and write streaming data with Delta Lake
# MAGIC - ✅ Implement **Liquid Clustering in streaming** (NEW - 2024)
# MAGIC - ✅ Build real-time aggregations with windowing
# MAGIC - ✅ Handle late-arriving data with watermarking
# MAGIC - ✅ Perform stream-to-stream joins
# MAGIC - ✅ Implement Change Data Capture (CDC) patterns
# MAGIC - ✅ Monitor and manage streaming queries
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📖 Documentation Links
# MAGIC
# MAGIC | Resource | Link |
# MAGIC |----------|------|
# MAGIC | 📘 Structured Streaming | [Databricks Streaming Guide](https://docs.databricks.com/en/structured-streaming/) |
# MAGIC | 🌊 Delta Streaming | [Delta Lake Streaming](https://docs.databricks.com/en/structured-streaming/delta-lake.html) |
# MAGIC | ⚡ Liquid Clustering Streaming | [Clustering in Streaming](https://docs.databricks.com/en/delta/clustering.html#streaming) |
# MAGIC | 🔄 Auto Loader | [Auto Loader Guide](https://docs.databricks.com/en/ingestion/auto-loader/) |
# MAGIC | 💧 Watermarking | [Handling Late Data](https://docs.databricks.com/en/structured-streaming/watermarking.html) |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔧 Setup and Configuration

# COMMAND ----------

# DBTITLE 1,Configure Spark for Streaming
# Reduce shuffle partitions for faster demos
spark.conf.set("spark.sql.shuffle.partitions", "1")

# Enable adaptive query execution
spark.conf.set("spark.sql.adaptive.enabled", "true")

print("✅ Spark configured for streaming demo")

# COMMAND ----------

# DBTITLE 1,Import Required Libraries
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable
import time

print("✅ Libraries imported")

# COMMAND ----------

# DBTITLE 1,Setup Directories
# Define paths for streaming demo
base_path = "/tmp/streaming_demo"
source_path = f"{base_path}/source"
checkpoint_path = f"{base_path}/checkpoints"
output_path = f"{base_path}/output"

# Clean up existing data
dbutils.fs.rm(base_path, recurse=True)

# Create directories
dbutils.fs.mkdirs(source_path)
dbutils.fs.mkdirs(checkpoint_path)

print(f"✅ Created directories:")
print(f"   Source: {source_path}")
print(f"   Checkpoints: {checkpoint_path}")
print(f"   Output: {output_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🌊 Structured Streaming Fundamentals
# MAGIC
# MAGIC ### What is Structured Streaming?
# MAGIC
# MAGIC **Structured Streaming** treats real-time data as an unbounded table:
# MAGIC
# MAGIC ```
# MAGIC Batch Processing:
# MAGIC ┌─────────────┐
# MAGIC │ Static Table│ → Query → Result
# MAGIC └─────────────┘
# MAGIC
# MAGIC Stream Processing:
# MAGIC ┌─────────────┐
# MAGIC │ Unbounded   │ → Continuous Query → Continuous Results
# MAGIC │ Table       │    (always running)
# MAGIC │ (growing)   │
# MAGIC └─────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### Key Concepts
# MAGIC
# MAGIC | Concept | Description |
# MAGIC |---------|-------------|
# MAGIC | 📥 **Source** | Where data comes from (files, Kafka, Delta) |
# MAGIC | 🔄 **Transformation** | Same DataFrame API as batch |
# MAGIC | 📤 **Sink** | Where results go (Delta, console, memory) |
# MAGIC | ✅ **Checkpoint** | Fault tolerance and exactly-once processing |
# MAGIC | 💧 **Watermark** | Handle late-arriving data |
# MAGIC | 🪟 **Window** | Time-based aggregations |
# MAGIC
# MAGIC ### Why Delta Lake for Streaming?
# MAGIC
# MAGIC ✅ **ACID transactions** – No partial writes or data corruption
# MAGIC ✅ **Exactly-once processing** – With checkpoints
# MAGIC ✅ **Schema enforcement** – Reject bad streaming data
# MAGIC ✅ **Time travel** – Query historical streaming data
# MAGIC ✅ **Upserts** – Handle CDC and late updates

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📥 Creating a Streaming Source
# MAGIC
# MAGIC Let's simulate a stream of loan applications:

# COMMAND ----------

# DBTITLE 1,Load Source Data
# Load the Lending Club dataset
source_data_path = (
    "/databricks-datasets/learning-spark-v2/loans/loan-risks.snappy.parquet"
)
df = spark.read.format("parquet").load(source_data_path)

print(f"✅ Loaded {df.count():,} records for streaming simulation")

# COMMAND ----------

# DBTITLE 1,Write Data as JSON Files (Simulating Streaming Source)
# Split data into batches and write as JSON files
batch_size = 1000
total_records = df.count()
num_batches = 5

for i in range(num_batches):
    batch_df = (
        df.limit(batch_size)
        .withColumn("batch_id", lit(i))
        .withColumn("event_time", current_timestamp())
    )

    batch_path = f"{source_path}/batch_{i}"
    batch_df.write.format("json").mode("overwrite").save(batch_path)

    print(f"✅ Wrote batch {i} to {batch_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔄 Reading Streaming Data

# COMMAND ----------

# DBTITLE 1,Define Schema for Streaming Source
# Define explicit schema (best practice for streaming)
loan_schema = StructType(
    [
        StructField("loan_amnt", IntegerType(), True),
        StructField("funded_amnt", IntegerType(), True),
        StructField("paid_amnt", DoubleType(), True),
        StructField("addr_state", StringType(), True),
        StructField("closed", StringType(), True),
        StructField("annual_inc", DoubleType(), True),
        StructField("emp_length", DoubleType(), True),
        StructField("dti", DoubleType(), True),
        StructField("delinq_2yrs", DoubleType(), True),
        StructField("revol_util", DoubleType(), True),
        StructField("total_acc", DoubleType(), True),
        StructField("credit_length_in_years", DoubleType(), True),
        StructField("term", StringType(), True),
        StructField("home_ownership", StringType(), True),
        StructField("purpose", StringType(), True),
        StructField("verification_status", StringType(), True),
        StructField("application_type", StringType(), True),
        StructField("batch_id", IntegerType(), True),
        StructField("event_time", TimestampType(), True),
    ]
)

print("✅ Schema defined for streaming source")

# COMMAND ----------

# DBTITLE 1,Create Streaming DataFrame
# Read streaming data from JSON files
streaming_df = (
    spark.readStream.format("json")
    .schema(loan_schema)
    .option("maxFilesPerTrigger", 1)  # Process one file at a time
    .load(source_path)
)

print("✅ Created streaming DataFrame")
print(f"   Is streaming: {streaming_df.isStreaming}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📤 Writing Streaming Data to Delta Lake

# COMMAND ----------

# DBTITLE 1,Write Stream to Delta Table (Append Mode)
# Define output table
output_table = "loans_streaming"
spark.sql(f"DROP TABLE IF EXISTS {output_table}")

# Write streaming data to Delta table
query = (
    streaming_df.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{checkpoint_path}/append")
    .trigger(processingTime="5 seconds")
    .toTable(output_table)
)

print(f"✅ Started streaming query: {query.name}")
print(f"   Output table: {output_table}")
print(f"   Checkpoint: {checkpoint_path}/append")

# COMMAND ----------

# DBTITLE 1,Monitor Streaming Query
# Wait for some data to be processed
time.sleep(10)

# Check query status
print(f"📊 Query Status: {query.status}")
print(f"📊 Is Active: {query.isActive}")

# Check record count
record_count = spark.table(output_table).count()
print(f"📊 Records in table: {record_count:,}")

# COMMAND ----------

# DBTITLE 1,View Streaming Data
# Display the streaming data
display(spark.table(output_table).limit(20))

# COMMAND ----------

# DBTITLE 1,Stop Streaming Query
# Stop the query
query.stop()
print("✅ Streaming query stopped")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⚡ Liquid Clustering in Streaming (NEW - 2024)
# MAGIC
# MAGIC ### Why Liquid Clustering for Streaming?
# MAGIC
# MAGIC Traditional streaming writes create many small files:
# MAGIC
# MAGIC ```
# MAGIC Without Liquid Clustering:
# MAGIC ├── part-001.parquet (CA data)
# MAGIC ├── part-002.parquet (NY data)
# MAGIC ├── part-003.parquet (CA data)  ← CA data scattered
# MAGIC ├── part-004.parquet (TX data)
# MAGIC └── part-005.parquet (CA data)  ← Slow queries
# MAGIC
# MAGIC With Liquid Clustering:
# MAGIC ├── part-001.parquet (all CA data together)
# MAGIC ├── part-002.parquet (all NY data together)
# MAGIC └── part-003.parquet (all TX data together)
# MAGIC → Fast queries, organized as data arrives!
# MAGIC ```
# MAGIC
# MAGIC ### Benefits
# MAGIC
# MAGIC ✅ **Data organized as it arrives** – No post-processing needed
# MAGIC ✅ **Faster queries** – Even on streaming data
# MAGIC ✅ **Automatic optimization** – Works with Predictive Optimization
# MAGIC ✅ **No manual tuning** – Set clustering keys once
# MAGIC
# MAGIC 📖 **Learn More**: [Liquid Clustering in Streaming](https://docs.databricks.com/en/delta/clustering.html#streaming)

# COMMAND ----------

# DBTITLE 1,Create Streaming Table with Liquid Clustering
# Create table with liquid clustering
clustered_table = "loans_streaming_clustered"
spark.sql(f"DROP TABLE IF EXISTS {clustered_table}")

# Create table with clustering
spark.sql(f"""
    CREATE TABLE {clustered_table} (
        loan_amnt INT,
        funded_amnt INT,
        paid_amnt DOUBLE,
        addr_state STRING,
        closed STRING,
        annual_inc DOUBLE,
        emp_length DOUBLE,
        dti DOUBLE,
        delinq_2yrs DOUBLE,
        revol_util DOUBLE,
        total_acc DOUBLE,
        credit_length_in_years DOUBLE,
        term STRING,
        home_ownership STRING,
        purpose STRING,
        verification_status STRING,
        application_type STRING,
        batch_id INT,
        event_time TIMESTAMP
    )
    USING DELTA
    CLUSTER BY (addr_state, term)
""")

print(f"✅ Created table with liquid clustering: {clustered_table}")
print("✅ Clustered by: addr_state, term")

# COMMAND ----------

# DBTITLE 1,Write Stream with Liquid Clustering
# Write streaming data with liquid clustering
clustered_query = (
    streaming_df.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{checkpoint_path}/clustered")
    .trigger(processingTime="5 seconds")
    .toTable(clustered_table)
)

print(f"✅ Started streaming query with liquid clustering")
print("⚡ Data will be automatically organized by addr_state and term")

# COMMAND ----------

# DBTITLE 1,Monitor Clustered Streaming
# Wait for data to be processed
time.sleep(10)

# Check record count
clustered_count = spark.table(clustered_table).count()
print(f"📊 Records in clustered table: {clustered_count:,}")

# View table details
display(spark.sql(f"DESCRIBE DETAIL {clustered_table}"))

# COMMAND ----------

# DBTITLE 1,Stop Clustered Query
clustered_query.stop()
print("✅ Clustered streaming query stopped")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🪟 Windowed Aggregations
# MAGIC
# MAGIC ### Why Windows?
# MAGIC
# MAGIC Windows enable time-based analytics on streaming data:
# MAGIC - 📊 **Tumbling windows**: Non-overlapping (e.g., hourly totals)
# MAGIC - 🔄 **Sliding windows**: Overlapping (e.g., 5-min average over last 10 min)
# MAGIC - 📅 **Session windows**: Based on activity gaps
# MAGIC
# MAGIC ### Real-Time Financial Analytics Example

# COMMAND ----------

# DBTITLE 1,Create Streaming Aggregation with Windows
# Calculate loan statistics per state in 1-minute windows
windowed_aggregation = (
    streaming_df.withWatermark("event_time", "10 minutes")  # Handle late data
    .groupBy(window(col("event_time"), "1 minute"), col("addr_state"))
    .agg(
        count("*").alias("loan_count"),
        round(avg("loan_amnt"), 2).alias("avg_loan_amount"),
        round(sum("loan_amnt"), 2).alias("total_loan_amount"),
        round(avg("annual_inc"), 2).alias("avg_income"),
    )
)

print("✅ Created windowed aggregation")

# COMMAND ----------

# DBTITLE 1,Write Windowed Aggregation to Delta
# Write aggregated results to Delta table
agg_table = "loans_streaming_aggregates"
spark.sql(f"DROP TABLE IF EXISTS {agg_table}")

agg_query = (
    windowed_aggregation.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{checkpoint_path}/aggregates")
    .trigger(processingTime="5 seconds")
    .toTable(agg_table)
)

print(f"✅ Started aggregation query: {agg_table}")

# COMMAND ----------

# DBTITLE 1,View Aggregated Results
# Wait for aggregations to process
time.sleep(10)

# Display aggregated results
display(
    spark.sql(f"""
    SELECT 
        window.start as window_start,
        window.end as window_end,
        addr_state,
        loan_count,
        avg_loan_amount,
        total_loan_amount,
        avg_income
    FROM {agg_table}
    ORDER BY window_start DESC, total_loan_amount DESC
    LIMIT 20
""")
)

# COMMAND ----------

# DBTITLE 1,Stop Aggregation Query
agg_query.stop()
print("✅ Aggregation query stopped")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 💧 Watermarking for Late Data
# MAGIC
# MAGIC ### Why Watermarking?
# MAGIC
# MAGIC Real-world data arrives late:
# MAGIC
# MAGIC ```
# MAGIC Expected:  Event 1 → Event 2 → Event 3 → Event 4
# MAGIC Reality:   Event 1 → Event 3 → Event 2 → Event 4
# MAGIC                                  ↑ Late arrival!
# MAGIC ```
# MAGIC
# MAGIC **Watermarking** defines how long to wait for late data:
# MAGIC
# MAGIC ```
# MAGIC Watermark = "10 minutes"
# MAGIC → Wait up to 10 minutes for late events
# MAGIC → Drop events older than 10 minutes
# MAGIC ```
# MAGIC
# MAGIC ### Trade-offs
# MAGIC
# MAGIC | Watermark | Accuracy | Memory | Latency |
# MAGIC |-----------|----------|--------|---------|
# MAGIC | Short (1 min) | ⚠️ May miss late data | ✅ Low | ✅ Fast |
# MAGIC | Long (1 hour) | ✅ Captures late data | ⚠️ High | ⚠️ Slow |

# COMMAND ----------

# DBTITLE 1,Aggregation with Watermarking
# Create aggregation with watermark for late data
watermarked_agg = (
    streaming_df.withWatermark(
        "event_time", "10 minutes"
    )  # Wait up to 10 min for late data
    .groupBy(
        window(col("event_time"), "5 minutes", "1 minute"),  # 5-min window, 1-min slide
        col("addr_state"),
    )
    .agg(
        count("*").alias("loan_count"),
        round(avg("loan_amnt"), 2).alias("avg_loan_amount"),
    )
)

print("✅ Created watermarked aggregation")
print("💧 Watermark: 10 minutes")
print("🪟 Window: 5 minutes (sliding every 1 minute)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔄 Stream-to-Stream Joins
# MAGIC
# MAGIC ### Why Stream Joins?
# MAGIC
# MAGIC Join multiple real-time data sources:
# MAGIC - 📊 Enrich loan applications with credit scores
# MAGIC - 🔗 Combine user activity with transaction data
# MAGIC - 🎯 Match events across systems

# COMMAND ----------

# DBTITLE 1,Create Second Streaming Source (Credit Scores)
# Simulate credit score updates
credit_scores = df.select(
    col("loan_amnt"),
    col("addr_state"),
    col("event_time"),
    (lit(300) + (rand() * 550)).cast("int").alias("credit_score"),
).limit(2000)

# Write credit scores as streaming source
credit_path = f"{base_path}/credit_scores"
dbutils.fs.rm(credit_path, recurse=True)

for i in range(3):
    batch = credit_scores.limit(500).withColumn("batch_id", lit(i))
    batch.write.format("json").mode("overwrite").save(f"{credit_path}/batch_{i}")
    print(f"✅ Wrote credit score batch {i}")

# COMMAND ----------

# DBTITLE 1,Create Credit Score Stream
# Define schema for credit scores
credit_schema = StructType(
    [
        StructField("loan_amnt", IntegerType(), True),
        StructField("addr_state", StringType(), True),
        StructField("event_time", TimestampType(), True),
        StructField("credit_score", IntegerType(), True),
        StructField("batch_id", IntegerType(), True),
    ]
)

# Create streaming DataFrame for credit scores
credit_stream = (
    spark.readStream.format("json")
    .schema(credit_schema)
    .option("maxFilesPerTrigger", 1)
    .load(credit_path)
)

print("✅ Created credit score stream")

# COMMAND ----------

# DBTITLE 1,Perform Stream-to-Stream Join
# Join loan applications with credit scores
joined_stream = (
    streaming_df.withWatermark("event_time", "10 minutes")
    .join(
        credit_stream.withWatermark("event_time", "10 minutes"),
        expr("""
            loan_amnt = credit_score.loan_amnt AND
            addr_state = credit_score.addr_state AND
            event_time >= credit_score.event_time - interval 5 minutes AND
            event_time <= credit_score.event_time + interval 5 minutes
        """),
        "inner",
    )
    .select(
        streaming_df["loan_amnt"],
        streaming_df["funded_amnt"],
        streaming_df["addr_state"],
        streaming_df["annual_inc"],
        streaming_df["term"],
        streaming_df["purpose"],
        credit_stream["credit_score"],
        streaming_df["event_time"],
    )
)

print("✅ Created stream-to-stream join")

# COMMAND ----------

# DBTITLE 1,Write Joined Stream to Delta
# Write enriched data to Delta table
enriched_table = "loans_enriched"
spark.sql(f"DROP TABLE IF EXISTS {enriched_table}")

enriched_query = (
    joined_stream.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{checkpoint_path}/enriched")
    .trigger(processingTime="5 seconds")
    .toTable(enriched_table)
)

print(f"✅ Started enriched stream: {enriched_table}")

# COMMAND ----------

# DBTITLE 1,View Enriched Data
# Wait for data to be processed
time.sleep(10)

# Display enriched data
display(
    spark.sql(f"""
    SELECT 
        loan_amnt,
        funded_amnt,
        addr_state,
        annual_inc,
        credit_score,
        term,
        purpose,
        event_time
    FROM {enriched_table}
    ORDER BY event_time DESC
    LIMIT 20
""")
)

# COMMAND ----------

# DBTITLE 1,Stop Enriched Query
enriched_query.stop()
print("✅ Enriched query stopped")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔀 Change Data Capture (CDC) with MERGE
# MAGIC
# MAGIC ### Why CDC?
# MAGIC
# MAGIC CDC captures changes from source systems:
# MAGIC - ➕ **INSERT**: New records
# MAGIC - 🔄 **UPDATE**: Modified records
# MAGIC - ❌ **DELETE**: Removed records
# MAGIC
# MAGIC ### CDC Pattern with Delta Lake
# MAGIC
# MAGIC ```
# MAGIC Source System → CDC Stream → Delta MERGE → Target Table
# MAGIC                              ↓
# MAGIC                    Upsert (INSERT + UPDATE)
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Create Target Table for CDC
# Create target table
cdc_table = "loans_cdc_target"
spark.sql(f"DROP TABLE IF EXISTS {cdc_table}")

# Initialize with some data
df.limit(5000).write.format("delta").mode("overwrite").saveAsTable(cdc_table)

print(f"✅ Created CDC target table: {cdc_table}")
print(f"📊 Initial record count: {spark.table(cdc_table).count():,}")

# COMMAND ----------

# DBTITLE 1,Simulate CDC Stream
# Create CDC stream with updates and new records
cdc_stream = streaming_df.withColumn(
    "operation", when(col("batch_id") % 2 == 0, lit("INSERT")).otherwise(lit("UPDATE"))
).withColumn(
    "updated_paid_amnt",
    when(col("operation") == "UPDATE", col("funded_amnt") * 0.5).otherwise(
        col("paid_amnt")
    ),
)

print("✅ Created CDC stream with INSERT and UPDATE operations")

# COMMAND ----------


# DBTITLE 1,Apply CDC with foreachBatch and MERGE
# Define function to merge CDC data
def merge_cdc_batch(batch_df, batch_id):
    # Load target table
    target_table = DeltaTable.forName(spark, cdc_table)

    # Perform MERGE
    target_table.alias("target").merge(
        batch_df.alias("source"),
        "target.loan_amnt = source.loan_amnt AND target.addr_state = source.addr_state",
    ).whenMatchedUpdate(
        set={"paid_amnt": "source.updated_paid_amnt", "closed": "source.closed"}
    ).whenNotMatchedInsertAll().execute()

    print(f"✅ Merged batch {batch_id}")


# Write stream with foreachBatch
cdc_query = (
    cdc_stream.writeStream.foreachBatch(merge_cdc_batch)
    .option("checkpointLocation", f"{checkpoint_path}/cdc")
    .trigger(processingTime="5 seconds")
    .start()
)

print("✅ Started CDC streaming query")

# COMMAND ----------

# DBTITLE 1,Monitor CDC Results
# Wait for CDC to process
time.sleep(15)

# Check record count
final_count = spark.table(cdc_table).count()
print(f"📊 Final record count: {final_count:,}")

# View some updated records
display(
    spark.sql(f"""
    SELECT loan_amnt, funded_amnt, paid_amnt, addr_state, closed
    FROM {cdc_table}
    WHERE paid_amnt > 0
    LIMIT 20
""")
)

# COMMAND ----------

# DBTITLE 1,Stop CDC Query
cdc_query.stop()
print("✅ CDC query stopped")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Monitoring Streaming Queries
# MAGIC
# MAGIC ### Key Metrics to Monitor
# MAGIC
# MAGIC | Metric | What It Means | Action If High |
# MAGIC |--------|---------------|----------------|
# MAGIC | **Input Rate** | Records/second arriving | Scale up if backlog grows |
# MAGIC | **Process Rate** | Records/second processed | Optimize query or add resources |
# MAGIC | **Batch Duration** | Time to process each batch | Reduce batch size or optimize |
# MAGIC | **Trigger Interval** | Time between batches | Adjust based on latency needs |

# COMMAND ----------

# DBTITLE 1,View Active Streaming Queries
# List all active streaming queries
active_queries = spark.streams.active

print(f"📊 Active streaming queries: {len(active_queries)}")
for query in active_queries:
    print(f"   - {query.name}: {query.status}")

# COMMAND ----------

# DBTITLE 1,View Query Progress
# Get detailed progress for a query (if any are running)
if len(active_queries) > 0:
    query = active_queries[0]
    progress = query.lastProgress

    if progress:
        print(f"📊 Query: {query.name}")
        print(f"📊 Batch ID: {progress['batchId']}")
        print(f"📊 Input Rows: {progress['numInputRows']}")
        print(
            f"📊 Process Rate: {progress.get('processedRowsPerSecond', 'N/A')} rows/sec"
        )
        print(f"📊 Batch Duration: {progress.get('batchDuration', 'N/A')} ms")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧹 Cleanup

# COMMAND ----------

# DBTITLE 1,Stop All Streaming Queries
# Stop all active queries
for query in spark.streams.active:
    query.stop()
    print(f"✅ Stopped query: {query.name}")

print("✅ All streaming queries stopped")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📝 Summary
# MAGIC
# MAGIC ### What We Covered
# MAGIC
# MAGIC ✅ **Structured Streaming Fundamentals**
# MAGIC - Understood streaming as unbounded tables
# MAGIC - Read and wrote streaming data with Delta Lake
# MAGIC - Implemented checkpoints for fault tolerance
# MAGIC
# MAGIC ✅ **⚡ Liquid Clustering in Streaming (NEW - 2024)**
# MAGIC - Enabled automatic data organization as data arrives
# MAGIC - Achieved faster queries on streaming data
# MAGIC - No post-processing optimization needed
# MAGIC
# MAGIC ✅ **Windowed Aggregations**
# MAGIC - Built real-time analytics with time windows
# MAGIC - Calculated rolling statistics
# MAGIC - Implemented tumbling and sliding windows
# MAGIC
# MAGIC ✅ **Watermarking**
# MAGIC - Handled late-arriving data gracefully
# MAGIC - Balanced accuracy vs. memory usage
# MAGIC - Set appropriate watermark thresholds
# MAGIC
# MAGIC ✅ **Stream-to-Stream Joins**
# MAGIC - Joined multiple real-time data sources
# MAGIC - Enriched streaming data with additional context
# MAGIC - Used time-based join conditions
# MAGIC
# MAGIC ✅ **Change Data Capture (CDC)**
# MAGIC - Implemented upsert patterns with MERGE
# MAGIC - Handled INSERT and UPDATE operations
# MAGIC - Used foreachBatch for complex logic
# MAGIC
# MAGIC ✅ **Monitoring**
# MAGIC - Tracked streaming query metrics
# MAGIC - Monitored input and process rates
# MAGIC - Managed query lifecycle
# MAGIC
# MAGIC ### Key Takeaways
# MAGIC
# MAGIC 💡 **Structured Streaming** = Same DataFrame API for batch and streaming
# MAGIC 💡 **Liquid Clustering** = Automatic data organization in streaming workloads
# MAGIC 💡 **Watermarking** = Essential for handling late data in aggregations
# MAGIC 💡 **Delta Lake** = ACID guarantees and exactly-once processing for streams
# MAGIC 💡 **foreachBatch** = Enables complex operations like MERGE in streaming
# MAGIC
# MAGIC ### Next Steps
# MAGIC
# MAGIC 🎯 **Ready for production?** Open **05-Production-Ready-Patterns.py** to learn:
# MAGIC - **Predictive Optimization** (NEW - GA 2024) for automatic maintenance
# MAGIC - **Serverless Compute** (NEW - GA 2025) for zero infrastructure management
# MAGIC - **Unity Catalog Iceberg Tables** (NEW - 2024) for interoperability
# MAGIC - Performance tuning and monitoring
# MAGIC - Data governance and security
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Outstanding! 🚀 You've mastered streaming with Delta Lake!**
