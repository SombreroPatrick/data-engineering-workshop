# Databricks notebook source
# MAGIC %md
# MAGIC # 📊 Getting Started with Databricks Data Engineering
# MAGIC
# MAGIC **Level**: Beginner
# MAGIC **Duration**: 20 minutes
# MAGIC **Dataset**: Retail-Org Sales Data (100K+ orders)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Learning Objectives
# MAGIC
# MAGIC By the end of this notebook, you will:
# MAGIC - ✅ Understand the tutorial series structure and learning path
# MAGIC - ✅ Know what Databricks and Delta Lake are and why they matter
# MAGIC - ✅ Explore the Retail-Org dataset we'll use throughout
# MAGIC - ✅ Verify your environment is ready for the tutorials
# MAGIC - ✅ Access key documentation resources
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📖 Documentation Links
# MAGIC
# MAGIC Keep these resources handy as you progress:
# MAGIC
# MAGIC | Resource | Link |
# MAGIC |----------|------|
# MAGIC | 📘 Databricks Docs | [docs.databricks.com](https://docs.databricks.com/) |
# MAGIC | 🔷 Delta Lake Tutorial | [Delta Lake Guide](https://docs.databricks.com/en/delta/tutorial.html) |
# MAGIC | 🐍 PySpark API | [PySpark Reference](https://spark.apache.org/docs/latest/api/python/) |
# MAGIC | 🎓 Databricks Academy | [Free Training](https://www.databricks.com/learn/training/home) |
# MAGIC | 💬 Community Forums | [community.databricks.com](https://community.databricks.com/) |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🗺️ Tutorial Series Overview
# MAGIC
# MAGIC This series takes you from **zero to production-ready** data engineering skills:
# MAGIC
# MAGIC | Notebook | Focus | What You'll Build |
# MAGIC |----------|-------|-------------------|
# MAGIC | **00** | Getting Started | Environment setup, dataset exploration |
# MAGIC | **01** | Data Ingestion | Load Parquet files, explore schemas, basic transformations |
# MAGIC | **02** | Delta Lake Basics | Create Delta tables, CRUD operations, Unity Catalog |
# MAGIC | **03** | Delta Advanced | Time travel, merge/upsert, optimization (Z-ordering) |
# MAGIC | **04** | Streaming | Real-time data processing with Structured Streaming |
# MAGIC | **05** | Production | Data quality, monitoring, performance tuning |
# MAGIC
# MAGIC ### 🎓 Learning Path
# MAGIC
# MAGIC ```
# MAGIC Beginner → Intermediate → Advanced
# MAGIC    ↓           ↓              ↓
# MAGIC  00-02       03-04           05
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🤔 What is Databricks?
# MAGIC
# MAGIC ### Why Databricks Matters
# MAGIC
# MAGIC **Databricks** is a unified analytics platform built on Apache Spark that simplifies data engineering, data science, and machine learning workflows.
# MAGIC
# MAGIC | Traditional Approach | Databricks Approach |
# MAGIC |---------------------|---------------------|
# MAGIC | ❌ Manage Spark clusters manually | ✅ Auto-scaling clusters |
# MAGIC | ❌ Configure infrastructure | ✅ Serverless compute |
# MAGIC | ❌ Separate tools for ETL, ML, BI | ✅ Unified platform |
# MAGIC | ❌ Complex security setup | ✅ Built-in governance (Unity Catalog) |
# MAGIC | ❌ Limited collaboration | ✅ Notebooks with real-time co-editing |
# MAGIC
# MAGIC ### 🔑 Key Features
# MAGIC
# MAGIC - **Apache Spark**: Distributed processing for big data
# MAGIC - **Delta Lake**: ACID transactions for data lakes
# MAGIC - **Unity Catalog**: Centralized data governance
# MAGIC - **Collaborative Notebooks**: Share code, visualizations, and insights
# MAGIC - **MLflow Integration**: Track experiments and deploy models
# MAGIC
# MAGIC 💡 **Think of Databricks as**: *"AWS for data engineering"* – it handles infrastructure so you focus on data.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔷 What is Delta Lake?
# MAGIC
# MAGIC ### The Problem with Data Lakes
# MAGIC
# MAGIC Traditional data lakes (Parquet, CSV, JSON) have limitations:
# MAGIC
# MAGIC ⚠️ **No ACID transactions** → Data corruption during concurrent writes
# MAGIC ⚠️ **No schema enforcement** → Bad data sneaks in
# MAGIC ⚠️ **No time travel** → Can't undo mistakes
# MAGIC ⚠️ **Slow metadata operations** → Listing files takes forever
# MAGIC
# MAGIC ### Delta Lake Solution
# MAGIC
# MAGIC **Delta Lake** adds a transaction log on top of Parquet files:
# MAGIC
# MAGIC | Feature | Benefit |
# MAGIC |---------|---------|
# MAGIC | ✅ ACID Transactions | Multiple users can write safely |
# MAGIC | ✅ Schema Enforcement | Reject bad data automatically |
# MAGIC | ✅ Time Travel | Query historical versions |
# MAGIC | ✅ Upserts & Deletes | Update/delete rows efficiently |
# MAGIC | ✅ Scalable Metadata | Fast queries on petabyte-scale data |
# MAGIC
# MAGIC ```
# MAGIC Parquet Files + Transaction Log = Delta Lake
# MAGIC     (storage)        (metadata)      (magic!)
# MAGIC ```
# MAGIC
# MAGIC 📖 **Learn More**: [What is Delta Lake?](https://delta.io/)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🛒 About the Retail-Org Dataset
# MAGIC
# MAGIC ### What is Retail-Org?
# MAGIC
# MAGIC The **retail-org** dataset contains e-commerce sales and customer data from a fictional online retail company.
# MAGIC
# MAGIC ### Dataset Details
# MAGIC
# MAGIC | Property | Value |
# MAGIC |----------|-------|
# MAGIC | **Records** | ~100K+ sales orders |
# MAGIC | **Format** | JSON (sales_orders), CSV (customers) |
# MAGIC | **Location** | `/databricks-datasets/retail-org/` |
# MAGIC | **Tables** | `sales_orders`, `customers` |
# MAGIC
# MAGIC ### Key Fields - Sales Orders
# MAGIC
# MAGIC | Field | Description | Example |
# MAGIC |-------|-------------|---------|
# MAGIC | `order_number` | Unique order ID | 1000001 |
# MAGIC | `order_datetime` | Order timestamp | "2019-03-11T10:30:25.00Z" |
# MAGIC | `customer_id` | Customer identifier | "C00001" |
# MAGIC | `customer_name` | Customer name | "John Smith" |
# MAGIC | `ordered_products` | Array of products in order | [struct, struct, ...] |
# MAGIC | `ordered_products.name` | Product name | "Widget A" |
# MAGIC | `ordered_products.price` | Unit price | 25 |
# MAGIC | `ordered_products.qty` | Quantity ordered | 2 |
# MAGIC
# MAGIC ### Key Fields - Customers
# MAGIC
# MAGIC | Field | Description | Example |
# MAGIC |-------|-------------|---------|
# MAGIC | `customer_id` | Unique customer ID | C12345 |
# MAGIC | `state` | Customer state | CA |
# MAGIC | `loyalty_segment` | Customer tier | Gold, Silver, Bronze |
# MAGIC
# MAGIC ### Why This Dataset?
# MAGIC
# MAGIC ✅ **Built-in to Databricks** (no download required)
# MAGIC ✅ **Realistic business data** (orders, customers, products)
# MAGIC ✅ **Multiple tables** for joins and relationships
# MAGIC ✅ **Perfect size** for learning (~50MB)
# MAGIC ✅ **Commonly used** in official Databricks tutorials

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Environment Verification
# MAGIC
# MAGIC Let's verify your Databricks environment is ready!

# COMMAND ----------

# DBTITLE 1,Check Spark Version
import sys

print(f"✅ Spark Version: {spark.version}")
print(f"✅ Python Version: {sys.version.split()[0]}")

# COMMAND ----------

# DBTITLE 1,Verify Dataset Access
# Check if the Retail-Org dataset exists
dataset_path = "/databricks-datasets/retail-org/sales_orders/"

try:
    # Try to read the dataset
    df = spark.read.json(dataset_path)
    record_count = df.count()
    column_count = len(df.columns)

    print(f"✅ Dataset found!")
    print(f"✅ Records: {record_count:,}")
    print(f"✅ Columns: {column_count}")
    print(f"✅ Path: {dataset_path}")
except Exception as e:
    print(f"❌ Error accessing dataset: {e}")
    print(
        f"⚠️  Make sure you're running on a Databricks cluster with access to /databricks-datasets/"
    )

# COMMAND ----------

# DBTITLE 1,Preview Dataset Schema
# Display the schema to see what fields we'll work with
df.printSchema()

# COMMAND ----------

# DBTITLE 1,Peek at Sample Data
# Show a few records to get familiar with the data
display(df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎨 Databricks Notebook Features
# MAGIC
# MAGIC ### Magic Commands
# MAGIC
# MAGIC Databricks notebooks support special commands:
# MAGIC
# MAGIC | Command | Purpose | Example |
# MAGIC |---------|---------|---------|
# MAGIC | `%md` | Markdown cell | `%md # Title` |
# MAGIC | `%sql` | SQL query | `%sql SELECT * FROM table` |
# MAGIC | `%python` | Python code (default) | `%python print("hello")` |
# MAGIC | `%scala` | Scala code | `%scala val x = 1` |
# MAGIC | `%r` | R code | `%r x <- 1` |
# MAGIC | `%sh` | Shell command | `%sh ls -la` |
# MAGIC | `%fs` | File system command | `%fs ls /databricks-datasets/` |
# MAGIC | `%run` | Run another notebook | `%run ./setup` |
# MAGIC
# MAGIC ### Display Function
# MAGIC
# MAGIC The `display()` function is **Databricks magic** for visualizations:
# MAGIC
# MAGIC ```python
# MAGIC display(df)  # Interactive table with charts, filters, downloads
# MAGIC ```
# MAGIC
# MAGIC 💡 **Pro Tip**: Click the chart icons above any `display()` output to create visualizations!

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔧 Configuration Best Practices
# MAGIC
# MAGIC ### Demo vs. Production Settings
# MAGIC
# MAGIC For these tutorials, we use **demo-optimized** settings:

# COMMAND ----------

# DBTITLE 1,Configure Spark (Optional)
# Note: Databricks Runtime comes pre-configured with optimal settings
# These overrides are optional for demo environments

print("ℹ️  Databricks Runtime includes pre-optimized Spark configurations")
print("✅ No manual configuration needed for this tutorial")

# Optional: Reduce partitions for small demo datasets
try:
    current = spark.conf.get("spark.sql.shuffle.partitions")
    print(f"📊 Current shuffle partitions: {current}")
except:
    print("📊 Using default shuffle partitions")

# COMMAND ----------

# MAGIC %md
# MAGIC ⚠️ **Production Note**:
# MAGIC - Use `spark.sql.shuffle.partitions` = 200+ for large datasets
# MAGIC - Tune based on cluster size and data volume
# MAGIC - Monitor query plans with `df.explain()` and Spark UI

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Quick Data Exploration
# MAGIC
# MAGIC Let's do a quick exploration to get familiar with the data:

# COMMAND ----------

# DBTITLE 1,Summary Statistics
# Get basic statistics for numeric columns
display(df.select("order_number", "number_of_line_items").summary())

# COMMAND ----------

# DBTITLE 1,Orders by Customer
# See which customers have the most orders
from pyspark.sql.functions import count, col

customer_distribution = (
    df.groupBy("customer_id", "customer_name")
    .agg(count("*").alias("order_count"))
    .orderBy(col("order_count").desc())
    .limit(10)
)

display(customer_distribution)

# COMMAND ----------

# MAGIC %md
# MAGIC 💡 **Visualization Tip**: Click the bar chart icon above to see a visual representation!

# COMMAND ----------

# DBTITLE 1,Top Products Ordered
# What products are customers ordering? (explode nested array)
from pyspark.sql.functions import count, explode

product_breakdown = (
    df.select(explode("ordered_products").alias("product"))
    .groupBy("product.name")
    .agg(count("*").alias("order_count"))
    .orderBy(col("order_count").desc())
    .limit(10)
)

display(product_breakdown)

# COMMAND ----------

# DBTITLE 1,Calculate Order Totals (Nested Array Example)
# Working with nested arrays: explode products and calculate line totals
from pyspark.sql.functions import explode, sum as _sum

df_exploded = df.select(
    "order_number",
    "customer_id",
    "customer_name",
    "order_datetime",
    explode("ordered_products").alias("product"),
)

df_with_line_total = df_exploded.withColumn(
    "line_total", col("product.price") * col("product.qty")
)

order_totals = (
    df_with_line_total.groupBy(
        "order_number", "customer_id", "customer_name", "order_datetime"
    )
    .agg(_sum("line_total").alias("order_total"))
    .orderBy(col("order_total").desc())
)

display(order_totals.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎓 Prerequisites Check
# MAGIC
# MAGIC Before continuing to the next notebooks, make sure you have:
# MAGIC
# MAGIC ### Required Knowledge
# MAGIC - ✅ Basic Python (variables, functions, loops)
# MAGIC - ✅ SQL fundamentals (SELECT, WHERE, GROUP BY)
# MAGIC - ✅ Understanding of DataFrames (like pandas)
# MAGIC
# MAGIC ### Optional but Helpful
# MAGIC - 📚 Distributed computing concepts
# MAGIC - 📚 Data warehousing basics
# MAGIC - 📚 Apache Spark fundamentals
# MAGIC
# MAGIC ### Databricks Environment
# MAGIC - ✅ Access to a Databricks workspace (Community Edition works!)
# MAGIC - ✅ Cluster attached to this notebook
# MAGIC - ✅ Dataset accessible (verified above)
# MAGIC
# MAGIC 💡 **New to Spark?** No problem! We'll explain concepts as we go.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🚀 What's Next?
# MAGIC
# MAGIC ### Notebook 01: Data Ingestion & Exploration
# MAGIC
# MAGIC In the next notebook, you'll learn:
# MAGIC - 📥 How to load Parquet files into Spark DataFrames
# MAGIC - 🔍 Schema inference vs. explicit schemas
# MAGIC - 🧹 Data quality checks (nulls, duplicates, outliers)
# MAGIC - 🔄 Basic transformations (filter, select, aggregate)
# MAGIC - 📊 Creating visualizations with `display()`
# MAGIC - ✅ Best practices for data ingestion
# MAGIC
# MAGIC ### Run the Next Notebook
# MAGIC
# MAGIC ```python
# MAGIC %run ./01-Data-Ingestion-and-Exploration
# MAGIC ```
# MAGIC
# MAGIC Or open it manually from the workspace sidebar.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📚 Additional Resources
# MAGIC
# MAGIC ### Free Training
# MAGIC - [Databricks Academy - Getting Started with Data Engineering](https://www.databricks.com/learn/training/getting-started-with-data-engineering)
# MAGIC - [Apache Spark Tutorial](https://spark.apache.org/docs/latest/quick-start.html)
# MAGIC - [Delta Lake Quickstart](https://docs.delta.io/latest/quick-start.html)
# MAGIC
# MAGIC ### Books
# MAGIC - 📖 *Learning Spark* (2nd Edition) by Jules Damji et al.
# MAGIC - 📖 *Spark: The Definitive Guide* by Bill Chambers & Matei Zaharia
# MAGIC - 📖 *Delta Lake: The Definitive Guide* by Denny Lee et al.
# MAGIC
# MAGIC ### Community
# MAGIC - 💬 [Databricks Community Forums](https://community.databricks.com/)
# MAGIC - 💬 [Stack Overflow - Databricks Tag](https://stackoverflow.com/questions/tagged/databricks)
# MAGIC - 💬 [Delta Lake Slack](https://delta.io/slack)
# MAGIC
# MAGIC ### Certifications
# MAGIC - 🎓 [Databricks Certified Data Engineer Associate](https://www.databricks.com/learn/certification/data-engineer-associate)
# MAGIC - 🎓 [Databricks Certified Data Engineer Professional](https://www.databricks.com/learn/certification/data-engineer-professional)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📝 Summary
# MAGIC
# MAGIC ### What We Covered
# MAGIC
# MAGIC ✅ **Tutorial series structure** – 6 notebooks from beginner to advanced
# MAGIC ✅ **Databricks platform** – Unified analytics with managed Spark
# MAGIC ✅ **Delta Lake benefits** – ACID transactions, time travel, schema enforcement
# MAGIC ✅ **Retail-Org dataset** – 100K+ e-commerce sales orders
# MAGIC ✅ **Environment verification** – Confirmed dataset access and Spark version
# MAGIC ✅ **Quick exploration** – Previewed data and created simple visualizations
# MAGIC
# MAGIC ### Key Takeaways
# MAGIC
# MAGIC 💡 **Databricks** = Managed Spark platform for data engineering
# MAGIC 💡 **Delta Lake** = Parquet + transaction log = reliable data lake
# MAGIC 💡 **display()** = Databricks function for interactive visualizations
# MAGIC 💡 **Retail-Org data** = Perfect dataset for learning data engineering
# MAGIC
# MAGIC ### Next Steps
# MAGIC
# MAGIC 🎯 **Ready to dive deeper?** Open **01-Data-Ingestion-and-Exploration.py** to start working with the data!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Happy Learning! 🚀**
