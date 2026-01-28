# Databricks notebook source
# MAGIC %md
# MAGIC # 📊 Getting Started with Databricks Data Engineering
# MAGIC
# MAGIC **Level**: Beginner
# MAGIC **Duration**: 20 minutes
# MAGIC **Dataset**: Lending Club Loan Data (14,705 records)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Learning Objectives
# MAGIC
# MAGIC By the end of this notebook, you will:
# MAGIC - ✅ Understand the tutorial series structure and learning path
# MAGIC - ✅ Know what Databricks and Delta Lake are and why they matter
# MAGIC - ✅ Explore the Lending Club dataset we'll use throughout
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
# MAGIC ## 💰 About the Lending Club Dataset
# MAGIC
# MAGIC ### What is Lending Club?
# MAGIC
# MAGIC **Lending Club** was a peer-to-peer lending platform where investors funded personal loans. This dataset contains **real loan data** with risk classifications.
# MAGIC
# MAGIC ### Dataset Details
# MAGIC
# MAGIC | Property | Value |
# MAGIC |----------|-------|
# MAGIC | **Records** | 14,705 loans |
# MAGIC | **Format** | Parquet (Snappy compressed) |
# MAGIC | **Size** | ~1.5 MB |
# MAGIC | **Location** | `/databricks-datasets/learning-spark-v2/loans/loan-risks.snappy.parquet` |
# MAGIC | **Source** | Learning Spark V2 book |
# MAGIC
# MAGIC ### Key Fields
# MAGIC
# MAGIC | Field | Description | Example |
# MAGIC |-------|-------------|---------|
# MAGIC | `loan_amnt` | Loan amount requested | $10,000 |
# MAGIC | `funded_amnt` | Amount funded by investors | $10,000 |
# MAGIC | `paid_amnt` | Amount paid back | $8,500 |
# MAGIC | `addr_state` | Borrower's state | CA, NY, TX |
# MAGIC | `annual_inc` | Annual income | $75,000 |
# MAGIC | `dti` | Debt-to-income ratio | 18.5% |
# MAGIC | `term` | Loan term | 36 months, 60 months |
# MAGIC | `home_ownership` | Housing status | RENT, OWN, MORTGAGE |
# MAGIC | `purpose` | Loan purpose | debt_consolidation, credit_card |
# MAGIC
# MAGIC ### Why This Dataset?
# MAGIC
# MAGIC ✅ **Built-in**: No downloads required
# MAGIC ✅ **Real-world**: Actual financial data
# MAGIC ✅ **Perfect size**: Large enough to be interesting, small enough to run fast
# MAGIC ✅ **Rich features**: Numeric, categorical, and text fields
# MAGIC ✅ **Industry-standard**: Used in 50+ Databricks tutorials

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Environment Verification
# MAGIC
# MAGIC Let's verify your Databricks environment is ready!

# COMMAND ----------

# DBTITLE 1,Check Spark Version
print(f"✅ Spark Version: {spark.version}")
print(
    f"✅ Python Version: {spark.conf.get('spark.executorEnv.PYTHONHASHSEED', 'default')}"
)

# COMMAND ----------

# DBTITLE 1,Verify Dataset Access
# Check if the Lending Club dataset exists
dataset_path = "/databricks-datasets/learning-spark-v2/loans/loan-risks.snappy.parquet"

try:
    # Try to read the dataset
    df = spark.read.format("parquet").load(dataset_path)
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

# DBTITLE 1,Set Demo Configuration
# Reduce shuffle partitions for faster demos (default is 200)
spark.conf.set("spark.sql.shuffle.partitions", "1")

# Enable adaptive query execution (AQE) for better performance
spark.conf.set("spark.sql.adaptive.enabled", "true")

print("✅ Configuration set for demo environment")
print(f"   Shuffle partitions: {spark.conf.get('spark.sql.shuffle.partitions')}")
print(f"   Adaptive query execution: {spark.conf.get('spark.sql.adaptive.enabled')}")

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
display(
    df.select("loan_amnt", "funded_amnt", "paid_amnt", "annual_inc", "dti").summary()
)

# COMMAND ----------

# DBTITLE 1,Loan Distribution by State
# See which states have the most loans
from pyspark.sql.functions import count, col

state_distribution = (
    df.groupBy("addr_state")
    .agg(count("*").alias("loan_count"))
    .orderBy(col("loan_count").desc())
    .limit(10)
)

display(state_distribution)

# COMMAND ----------

# MAGIC %md
# MAGIC 💡 **Visualization Tip**: Click the bar chart icon above to see a visual representation!

# COMMAND ----------

# DBTITLE 1,Loan Purpose Breakdown
# What are people borrowing money for?
from pyspark.sql.functions import count

purpose_breakdown = (
    df.groupBy("purpose").agg(count("*").alias("count")).orderBy(col("count").desc())
)

display(purpose_breakdown)

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
# MAGIC ✅ **Lending Club dataset** – 14,705 real loan records
# MAGIC ✅ **Environment verification** – Confirmed dataset access and Spark version
# MAGIC ✅ **Quick exploration** – Previewed data and created simple visualizations
# MAGIC
# MAGIC ### Key Takeaways
# MAGIC
# MAGIC 💡 **Databricks** = Managed Spark platform for data engineering
# MAGIC 💡 **Delta Lake** = Parquet + transaction log = reliable data lake
# MAGIC 💡 **display()** = Databricks function for interactive visualizations
# MAGIC 💡 **Lending Club data** = Perfect dataset for learning data engineering
# MAGIC
# MAGIC ### Next Steps
# MAGIC
# MAGIC 🎯 **Ready to dive deeper?** Open **01-Data-Ingestion-and-Exploration.py** to start working with the data!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Happy Learning! 🚀**
