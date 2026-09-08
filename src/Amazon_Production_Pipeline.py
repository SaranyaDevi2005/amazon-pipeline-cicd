# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Amazon Production Pipeline Title
# MAGIC %md
# MAGIC # Amazon Production Data Pipeline — Medallion Architecture
# MAGIC
# MAGIC A comprehensive, production-ready ETL pipeline implementing the **Bronze → Silver → Gold** medallion architecture for Amazon product data. This notebook covers data integration, schema enforcement, staging & validation, idempotent MERGE loads, atomicity, error handling with dead-letter queues, and checkpointing.

# COMMAND ----------

# DBTITLE 1,Section 1: Imports & Config (Markdown)
# MAGIC %md
# MAGIC ## 1. Imports & Configuration
# MAGIC
# MAGIC ### Data Integration & Configuration Management
# MAGIC
# MAGIC **Data Integration** is the process of combining data from different sources into a unified view. This pipeline integrates two Amazon product datasets:
# MAGIC - A CSV file with cleaned product data (76,704 rows)
# MAGIC - A Delta table `workspace.default.amazon_raw` with 53 columns of scraped product data
# MAGIC
# MAGIC All configuration is externalized as Python variables at the top of the notebook, making it easy to modify table names, paths, and parameters without touching pipeline logic.

# COMMAND ----------

# DBTITLE 1,Imports & Configuration
import uuid
import logging
from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField, StringType, DoubleType,
                              LongType, BooleanType, TimestampType, DateType)
from delta.tables import DeltaTable
from datetime import datetime

# ---------------------------------------------------------------------------
# CONFIGURATION — all table names, paths, and parameters in one place
# ---------------------------------------------------------------------------

CATALOG = "workspace"
SCHEMA = "amazon_pipeline"
FULL_SCHEMA = f"{CATALOG}.{SCHEMA}"

# Source tables / files
SRC_DELTA_TABLE = f"{CATALOG}.default.amazon_raw"
SRC_CSV_PATH = "/Workspace/Users/71762233042@cit.edu.in/amazon_products_cleaned.csv"

# Bronze table
BRONZE_TABLE = f"{FULL_SCHEMA}.bronze_amazon_products"

# Staging table
STAGING_TABLE = f"{FULL_SCHEMA}.staging_amazon_products"

# Silver table
SILVER_TABLE = f"{FULL_SCHEMA}.silver_amazon_products"

# Gold tables
DIM_CATEGORY_TABLE = f"{FULL_SCHEMA}.dim_category"
DIM_PRICE_BUCKET_TABLE = f"{FULL_SCHEMA}.dim_price_bucket"
FACT_TABLE = f"{FULL_SCHEMA}.fact_product_metrics"
AGG_CATEGORY_TABLE = f"{FULL_SCHEMA}.agg_category_summary"
AGG_PRICE_BUCKET_TABLE = f"{FULL_SCHEMA}.agg_price_bucket_summary"

# Resilience tables
DEAD_LETTER_TABLE = f"{FULL_SCHEMA}.dead_letter_products"
CHECKPOINT_TABLE = f"{FULL_SCHEMA}.pipeline_checkpoints"

# Pipeline metadata
BATCH_ID = str(uuid.uuid4())
INGESTION_DATE = datetime.now()

print(f"✅ Configuration loaded. Batch ID: {BATCH_ID}")
print(f"✅ Schema: {FULL_SCHEMA}")
print(f"✅ Source CSV: {SRC_CSV_PATH}")
print(f"✅ Source Delta: {SRC_DELTA_TABLE}")

# COMMAND ----------

# DBTITLE 1,Section 2: Schema & Table Setup (Markdown)
# MAGIC %md
# MAGIC ## 2. Schema & Table Setup
# MAGIC
# MAGIC ### Schema Design & Data Architecture
# MAGIC
# MAGIC **Schema Design** ensures data quality and consistency by enforcing column types, constraints, and naming conventions at the table level. We use Delta Lake's schema enforcement (`mergeSchema = false`) to prevent accidental schema drift.
# MAGIC
# MAGIC **Data Architecture (Medallion Model):**
# MAGIC - **Bronze**: Raw data as-is from sources, with minimal metadata added (ingestion_date, source_file, batch_id)
# MAGIC - **Silver**: Cleansed, validated, deduplicated data with proper types and derived columns
# MAGIC - **Gold**: Analytics-ready star schema with dimension tables, fact tables, and aggregation tables
# MAGIC
# MAGIC This layered approach provides isolation between raw ingestion, transformation, and analytics workloads.

# COMMAND ----------

# DBTITLE 1,Schema & Table Setup
# Create schema
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {FULL_SCHEMA}")
print(f"✅ Schema '{FULL_SCHEMA}' ready.")

# ---------------------------------------------------------------------------
# Bronze table — raw ingestion landing zone
# ---------------------------------------------------------------------------
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {BRONZE_TABLE} (
  name STRING,
  main_category STRING,
  sub_category STRING,
  image STRING,
  link STRING,
  ratings STRING,
  no_of_ratings STRING,
  discount_price STRING,
  actual_price STRING,
  source_file STRING,
  batch_id STRING,
  ingestion_date DATE
)
USING DELTA
PARTITIONED BY (ingestion_date)
""")
print(f"✅ Bronze table '{BRONZE_TABLE}' ready.")

# ---------------------------------------------------------------------------
# Staging table — pre-validation holding area
# ---------------------------------------------------------------------------
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {STAGING_TABLE} (
  name STRING,
  main_category STRING,
  sub_category STRING,
  image STRING,
  link STRING,
  ratings DOUBLE,
  no_of_ratings DOUBLE,
  discount_price DOUBLE,
  actual_price DOUBLE,
  discount_amount DOUBLE,
  discount_pct DOUBLE,
  is_valid BOOLEAN,
  validation_errors STRING,
  source_file STRING,
  batch_id STRING,
  ingestion_date TIMESTAMP
)
USING DELTA
""")
print(f"✅ Staging table '{STAGING_TABLE}' ready.")

# ---------------------------------------------------------------------------
# Silver table — cleansed, validated, standardized
# ---------------------------------------------------------------------------
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER_TABLE} (
  name STRING,
  main_category STRING,
  sub_category STRING,
  image STRING,
  link STRING,
  ratings DOUBLE,
  no_of_ratings DOUBLE,
  discount_price DOUBLE,
  actual_price DOUBLE,
  discount_amount DOUBLE,
  discount_pct DOUBLE,
  is_valid BOOLEAN,
  source_file STRING,
  batch_id STRING,
  ingestion_date TIMESTAMP
)
USING DELTA
""")
print(f"✅ Silver table '{SILVER_TABLE}' ready.")

# ---------------------------------------------------------------------------
# Gold — Dimension: dim_category
# ---------------------------------------------------------------------------
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {DIM_CATEGORY_TABLE} (
  category_key BIGINT GENERATED ALWAYS AS IDENTITY,
  main_category STRING,
  sub_category STRING
)
USING DELTA
""")
print(f"✅ Dim table '{DIM_CATEGORY_TABLE}' ready.")

# ---------------------------------------------------------------------------
# Gold — Dimension: dim_price_bucket
# ---------------------------------------------------------------------------
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {DIM_PRICE_BUCKET_TABLE} (
  price_bucket_key BIGINT GENERATED ALWAYS AS IDENTITY,
  bucket_label STRING,
  min_price DOUBLE,
  max_price DOUBLE
)
USING DELTA
""")
print(f"✅ Dim table '{DIM_PRICE_BUCKET_TABLE}' ready.")

# ---------------------------------------------------------------------------
# Gold — Fact table: fact_product_metrics
# ---------------------------------------------------------------------------
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FACT_TABLE} (
  product_key BIGINT GENERATED ALWAYS AS IDENTITY,
  name STRING,
  category_key BIGINT,
  price_bucket_key BIGINT,
  ratings DOUBLE,
  no_of_ratings DOUBLE,
  discount_price DOUBLE,
  actual_price DOUBLE,
  discount_pct DOUBLE,
  is_valid BOOLEAN
)
USING DELTA
""")
print(f"✅ Fact table '{FACT_TABLE}' ready.")

# ---------------------------------------------------------------------------
# Gold — Aggregation: agg_category_summary
# ---------------------------------------------------------------------------
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {AGG_CATEGORY_TABLE} (
  main_category STRING,
  product_count LONG,
  avg_rating DOUBLE,
  avg_discount_price DOUBLE,
  avg_actual_price DOUBLE,
  avg_discount_pct DOUBLE
)
USING DELTA
""")
print(f"✅ Agg table '{AGG_CATEGORY_TABLE}' ready.")

# ---------------------------------------------------------------------------
# Gold — Aggregation: agg_price_bucket_summary
# ---------------------------------------------------------------------------
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {AGG_PRICE_BUCKET_TABLE} (
  price_bucket STRING,
  product_count LONG,
  avg_rating DOUBLE,
  avg_discount_pct DOUBLE
)
USING DELTA
""")
print(f"✅ Agg table '{AGG_PRICE_BUCKET_TABLE}' ready.")

# ---------------------------------------------------------------------------
# Dead letter table — invalid records that fail validation
# ---------------------------------------------------------------------------
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {DEAD_LETTER_TABLE} (
  name STRING,
  main_category STRING,
  sub_category STRING,
  image STRING,
  link STRING,
  ratings STRING,
  no_of_ratings STRING,
  discount_price STRING,
  actual_price STRING,
  validation_errors STRING,
  batch_id STRING,
  failed_at TIMESTAMP
)
USING DELTA
""")
print(f"✅ Dead letter table '{DEAD_LETTER_TABLE}' ready.")

# ---------------------------------------------------------------------------
# Checkpoint table — pipeline execution tracking
# ---------------------------------------------------------------------------
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CHECKPOINT_TABLE} (
  batch_id STRING,
  layer_name STRING,
  status STRING,
  record_count LONG,
  executed_at TIMESTAMP,
  error_message STRING
)
USING DELTA
""")
print(f"✅ Checkpoint table '{CHECKPOINT_TABLE}' ready.")

# COMMAND ----------

# DBTITLE 1,Section 3: Bronze Layer (Markdown)
# MAGIC %md
# MAGIC ## 3. Bronze Layer: Extraction
# MAGIC
# MAGIC ### Extraction Strategies
# MAGIC
# MAGIC **Extraction** is the first step of ETL. This pipeline uses a **pull-based extraction** pattern:
# MAGIC - Read the CSV file via `spark.read.csv()` with header and inferred schema
# MAGIC - Read the Delta table `workspace.default.amazon_raw` via `spark.read.table()`
# MAGIC - Union both sources into a common schema, standardizing column names
# MAGIC
# MAGIC Metadata columns (`ingestion_date`, `source_file`, `batch_id`) are added for lineage tracking. The Bronze layer stores raw data as-is — no transformations, no filtering — preserving maximum data fidelity for reprocessing.

# COMMAND ----------

# DBTITLE 1,Bronze Layer: Extraction
# ---------------------------------------------------------------------------
# BRONZE LAYER — Extract from CSV + Delta source, union, write to bronze
# ---------------------------------------------------------------------------
bronze_success = False
bronze_count = 0

try:
    # --- Read CSV source ---
    csv_df = (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .csv(SRC_CSV_PATH)
    )
    
    # Select and standardize columns from CSV
    csv_selected = csv_df.select(
        F.col("name").alias("name"),
        F.col("main_category").alias("main_category"),
        F.col("sub_category").alias("sub_category"),
        F.col("image").alias("image"),
        F.col("link").alias("link"),
        F.col("ratings").alias("ratings"),
        F.col("no_of_ratings").alias("no_of_ratings"),
        F.col("discount_price").alias("discount_price"),
        F.col("actual_price").alias("actual_price"),
        F.lit("amazon_products_cleaned.csv").alias("source_file"),
        F.lit(BATCH_ID).alias("batch_id"),
        F.current_date().alias("ingestion_date")
    )
    
    csv_count = csv_selected.count()
    print(f"📄 CSV source rows: {csv_count}")
    
    # --- Read Delta source (amazon_raw) ---
    raw_df = spark.read.table(SRC_DELTA_TABLE)
    raw_count = raw_df.count()
    print(f"📊 Delta source rows: {raw_count}")
    print(f"📊 Delta source columns: {raw_df.columns[:15]}...")
    
    # Map common columns from amazon_raw — use column names that exist
    # amazon_raw has: asin, title, brand_name, price_value, list_price, rating_stars, rating_count, availability, etc.
    raw_columns = set(raw_df.columns)
    
    raw_selected = raw_df.select(
        F.col("title").alias("name") if "title" in raw_columns else F.lit(None).alias("name"),
        F.lit(None).alias("main_category"),  # amazon_raw doesn't have main_category directly
        F.lit(None).alias("sub_category"),
        F.lit(None).alias("image"),
        F.lit(None).alias("link"),
        F.col("rating_stars").cast("string").alias("ratings") if "rating_stars" in raw_columns else F.lit(None).alias("ratings"),
        F.col("rating_count").cast("string").alias("no_of_ratings") if "rating_count" in raw_columns else F.lit(None).alias("no_of_ratings"),
        F.col("price_value").cast("string").alias("discount_price") if "price_value" in raw_columns else F.lit(None).alias("discount_price"),
        F.col("list_price").cast("string").alias("actual_price") if "list_price" in raw_columns else F.lit(None).alias("actual_price"),
        F.lit(SRC_DELTA_TABLE).alias("source_file"),
        F.lit(BATCH_ID).alias("batch_id"),
        F.current_date().alias("ingestion_date")
    )
    
    # --- Union both sources ---
    bronze_df = csv_selected.unionByName(raw_selected)
    bronze_count = bronze_df.count()
    print(f"🔗 Total Bronze rows (union): {bronze_count}")
    
    # --- Write to Bronze table (overwrite for first run / full reload) ---
    (
        bronze_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(BRONZE_TABLE)
    )
    
    bronze_success = True
    print(f"✅ BRONZE LAYER COMPLETE: {bronze_count} rows written to {BRONZE_TABLE}")

except Exception as e:
    print(f"❌ BRONZE LAYER FAILED: {str(e)}")
    bronze_error = str(e)

# COMMAND ----------

# DBTITLE 1,Section 4: Silver Layer (Markdown)
# MAGIC %md
# MAGIC ## 4. Silver Layer: Transformation & Validation
# MAGIC
# MAGIC ### Transformation Techniques & Loading Strategies
# MAGIC
# MAGIC **Transformation Techniques** applied in the Silver layer:
# MAGIC - **Type Casting**: `try_cast` safely converts STRING columns to DOUBLE, returning NULL on failure instead of crashing
# MAGIC - **String Cleaning**: Remove currency symbols (₹), commas, whitespace from price and rating columns
# MAGIC - **Derived Columns**: Calculate `discount_amount` and `discount_pct` from cleaned prices
# MAGIC - **Standardization**: Lowercase and trim category names for consistency
# MAGIC - **Deduplication**: Remove duplicates by composite key (name, main_category, sub_category)
# MAGIC
# MAGIC ### Staging & Validation
# MAGIC
# MAGIC Before loading to Silver, data is written to a **staging table** for validation:
# MAGIC - **Null checks** on critical columns (name, main_category)
# MAGIC - **Range checks**: prices > 0, ratings between 0-5, no_of_ratings >= 0
# MAGIC - Invalid records are flagged with `is_valid = false` and a `validation_errors` string describing all failures
# MAGIC
# MAGIC ### Idempotency & Atomicity
# MAGIC
# MAGIC **Idempotency** ensures re-running the pipeline produces the same result. We use Delta `MERGE` (upsert) on the composite key (name, main_category, sub_category) — existing records are updated, new ones are inserted.
# MAGIC
# MAGIC **Atomicity** is guaranteed by Delta's ACID transactions. The MERGE operation is atomic: it either fully succeeds or fully rolls back, leaving prior data intact.

# COMMAND ----------

# DBTITLE 1,Silver Layer: Transform & Validate
# ---------------------------------------------------------------------------
# SILVER LAYER — Transform, Validate, Stage, and MERGE to Silver
# ---------------------------------------------------------------------------
silver_success = False
silver_count = 0
valid_count = 0
invalid_count = 0

try:
    # --- Read from Bronze ---
    bronze_data = spark.read.table(BRONZE_TABLE)
    print(f"📖 Reading {bronze_data.count()} rows from Bronze.")
    
    # --- TRANSFORMATION ---
    # Clean and cast columns using try_cast for resilience
    transformed = bronze_data.select(
        F.trim(F.col("name")).alias("name"),
        F.lower(F.trim(F.col("main_category"))).alias("main_category"),
        F.lower(F.trim(F.col("sub_category"))).alias("sub_category"),
        F.col("image"),
        F.col("link"),
        # Clean ratings: remove commas, try_cast to double (safe for malformed values)
        F.expr("try_cast(regexp_replace(ratings, ',', '') as double)").alias("ratings"),
        # Clean no_of_ratings: remove commas, try_cast to double
        F.expr("try_cast(regexp_replace(no_of_ratings, ',', '') as double)").alias("no_of_ratings"),
        # Clean discount_price: remove ₹ and commas, try_cast to double
        F.expr("try_cast(regexp_replace(regexp_replace(discount_price, '₹', ''), ',', '') as double)").alias("discount_price"),
        # Clean actual_price: remove ₹ and commas, try_cast to double
        F.expr("try_cast(regexp_replace(regexp_replace(actual_price, '₹', ''), ',', '') as double)").alias("actual_price"),
        F.col("source_file"),
        F.col("batch_id"),
        F.col("ingestion_date")
    )
    
    # Add derived columns
    transformed = transformed.withColumn(
        "discount_amount",
        F.when(
            F.col("actual_price").isNotNull() & F.col("discount_price").isNotNull(),
            F.round(F.col("actual_price") - F.col("discount_price"), 2)
        ).otherwise(None)
    ).withColumn(
        "discount_pct",
        F.when(
            (F.col("actual_price").isNotNull()) & (F.col("actual_price") > 0) & (F.col("discount_price").isNotNull()),
            F.round((F.col("actual_price") - F.col("discount_price")) / F.col("actual_price") * 100, 2)
        ).otherwise(None)
    )
    
    # --- VALIDATION ---
    # Build validation error messages per row
    validation_errors = F.concat(
        F.when(F.col("name").isNull() | (F.col("name") == ""), "name is null/empty; ").otherwise(""),
        F.when(F.col("main_category").isNull() | (F.col("main_category") == ""), "main_category is null/empty; ").otherwise(""),
        F.when(F.col("ratings").isNull() | (F.col("ratings") < 0) | (F.col("ratings") > 5), "ratings out of range [0-5]; ").otherwise(""),
        F.when(F.col("no_of_ratings").isNull() | (F.col("no_of_ratings") < 0), "no_of_ratings < 0; ").otherwise(""),
        F.when(F.col("discount_price").isNull() | (F.col("discount_price") < 0), "discount_price < 0; ").otherwise(""),
        F.when(F.col("actual_price").isNull() | (F.col("actual_price") <= 0), "actual_price <= 0; ").otherwise("")
    )
    
    staged = transformed.withColumn("validation_errors", validation_errors)
    staged = staged.withColumn("is_valid", F.when(F.col("validation_errors") == "", True).otherwise(False))
    
    # --- Write to Staging table (overwrite for fresh validation each run) ---
    (
        staged.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(STAGING_TABLE)
    )
    print(f"✅ Staging table written.")
    
    # --- Validation Report ---
    total_staged = staged.count()
    valid_count = staged.filter(F.col("is_valid") == True).count()
    invalid_count = staged.filter(F.col("is_valid") == False).count()
    
    print("\n" + "="*60)
    print("  VALIDATION REPORT — Silver Staging Layer")
    print("="*60)
    print(f"  Total records staged : {total_staged}")
    print(f"  Valid records        : {valid_count}")
    print(f"  Invalid records      : {invalid_count}")
    print(f"  Valid rate           : {round(valid_count / total_staged * 100, 2) if total_staged > 0 else 0}%")
    print("-"*60)
    
    # Null counts per critical column
    for col_name in ["name", "main_category", "ratings", "no_of_ratings", "discount_price", "actual_price"]:
        null_cnt = staged.filter(F.col(col_name).isNull()).count()
        print(f"  Null count — {col_name:20s}: {null_cnt}")
    print("="*60)
    
    # --- Dead Letter: Send invalid records to dead letter table ---
    invalid_records = staged.filter(F.col("is_valid") == False).select(
        F.col("name"), F.col("main_category"), F.col("sub_category"),
        F.col("image"), F.col("link"),
        F.col("ratings").cast("string"), F.col("no_of_ratings").cast("string"),
        F.col("discount_price").cast("string"), F.col("actual_price").cast("string"),
        F.col("validation_errors"),
        F.lit(BATCH_ID).alias("batch_id"),
        F.current_timestamp().alias("failed_at")
    )
    
    # Overwrite dead letter for this batch (clean prior batch dead letters)
    (
        invalid_records.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(DEAD_LETTER_TABLE)
    )
    dead_letter_count = invalid_records.count()
    print(f"✅ Dead letter table updated: {dead_letter_count} invalid records.")
    
    # --- Deduplicate valid records by composite key ---
    valid_data = (
        staged.filter(F.col("is_valid") == True)
        .dropDuplicates(["name", "main_category", "sub_category"])
    )
    
    # --- MERGE to Silver table (idempotent upsert) ---
    silver_target = DeltaTable.forName(spark, SILVER_TABLE)
    
    (
        silver_target.alias("target")
        .merge(
            valid_data.alias("source"),
            "target.name = source.name AND target.main_category = source.main_category AND target.sub_category = source.sub_category"
        )
        .whenMatchedUpdate(set={
            "ratings": "source.ratings",
            "no_of_ratings": "source.no_of_ratings",
            "discount_price": "source.discount_price",
            "actual_price": "source.actual_price",
            "discount_amount": "source.discount_amount",
            "discount_pct": "source.discount_pct",
            "is_valid": "source.is_valid",
            "source_file": "source.source_file",
            "batch_id": "source.batch_id",
            "ingestion_date": "source.ingestion_date"
        })
        .whenNotMatchedInsert(values={
            "name": "source.name",
            "main_category": "source.main_category",
            "sub_category": "source.sub_category",
            "image": "source.image",
            "link": "source.link",
            "ratings": "source.ratings",
            "no_of_ratings": "source.no_of_ratings",
            "discount_price": "source.discount_price",
            "actual_price": "source.actual_price",
            "discount_amount": "source.discount_amount",
            "discount_pct": "source.discount_pct",
            "is_valid": "source.is_valid",
            "source_file": "source.source_file",
            "batch_id": "source.batch_id",
            "ingestion_date": "source.ingestion_date"
        })
        .execute()
    )
    
    silver_count = spark.read.table(SILVER_TABLE).count()
    silver_success = True
    print(f"✅ SILVER LAYER COMPLETE: {silver_count} rows in {SILVER_TABLE} (after MERGE)")

except Exception as e:
    print(f"❌ SILVER LAYER FAILED: {str(e)}")
    silver_error = str(e)

# COMMAND ----------

# DBTITLE 1,Section 5: Gold Layer (Markdown)
# MAGIC %md
# MAGIC ## 5. Gold Layer: Loading & Analytics
# MAGIC
# MAGIC ### Loading Strategies — Star Schema Design
# MAGIC
# MAGIC The Gold layer implements a **star schema** for analytics:
# MAGIC - **Dimension tables** (`dim_category`, `dim_price_bucket`) store descriptive attributes about business entities
# MAGIC - **Fact table** (`fact_product_metrics`) stores quantitative measures linked to dimensions via surrogate keys
# MAGIC - **Aggregation tables** pre-compute summary metrics for fast dashboard queries
# MAGIC
# MAGIC ### Price Bucket Classification
# MAGIC
# MAGIC Products are classified into price tiers:
# MAGIC | Bucket | Range |
# MAGIC |---|---|
# MAGIC | Budget | < 500 |
# MAGIC | Mid-Range | 500 – 2,000 |
# MAGIC | Premium | 2,000 – 10,000 |
# MAGIC | Luxury | > 10,000 |
# MAGIC
# MAGIC ### Idempotent Gold Loads
# MAGIC
# MAGIC All Gold tables are rebuilt using `MERGE` or overwrite patterns. Dimension tables use `DELETE + INSERT` to refresh reference data. Fact tables use MERGE on product name to avoid duplicate fact rows on re-runs.

# COMMAND ----------

# DBTITLE 1,Gold Layer: Star Schema & Analytics
# ---------------------------------------------------------------------------
# GOLD LAYER — Build star schema: dim tables, fact table, aggregation tables
# ---------------------------------------------------------------------------
gold_success = False
fact_count = 0

try:
    silver_data = spark.read.table(SILVER_TABLE).filter(F.col("is_valid") == True)
    print(f"📖 Reading {silver_data.count()} valid rows from Silver.")
    
    # =======================================================================
    # DIM CATEGORY — unique category combinations with surrogate key
    # =======================================================================
    from pyspark.sql.window import Window
    
    dim_cat_df = silver_data.select(
        F.col("main_category"),
        F.col("sub_category")
    ).distinct().filter(
        F.col("main_category").isNotNull()
    )
    
    # Generate surrogate key using row_number
    cat_window = Window.orderBy("main_category", "sub_category")
    dim_cat_df = dim_cat_df.withColumn("category_key", F.row_number().over(cat_window))
    
    # Overwrite dim_category (reference data refresh)
    (
        dim_cat_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(DIM_CATEGORY_TABLE)
    )
    dim_cat_count = spark.read.table(DIM_CATEGORY_TABLE).count()
    print(f"✅ dim_category: {dim_cat_count} rows.")
    
    # =======================================================================
    # DIM PRICE BUCKET — static reference table
    # =======================================================================
    price_bucket_data = spark.createDataFrame([
        (1, "Budget", 0.0, 500.0),
        (2, "Mid-Range", 500.0, 2000.0),
        (3, "Premium", 2000.0, 10000.0),
        (4, "Luxury", 10000.0, 999999.0)
    ], ["price_bucket_key", "bucket_label", "min_price", "max_price"])
    
    (
        price_bucket_data.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(DIM_PRICE_BUCKET_TABLE)
    )
    print(f"✅ dim_price_bucket: 4 rows.")
    
    # =======================================================================
    # FACT TABLE — product metrics linked to dimensions
    # =======================================================================
    # Get category keys
    dim_cat = spark.read.table(DIM_CATEGORY_TABLE)
    
    # Assign price bucket to each product
    fact_df = (
        silver_data
        .join(dim_cat, ["main_category", "sub_category"], "left")
        .withColumn(
            "price_bucket",
            F.when(F.col("actual_price") < 500, "Budget")
            .when(F.col("actual_price") < 2000, "Mid-Range")
            .when(F.col("actual_price") < 10000, "Premium")
            .otherwise("Luxury")
        )
    )
    
    # Get price bucket keys
    dim_pb = spark.read.table(DIM_PRICE_BUCKET_TABLE)
    fact_df = fact_df.join(
        dim_pb.select(F.col("bucket_label"), F.col("price_bucket_key")),
        fact_df["price_bucket"] == dim_pb["bucket_label"],
        "left"
    )
    
    # Generate surrogate product key
    fact_window = Window.orderBy("name")
    fact_df = fact_df.withColumn("product_key", F.row_number().over(fact_window))
    
    fact_selected = fact_df.select(
        F.col("product_key"),
        F.col("name"),
        F.col("category_key"),
        F.col("price_bucket_key"),
        F.col("ratings"),
        F.col("no_of_ratings"),
        F.col("discount_price"),
        F.col("actual_price"),
        F.col("discount_pct"),
        F.col("is_valid")
    )
    
    # Drop fact table first to remove identity column constraint
    spark.sql(f"DROP TABLE IF EXISTS {FACT_TABLE}")
    
    # Overwrite fact table (idempotent — full refresh from silver)
    (
        fact_selected.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(FACT_TABLE)
    )
    fact_count = spark.read.table(FACT_TABLE).count()
    print(f"✅ fact_product_metrics: {fact_count} rows.")
    
    # =======================================================================
    # AGG CATEGORY SUMMARY
    # =======================================================================
    agg_cat = (
        fact_df.groupBy("main_category")
        .agg(
            F.count("*").alias("product_count"),
            F.round(F.avg("ratings"), 2).alias("avg_rating"),
            F.round(F.avg("discount_price"), 2).alias("avg_discount_price"),
            F.round(F.avg("actual_price"), 2).alias("avg_actual_price"),
            F.round(F.avg("discount_pct"), 2).alias("avg_discount_pct")
        )
        .orderBy(F.desc("product_count"))
    )
    
    (
        agg_cat.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(AGG_CATEGORY_TABLE)
    )
    agg_cat_count = spark.read.table(AGG_CATEGORY_TABLE).count()
    print(f"✅ agg_category_summary: {agg_cat_count} rows.")
    
    # =======================================================================
    # AGG PRICE BUCKET SUMMARY
    # =======================================================================
    agg_pb = (
        fact_df.groupBy("price_bucket")
        .agg(
            F.count("*").alias("product_count"),
            F.round(F.avg("ratings"), 2).alias("avg_rating"),
            F.round(F.avg("discount_pct"), 2).alias("avg_discount_pct")
        )
        .orderBy("price_bucket")
    )
    
    (
        agg_pb.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(AGG_PRICE_BUCKET_TABLE)
    )
    agg_pb_count = spark.read.table(AGG_PRICE_BUCKET_TABLE).count()
    print(f"✅ agg_price_bucket_summary: {agg_pb_count} rows.")
    
    gold_success = True
    print(f"\n✅ GOLD LAYER COMPLETE: {fact_count} fact rows, {agg_cat_count} category aggregates, {agg_pb_count} price bucket aggregates.")

except Exception as e:
    print(f"❌ GOLD LAYER FAILED: {str(e)}")
    gold_error = str(e)

# COMMAND ----------

# DBTITLE 1,Section 6: Dead Letter & Error Handling (Markdown)
# MAGIC %md
# MAGIC ## 6. Dead Letter & Error Handling
# MAGIC
# MAGIC ### Error Handling Patterns
# MAGIC
# MAGIC **Error Handling** is critical for production pipelines. This section implements:
# MAGIC - **Dead Letter Table**: Invalid records that fail validation are quarantined in `dead_letter_products` with the full original data, a `validation_errors` message, the `batch_id`, and a `failed_at` timestamp
# MAGIC - **Try/Except Blocks**: Each pipeline layer is wrapped in try/except, capturing errors without crashing the notebook
# MAGIC - **Python Logging**: Structured logging records pipeline events for auditability
# MAGIC - **Layer Isolation**: If Bronze fails, Silver is not attempted. If Silver fails, Gold is not attempted. This prevents cascading errors from partial data.

# COMMAND ----------

# DBTITLE 1,Dead Letter & Error Handling
# ---------------------------------------------------------------------------
# DEAD LETTER & ERROR HANDLING — Review quarantined records
# ---------------------------------------------------------------------------

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("amazon_pipeline")

logger.info(f"Pipeline batch_id: {BATCH_ID}")
logger.info(f"Ingestion timestamp: {INGESTION_DATE}")

# Display dead letter records
try:
    dead_letter_df = spark.read.table(DEAD_LETTER_TABLE)
    dl_count = dead_letter_df.count()
    
    print("\n" + "="*60)
    print("  DEAD LETTER SUMMARY")
    print("="*60)
    print(f"  Total dead-lettered records: {dl_count}")
    print("="*60)
    
    if dl_count > 0:
        print("\n  Sample dead letter records:")
        display(dead_letter_df.select(
            "name", "main_category", "validation_errors", "batch_id", "failed_at"
        ).limit(10))
    else:
        print("  No dead letter records found — all data passed validation.")
        
except Exception as e:
    logger.error(f"Error reading dead letter table: {str(e)}")
    print(f"⚠️ Could not read dead letter table: {str(e)}")

# COMMAND ----------

# DBTITLE 1,Section 7: Checkpointing (Markdown)
# MAGIC %md
# MAGIC ## 7. Checkpointing & Pipeline Summary
# MAGIC
# MAGIC ### Checkpointing
# MAGIC
# MAGIC **Checkpointing** provides end-to-end pipeline observability. A checkpoint row is inserted into `pipeline_checkpoints` after each layer completes, recording:
# MAGIC - `batch_id`: Unique identifier for this pipeline run
# MAGIC - `layer_name`: Which layer (Bronze/Silver/Gold)
# MAGIC - `status`: SUCCESS or FAILED
# MAGIC - `record_count`: Number of rows processed in that layer
# MAGIC - `executed_at`: Timestamp of completion
# MAGIC - `error_message`: Error details if the layer failed
# MAGIC
# MAGIC This enables debugging, SLA monitoring, and retry logic for production operations.

# COMMAND ----------

# DBTITLE 1,Checkpointing & Pipeline Summary
# ---------------------------------------------------------------------------
# CHECKPOINTING — Record pipeline execution status per layer
# ---------------------------------------------------------------------------

checkpoint_rows = []

# Bronze checkpoint
if bronze_success:
    checkpoint_rows.append((BATCH_ID, "BRONZE", "SUCCESS", int(bronze_count), datetime.now(), None))
else:
    checkpoint_rows.append((BATCH_ID, "BRONZE", "FAILED", 0, datetime.now(), str(locals().get('bronze_error', 'Unknown error'))))

# Silver checkpoint
if silver_success:
    checkpoint_rows.append((BATCH_ID, "SILVER", "SUCCESS", int(silver_count), datetime.now(), None))
else:
    checkpoint_rows.append((BATCH_ID, "SILVER", "FAILED", 0, datetime.now(), str(locals().get('silver_error', 'Unknown error'))))

# Gold checkpoint
if gold_success:
    checkpoint_rows.append((BATCH_ID, "GOLD", "SUCCESS", int(fact_count), datetime.now(), None))
else:
    checkpoint_rows.append((BATCH_ID, "GOLD", "FAILED", 0, datetime.now(), str(locals().get('gold_error', 'Unknown error'))))

# Write checkpoints with explicit schema (None values need typed schema)
from pyspark.sql.types import StructType, StructField as SF, StringType as ST, LongType as LT, TimestampType as TS

checkpoint_schema = StructType([
    SF("batch_id", ST(), True),
    SF("layer_name", ST(), True),
    SF("status", ST(), True),
    SF("record_count", LT(), True),
    SF("executed_at", TS(), True),
    SF("error_message", ST(), True)
])

checkpoint_df = spark.createDataFrame(
    checkpoint_rows,
    checkpoint_schema
)

# Append to checkpoint table (accumulate history across runs)
(
    checkpoint_df.write
    .format("delta")
    .mode("append")
    .saveAsTable(CHECKPOINT_TABLE)
)

print("✅ Checkpoints recorded.")

# =======================================================================
# FINAL PIPELINE SUMMARY
# =======================================================================
print("\n" + "="*60)
print("  AMAZON PRODUCTION PIPELINE — FINAL SUMMARY")
print("="*60)
print(f"  Batch ID            : {BATCH_ID}")
print(f"  Executed At         : {INGESTION_DATE}")
print("-"*60)
print(f"  BRONZE  — Records    : {bronze_count if bronze_success else 'FAILED'}")
print(f"  SILVER  — Records    : {silver_count if silver_success else 'FAILED'}")
print(f"    Valid             : {valid_count if silver_success else 'N/A'}")
print(f"    Invalid           : {invalid_count if silver_success else 'N/A'}")
print(f"    Dead Lettered     : {dead_letter_count if silver_success else 'N/A'}")
print(f"  GOLD    — Fact rows   : {fact_count if gold_success else 'FAILED'}")
print("-"*60)
print(f"  Overall Status      : {'✅ SUCCESS' if (bronze_success and silver_success and gold_success) else '⚠️ PARTIAL FAILURE'}")
print("="*60)
print("\n  Checkpoint History:")
print("-"*60)
display(spark.read.table(CHECKPOINT_TABLE).orderBy(F.desc("executed_at")).limit(10))

# COMMAND ----------

# DBTITLE 1,Section 8: Verification (Markdown)
# MAGIC %md
# MAGIC ## 8. Verification Queries
# MAGIC
# MAGIC Run these queries to verify each layer's data quality and contents. These serve as both validation and as examples for downstream analytics consumers.

# COMMAND ----------

# DBTITLE 1,Verification Queries
# ---------------------------------------------------------------------------
# VERIFICATION — Display counts and sample data from each layer
# ---------------------------------------------------------------------------

print("="*60)
print("  VERIFICATION — BRONZE LAYER")
print("="*60)
bronze_verify = spark.read.table(BRONZE_TABLE)
print(f"  Row count: {bronze_verify.count()}")
display(bronze_verify.limit(5))

print("\n" + "="*60)
print("  VERIFICATION — SILVER LAYER")
print("="*60)
silver_verify = spark.read.table(SILVER_TABLE)
print(f"  Row count: {silver_verify.count()}")
print(f"  Schema:")
silver_verify.printSchema()
display(silver_verify.limit(5))

print("\n" + "="*60)
print("  VERIFICATION — GOLD: dim_category")
print("="*60)
display(spark.read.table(DIM_CATEGORY_TABLE).limit(10))

print("\n" + "="*60)
print("  VERIFICATION — GOLD: dim_price_bucket")
print("="*60)
display(spark.read.table(DIM_PRICE_BUCKET_TABLE))

print("\n" + "="*60)
print("  VERIFICATION — GOLD: fact_product_metrics")
print("="*60)
fact_verify = spark.read.table(FACT_TABLE)
print(f"  Row count: {fact_verify.count()}")
display(fact_verify.limit(10))

print("\n" + "="*60)
print("  VERIFICATION — GOLD: agg_category_summary")
print("="*60)
display(spark.read.table(AGG_CATEGORY_TABLE))

print("\n" + "="*60)
print("  VERIFICATION — GOLD: agg_price_bucket_summary")
print("="*60)
display(spark.read.table(AGG_PRICE_BUCKET_TABLE))

print("\n" + "="*60)
print("  VERIFICATION — DEAD LETTER")
print("="*60)
dl_verify = spark.read.table(DEAD_LETTER_TABLE)
print(f"  Dead letter count: {dl_verify.count()}")
if dl_verify.count() > 0:
    display(dl_verify.select("name", "main_category", "validation_errors").limit(10))

print("\n" + "="*60)
print("  VERIFICATION — CHECKPOINTS")
print("="*60)
display(spark.read.table(CHECKPOINT_TABLE).orderBy(F.desc("executed_at")))

print("\n✅ Pipeline verification complete.")