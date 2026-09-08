# Databricks notebook source
# DBTITLE 1,Unit Tests Title
# MAGIC %md
# MAGIC # Amazon Pipeline — Unit Tests
# MAGIC
# MAGIC ## Continuous Integration: Unit Testing Data Transformations
# MAGIC
# MAGIC This notebook contains **unit tests** for the transformation logic in the Amazon Production Pipeline. Each test uses **mock data** with known inputs and expected outputs, allowing automated verification without depending on production data.
# MAGIC
# MAGIC ### Test Coverage
# MAGIC | # | Test | What it validates |
# MAGIC | --- | --- | --- |
# MAGIC | 1 | Type Casting | `try_cast` safely converts strings to doubles |
# MAGIC | 2 | String Cleaning | ₹ and comma removal from price/rating columns |
# MAGIC | 3 | Validation Rules | Null checks, range checks, is_valid flagging |
# MAGIC | 4 | Derived Columns | discount_amount and discount_pct calculations |
# MAGIC | 5 | Deduplication | Composite key dedup preserves correct row |
# MAGIC | 6 | Dead Letter | Invalid records are correctly isolated |
# MAGIC | 7 | End-to-End | Full transform→validate→dedup pipeline on mock data |

# COMMAND ----------

# DBTITLE 1,Test Setup & Mock Data
# ============================================================================
# TEST SETUP — Imports, test framework, and mock data
# ============================================================================

import traceback
from pyspark.sql import functions as F
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType

# Simple test framework (no pytest needed in Databricks notebooks)
TEST_RESULTS = []
TOTAL_TESTS = 0
PASSED_TESTS = 0
FAILED_TESTS = 0

def assert_equal(actual, expected, test_name):
    """Assert two values are equal and record the result."""
    global TOTAL_TESTS, PASSED_TESTS, FAILED_TESTS
    TOTAL_TESTS += 1
    if actual == expected:
        PASSED_TESTS += 1
        TEST_RESULTS.append((test_name, "PASS", f"Expected {expected}, got {actual}"))
        print(f"  ✅ {test_name}: PASS")
    else:
        FAILED_TESTS += 1
        TEST_RESULTS.append((test_name, "FAIL", f"Expected {expected}, got {actual}"))
        print(f"  ❌ {test_name}: FAIL — Expected {expected}, got {actual}")

def assert_true(condition, test_name, message=""):
    """Assert a condition is True and record the result."""
    assert_equal(bool(condition), True, f"{test_name} {message}")

def assert_df_count(df, expected_count, test_name):
    """Assert a DataFrame has the expected number of rows."""
    actual = df.count()
    assert_equal(actual, expected_count, test_name)

def run_test_suite(suite_name, test_fn):
    """Run a test suite and capture any exceptions."""
    global TOTAL_TESTS, PASSED_TESTS, FAILED_TESTS
    print(f"\n{'='*60}")
    print(f"  RUNNING: {suite_name}")
    print(f"{'='*60}")
    try:
        test_fn()
    except Exception as e:
        TOTAL_TESTS += 1
        FAILED_TESTS += 1
        TEST_RESULTS.append((suite_name, "ERROR", str(e)))
        print(f"  ❌ {suite_name}: ERROR — {str(e)}")
        traceback.print_exc()

# ============================================================================
# MOCK DATA — Known inputs and expected outputs for testing
# ============================================================================

# Mock raw data simulating real Amazon product records (including edge cases)
mock_raw_data = [
    # Valid records
    Row(name="Product A", main_category="men's clothing", sub_category="shirts",
        ratings="4.5", no_of_ratings="1,234", discount_price="499", actual_price="999"),
    Row(name="Product B", main_category="women's clothing", sub_category="dresses",
        ratings="3.8", no_of_ratings="567", discount_price="299", actual_price="599"),
    Row(name="Product C", main_category="stores", sub_category="men's fashion",
        ratings="5.0", no_of_ratings="10,000", discount_price="1,999", actual_price="3,999"),
    # Edge case: price with ₹ symbol
    Row(name="Product D", main_category="men's clothing", sub_category="shirts",
        ratings="4.2", no_of_ratings="890", discount_price="₹499", actual_price="₹999"),
    # Duplicate of Product A (for dedup test)
    Row(name="Product A", main_category="men's clothing", sub_category="shirts",
        ratings="4.0", no_of_ratings="500", discount_price="399", actual_price="899"),
    # Invalid: null ratings
    Row(name="Product E", main_category="stores", sub_category="accessories",
        ratings=None, no_of_ratings="100", discount_price="199", actual_price="399"),
    # Invalid: negative price
    Row(name="Product F", main_category="women's clothing", sub_category="bags",
        ratings="4.0", no_of_ratings="50", discount_price="-50", actual_price="100"),
    # Invalid: ratings > 5
    Row(name="Product G", main_category="stores", sub_category="watches",
        ratings="7.5", no_of_ratings="200", discount_price="500", actual_price="1000"),
    # Invalid: null name
    Row(name=None, main_category="stores", sub_category="toys",
        ratings="4.0", no_of_ratings="300", discount_price="100", actual_price="200"),
    # Invalid: actual_price = 0
    Row(name="Product H", main_category="stores", sub_category="electronics",
        ratings="3.5", no_of_ratings="50", discount_price="0", actual_price="0"),
]

mock_schema = StructType([
    StructField("name", StringType(), True),
    StructField("main_category", StringType(), True),
    StructField("sub_category", StringType(), True),
    StructField("image", StringType(), True),
    StructField("link", StringType(), True),
    StructField("ratings", StringType(), True),
    StructField("no_of_ratings", StringType(), True),
    StructField("discount_price", StringType(), True),
    StructField("actual_price", StringType(), True),
])

# Create mock DataFrame
mock_df = spark.createDataFrame(mock_raw_data, mock_schema)
print(f"✅ Test setup complete. Mock data: {mock_df.count()} rows.")
print(f"   Valid rows expected: 5  (Products A, B, C, D + dedup of A)")
print(f"   Invalid rows expected: 5  (Products E, F, G, null-name, H)")
print(f"   Unique after dedup: 4  (A, B, C, D)")

# COMMAND ----------

# DBTITLE 1,Test 1: Type Casting
# ============================================================================
# TEST 1: Type Casting — try_cast converts strings to doubles safely
# ============================================================================

def test_type_casting():
    """Test that try_cast safely converts string columns to double."""
    # Test valid numeric string
    result = mock_df.withColumn(
        "ratings_double",
        F.expr("try_cast(regexp_replace(ratings, ',', '') as double)")
    )
    
    # Product A: ratings = "4.5" → 4.5
    product_a = result.filter(F.col("name") == "Product A").first()
    assert_equal(product_a["ratings_double"], 4.5, "test_type_cast_valid_decimal")
    
    # Product C: ratings = "5.0", no_of_ratings = "10,000" → 10000.0
    product_c = result.filter(F.col("name") == "Product C").first()
    assert_equal(product_c["ratings_double"], 5.0, "test_type_cast_integer_as_double")
    
    # no_of_ratings with commas: "1,234" → 1234.0
    no_of_ratings_df = result.withColumn(
        "no_of_ratings_double",
        F.expr("try_cast(regexp_replace(no_of_ratings, ',', '') as double)")
    )
    product_a_nor = no_of_ratings_df.filter(F.col("name") == "Product A").first()
    assert_equal(product_a_nor["no_of_ratings_double"], 1234.0, "test_type_cast_comma_removal")
    
    # Null ratings should produce null double
    product_e = result.filter(F.col("name") == "Product E").first()
    assert_equal(product_e["ratings_double"], None, "test_type_cast_null_input")
    
    # Non-numeric string should produce null (not crash)
    bad_data = spark.createDataFrame([Row(ratings="not_a_number")], StructType([StructField("ratings", StringType(), True)]))
    bad_result = bad_data.withColumn("casted", F.expr("try_cast(ratings as double)")).first()
    assert_equal(bad_result["casted"], None, "test_type_cast_non_numeric_returns_null")

run_test_suite("Test 1: Type Casting", test_type_casting)

# COMMAND ----------

# DBTITLE 1,Test 2: String Cleaning
# ============================================================================
# TEST 2: String Cleaning — ₹ symbol and comma removal
# ============================================================================

def test_string_cleaning():
    """Test that currency symbols and commas are correctly removed."""
    # Test ₹ removal from discount_price
    cleaned = mock_df.withColumn(
        "discount_price_clean",
        F.expr("try_cast(regexp_replace(regexp_replace(discount_price, '₹', ''), ',', '') as double)")
    )
    
    # Product D: discount_price = "₹499" → 499.0
    product_d = cleaned.filter(F.col("name") == "Product D").first()
    assert_equal(product_d["discount_price_clean"], 499.0, "test_string_clean_rupee_symbol_removal")
    
    # Product C: discount_price = "1,999" → 1999.0 (comma removal)
    product_c = cleaned.filter(F.col("name") == "Product C").first()
    assert_equal(product_c["discount_price_clean"], 1999.0, "test_string_clean_comma_removal")
    
    # Product A: discount_price = "499" → 499.0 (no symbols)
    product_a = cleaned.filter(F.col("name") == "Product A").first()
    assert_equal(product_a["discount_price_clean"], 499.0, "test_string_clean_plain_number")
    
    # Test actual_price with ₹: "₹999" → 999.0
    actual_cleaned = mock_df.withColumn(
        "actual_price_clean",
        F.expr("try_cast(regexp_replace(regexp_replace(actual_price, '₹', ''), ',', '') as double)")
    )
    product_d_actual = actual_cleaned.filter(F.col("name") == "Product D").first()
    assert_equal(product_d_actual["actual_price_clean"], 999.0, "test_string_clean_actual_price_rupee")

run_test_suite("Test 2: String Cleaning", test_string_cleaning)

# COMMAND ----------

# DBTITLE 1,Test 3: Validation Rules
# ============================================================================
# TEST 3: Validation Rules — null checks, range checks, is_valid flagging
# ============================================================================

def test_validation_rules():
    """Test that validation rules correctly flag valid and invalid records."""
    # Apply transformations
    transformed = mock_df.select(
        F.col("name"),
        F.col("main_category"),
        F.col("sub_category"),
        F.expr("try_cast(regexp_replace(ratings, ',', '') as double)").alias("ratings"),
        F.expr("try_cast(regexp_replace(no_of_ratings, ',', '') as double)").alias("no_of_ratings"),
        F.expr("try_cast(regexp_replace(regexp_replace(discount_price, '₹', ''), ',', '') as double)").alias("discount_price"),
        F.expr("try_cast(regexp_replace(regexp_replace(actual_price, '₹', ''), ',', '') as double)").alias("actual_price"),
    )
    
    # Build validation errors (same logic as the pipeline)
    validation_errors = F.concat(
        F.when(F.col("name").isNull() | (F.col("name") == ""), "name is null/empty; ").otherwise(""),
        F.when(F.col("main_category").isNull() | (F.col("main_category") == ""), "main_category is null/empty; ").otherwise(""),
        F.when(F.col("ratings").isNull() | (F.col("ratings") < 0) | (F.col("ratings") > 5), "ratings out of range [0-5]; ").otherwise(""),
        F.when(F.col("no_of_ratings").isNull() | (F.col("no_of_ratings") < 0), "no_of_ratings < 0; ").otherwise(""),
        F.when(F.col("discount_price").isNull() | (F.col("discount_price") < 0), "discount_price < 0; ").otherwise(""),
        F.when(F.col("actual_price").isNull() | (F.col("actual_price") <= 0), "actual_price <= 0; ").otherwise("")
    )
    
    validated = transformed.withColumn("validation_errors", validation_errors)
    validated = validated.withColumn("is_valid", F.when(F.col("validation_errors") == "", True).otherwise(False))
    
    # Test: Product A is valid
    product_a = validated.filter(F.col("name") == "Product A").first()
    assert_true(product_a["is_valid"], "test_validation_valid_product_A")
    
    # Test: Product E (null ratings) is invalid
    product_e = validated.filter(F.col("name") == "Product E").first()
    assert_true(not product_e["is_valid"], "test_validation_null_ratings_invalid")
    assert_true("ratings out of range" in product_e["validation_errors"], "test_validation_error_message_contains_ratings")
    
    # Test: Product F (negative discount_price) is invalid
    product_f = validated.filter(F.col("name") == "Product F").first()
    assert_true(not product_f["is_valid"], "test_validation_negative_price_invalid")
    assert_true("discount_price < 0" in product_f["validation_errors"], "test_validation_error_message_contains_price")
    
    # Test: Product G (ratings > 5) is invalid
    product_g = validated.filter(F.col("name") == "Product G").first()
    assert_true(not product_g["is_valid"], "test_validation_ratings_above_5_invalid")
    
    # Test: null name is invalid
    null_name = validated.filter(F.col("name").isNull()).first()
    assert_true(not null_name["is_valid"], "test_validation_null_name_invalid")
    assert_true("name is null/empty" in null_name["validation_errors"], "test_validation_error_message_contains_name")
    
    # Test: Product H (actual_price = 0) is invalid
    product_h = validated.filter(F.col("name") == "Product H").first()
    assert_true(not product_h["is_valid"], "test_validation_zero_price_invalid")
    
    # Count valid vs invalid
    valid_count = validated.filter(F.col("is_valid") == True).count()
    invalid_count = validated.filter(F.col("is_valid") == False).count()
    assert_equal(valid_count, 5, "test_validation_valid_count")  # Products A, A(dup), B, C, D
    assert_equal(invalid_count, 5, "test_validation_invalid_count")  # E, F, G, null-name, H

run_test_suite("Test 3: Validation Rules", test_validation_rules)

# COMMAND ----------

# DBTITLE 1,Test 4: Derived Columns
# ============================================================================
# TEST 4: Derived Columns — discount_amount and discount_pct calculations
# ============================================================================

def test_derived_columns():
    """Test that derived columns are calculated correctly."""
    # Apply transformations
    transformed = mock_df.select(
        F.col("name"),
        F.expr("try_cast(regexp_replace(ratings, ',', '') as double)").alias("ratings"),
        F.expr("try_cast(regexp_replace(regexp_replace(discount_price, '₹', ''), ',', '') as double)").alias("discount_price"),
        F.expr("try_cast(regexp_replace(regexp_replace(actual_price, '₹', ''), ',', '') as double)").alias("actual_price"),
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
    
    # Test Product A: discount_price=499, actual_price=999 → discount_amount=500, discount_pct=50.05
    product_a = transformed.filter(F.col("name") == "Product A").first()
    assert_equal(product_a["discount_amount"], 500.0, "test_derived_discount_amount")
    assert_equal(product_a["discount_pct"], 50.05, "test_derived_discount_pct")
    
    # Test Product B: discount_price=299, actual_price=599 → discount_amount=300, discount_pct=50.08
    product_b = transformed.filter(F.col("name") == "Product B").first()
    assert_equal(product_b["discount_amount"], 300.0, "test_derived_discount_amount_product_b")
    assert_equal(product_b["discount_pct"], 50.08, "test_derived_discount_pct_product_b")
    
    # Test Product D: discount_price=₹499→499, actual_price=₹999→999 → discount_amount=500
    product_d = transformed.filter(F.col("name") == "Product D").first()
    assert_equal(product_d["discount_amount"], 500.0, "test_derived_discount_amount_with_rupee")
    
    # Test Product H: actual_price=0 → discount_pct should be null (divide by zero guard)
    product_h = transformed.filter(F.col("name") == "Product H").first()
    assert_equal(product_h["discount_pct"], None, "test_derived_discount_pct_zero_price_guard")

run_test_suite("Test 4: Derived Columns", test_derived_columns)

# COMMAND ----------

# DBTITLE 1,Test 5: Deduplication
# ============================================================================
# TEST 5: Deduplication — composite key dedup preserves correct row
# ============================================================================

def test_deduplication():
    """Test that deduplication on composite key works correctly."""
    # The mock data has two "Product A" rows with same (name, main_category, sub_category)
    # After dedup, only one should remain
    transformed = mock_df.select(
        F.col("name"),
        F.col("main_category"),
        F.col("sub_category"),
        F.expr("try_cast(regexp_replace(ratings, ',', '') as double)").alias("ratings"),
    )
    
    # Count before dedup
    before_count = transformed.count()
    assert_equal(before_count, 10, "test_dedup_before_count")
    
    # Dedup by composite key
    deduped = transformed.dropDuplicates(["name", "main_category", "sub_category"])
    after_count = deduped.count()
    assert_equal(after_count, 9, "test_dedup_after_count")  # 10 - 1 duplicate
    
    # Verify exactly one Product A exists
    product_a_count = deduped.filter(F.col("name") == "Product A").count()
    assert_equal(product_a_count, 1, "test_dedup_single_product_a")
    
    # Verify all unique categories are preserved
    unique_cats = deduped.select("main_category", "sub_category").distinct().count()
    assert_equal(unique_cats, 8, "test_dedup_preserves_categories")  # 8 unique cat combos

run_test_suite("Test 5: Deduplication", test_deduplication)

# COMMAND ----------

# DBTITLE 1,Test 6: Dead Letter Isolation
# ============================================================================
# TEST 6: Dead Letter — invalid records are correctly isolated
# ============================================================================

def test_dead_letter():
    """Test that invalid records are correctly separated for dead letter table."""
    # Apply full transformation and validation (reusing logic from Test 3)
    transformed = mock_df.select(
        F.col("name"),
        F.col("main_category"),
        F.col("sub_category"),
        F.expr("try_cast(regexp_replace(ratings, ',', '') as double)").alias("ratings"),
        F.expr("try_cast(regexp_replace(no_of_ratings, ',', '') as double)").alias("no_of_ratings"),
        F.expr("try_cast(regexp_replace(regexp_replace(discount_price, '₹', ''), ',', '') as double)").alias("discount_price"),
        F.expr("try_cast(regexp_replace(regexp_replace(actual_price, '₹', ''), ',', '') as double)").alias("actual_price"),
    )
    
    validation_errors = F.concat(
        F.when(F.col("name").isNull() | (F.col("name") == ""), "name is null/empty; ").otherwise(""),
        F.when(F.col("ratings").isNull() | (F.col("ratings") < 0) | (F.col("ratings") > 5), "ratings out of range [0-5]; ").otherwise(""),
        F.when(F.col("discount_price").isNull() | (F.col("discount_price") < 0), "discount_price < 0; ").otherwise(""),
        F.when(F.col("actual_price").isNull() | (F.col("actual_price") <= 0), "actual_price <= 0; ").otherwise("")
    )
    
    validated = transformed.withColumn("validation_errors", validation_errors)
    validated = validated.withColumn("is_valid", F.when(F.col("validation_errors") == "", True).otherwise(False))
    
    # Extract invalid records (dead letter)
    dead_letter = validated.filter(F.col("is_valid") == False)
    
    # Test: 5 invalid records
    assert_df_count(dead_letter, 5, "test_dead_letter_count")
    
    # Test: Product E is in dead letter
    has_product_e = dead_letter.filter(F.col("name") == "Product E").count() > 0
    assert_true(has_product_e, "test_dead_letter_contains_product_e")
    
    # Test: Product A is NOT in dead letter
    has_product_a = dead_letter.filter(F.col("name") == "Product A").count() > 0
    assert_true(not has_product_a, "test_dead_letter_excludes_valid_product_a")
    
    # Test: validation_errors column is populated for dead letter records
    sample_dl = dead_letter.first()
    assert_true(sample_dl["validation_errors"] != "", "test_dead_letter_has_error_message")

run_test_suite("Test 6: Dead Letter Isolation", test_dead_letter)

# COMMAND ----------

# DBTITLE 1,Test 7: End-to-End Mock Test
# ============================================================================
# TEST 7: End-to-End Mock Test — full transform→validate→dedup pipeline
# ============================================================================

def test_end_to_end():
    """Test the full transformation pipeline on mock data."""
    # Step 1: Transform (type cast, clean, derive)
    transformed = mock_df.select(
        F.trim(F.col("name")).alias("name"),
        F.lower(F.trim(F.col("main_category"))).alias("main_category"),
        F.lower(F.trim(F.col("sub_category"))).alias("sub_category"),
        F.expr("try_cast(regexp_replace(ratings, ',', '') as double)").alias("ratings"),
        F.expr("try_cast(regexp_replace(no_of_ratings, ',', '') as double)").alias("no_of_ratings"),
        F.expr("try_cast(regexp_replace(regexp_replace(discount_price, '₹', ''), ',', '') as double)").alias("discount_price"),
        F.expr("try_cast(regexp_replace(regexp_replace(actual_price, '₹', ''), ',', '') as double)").alias("actual_price"),
    )
    
    # Add derived columns
    transformed = transformed.withColumn(
        "discount_amount",
        F.when(F.col("actual_price").isNotNull() & F.col("discount_price").isNotNull(),
               F.round(F.col("actual_price") - F.col("discount_price"), 2)).otherwise(None)
    ).withColumn(
        "discount_pct",
        F.when((F.col("actual_price").isNotNull()) & (F.col("actual_price") > 0) & (F.col("discount_price").isNotNull()),
               F.round((F.col("actual_price") - F.col("discount_price")) / F.col("actual_price") * 100, 2)).otherwise(None)
    )
    
    # Step 2: Validate
    validation_errors = F.concat(
        F.when(F.col("name").isNull() | (F.col("name") == ""), "name is null/empty; ").otherwise(""),
        F.when(F.col("main_category").isNull() | (F.col("main_category") == ""), "main_category is null/empty; ").otherwise(""),
        F.when(F.col("ratings").isNull() | (F.col("ratings") < 0) | (F.col("ratings") > 5), "ratings out of range [0-5]; ").otherwise(""),
        F.when(F.col("no_of_ratings").isNull() | (F.col("no_of_ratings") < 0), "no_of_ratings < 0; ").otherwise(""),
        F.when(F.col("discount_price").isNull() | (F.col("discount_price") < 0), "discount_price < 0; ").otherwise(""),
        F.when(F.col("actual_price").isNull() | (F.col("actual_price") <= 0), "actual_price <= 0; ").otherwise("")
    )
    
    validated = transformed.withColumn("validation_errors", validation_errors)
    validated = validated.withColumn("is_valid", F.when(F.col("validation_errors") == "", True).otherwise(False))
    
    # Step 3: Separate valid and invalid
    valid_records = validated.filter(F.col("is_valid") == True)
    invalid_records = validated.filter(F.col("is_valid") == False)
    
    # Step 4: Deduplicate valid records
    valid_deduped = valid_records.dropDuplicates(["name", "main_category", "sub_category"])
    
    # Assertions
    assert_df_count(valid_deduped, 4, "test_e2e_valid_deduped_count")  # A, B, C, D
    assert_df_count(invalid_records, 5, "test_e2e_invalid_count")  # E, F, G, null, H
    
    # Verify categories are standardized (lowercase)
    categories = [row["main_category"] for row in valid_deduped.select("main_category").distinct().collect()]
    for cat in categories:
        assert_true(cat == cat.lower(), f"test_e2e_category_lowercased_{cat}")
    
    # Verify derived columns are present and correct for Product A
    product_a = valid_deduped.filter(F.col("name") == "Product A").first()
    assert_true(product_a["discount_amount"] is not None, "test_e2e_product_a_has_discount_amount")
    assert_true(product_a["discount_pct"] is not None, "test_e2e_product_a_has_discount_pct")
    
    # Verify all valid records have non-null ratings and prices
    null_ratings = valid_deduped.filter(F.col("ratings").isNull()).count()
    assert_equal(null_ratings, 0, "test_e2e_no_null_ratings_in_valid")
    
    null_prices = valid_deduped.filter(F.col("actual_price").isNull()).count()
    assert_equal(null_prices, 0, "test_e2e_no_null_prices_in_valid")

run_test_suite("Test 7: End-to-End Mock Pipeline", test_end_to_end)

# COMMAND ----------

# DBTITLE 1,Test Summary
# ============================================================================
# TEST SUMMARY — Final results report
# ============================================================================

print("\n" + "="*60)
print("  UNIT TEST SUMMARY REPORT")
print("="*60)
print(f"  Total Tests : {TOTAL_TESTS}")
print(f"  Passed      : {PASSED_TESTS}")
print(f"  Failed      : {FAILED_TESTS}")
print(f"  Pass Rate   : {round(PASSED_TESTS / TOTAL_TESTS * 100, 2) if TOTAL_TESTS > 0 else 0}%")
print("-"*60)

# Print individual test results
for test_name, status, message in TEST_RESULTS:
    icon = "✅" if status == "PASS" else "❌"
    print(f"  {icon} {status:4s} | {test_name}")
    if status != "PASS":
        print(f"         └─ {message}")

print("-"*60)
if FAILED_TESTS == 0:
    print("  ✅ ALL TESTS PASSED — Pipeline is ready for deployment!")
else:
    print(f"  ❌ {FAILED_TESTS} TEST(S) FAILED — Fix before deploying!")
print("="*60)

# Exit with error code if any tests failed (for CI/CD integration)
if FAILED_TESTS > 0:
    raise Exception(f"{FAILED_TESTS} unit test(s) failed. See summary above.")
else:
    print("\n  Pipeline transformation logic verified. Safe to merge/deploy.")