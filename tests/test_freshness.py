import sys
import logging
from datetime import datetime, timedelta
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType

sys.path.append('/Workspace/Repos/maha.b.lakshmi@gmail.com/data-quality-testing/src/checks')
from dq_checks import check_freshness, check_all_freshness

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

test_data_path = "/Workspace/Repos/maha.b.lakshmi@gmail.com/data-quality-testing/tests/test_data/orders_with_timestamps.csv"

def load_test_df():
    """Load test data with timestamps."""
    return spark.read.option("header", True).option("inferSchema", True).csv(test_data_path)

def create_fresh_data():
    """Create DataFrame with fresh data (within last hour)."""
    schema = StructType([
        StructField("order_id", IntegerType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("amount", IntegerType(), True),
        StructField("order_date", TimestampType(), True)
    ])
    
    now = datetime.now()
    data = [
        (1, 101, 500, now - timedelta(minutes=30)),
        (2, 102, 700, now - timedelta(minutes=45)),
        (3, 103, 300, now - timedelta(hours=0.5))
    ]
    return spark.createDataFrame(data, schema)

def create_stale_data():
    """Create DataFrame with stale data (older than 24 hours)."""
    schema = StructType([
        StructField("order_id", IntegerType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("amount", IntegerType(), True),
        StructField("order_date", TimestampType(), True)
    ])
    
    now = datetime.now()
    data = [
        (1, 101, 500, now - timedelta(hours=48)),
        (2, 102, 700, now - timedelta(hours=36)),
        (3, 103, 300, now - timedelta(days=2))
    ]
    return spark.createDataFrame(data, schema)

def create_empty_data():
    """Create DataFrame with no data."""
    schema = StructType([
        StructField("order_id", IntegerType(), True),
        StructField("order_date", TimestampType(), True)
    ])
    return spark.createDataFrame([], schema)

def create_null_timestamp_data():
    """Create DataFrame where all timestamps are null."""
    schema = StructType([
        StructField("order_id", IntegerType(), True),
        StructField("order_date", TimestampType(), True)
    ])
    data = [(1, None), (2, None), (3, None)]
    return spark.createDataFrame(data, schema)

# ============================================================================
# TEST CASES
# ============================================================================

def test_freshness_data_is_fresh():
    """Test that freshness check passes when data is fresh (within threshold)."""
    df = create_fresh_data()
    result = check_freshness(df, "order_date", max_age_hours=24)
    logging.info(f"Fresh data test result: {result}")
    assert result["passed"] == True
    assert result["age_hours"] < 24

def test_freshness_data_is_stale():
    """Test that freshness check fails when data is stale (exceeds threshold)."""
    df = create_stale_data()
    result = check_freshness(df, "order_date", max_age_hours=24)
    logging.info(f"Stale data test result: {result}")
    assert result["passed"] == False
    assert result["age_hours"] > 24

def test_freshness_with_custom_threshold():
    """Test freshness check with custom threshold (1 hour)."""
    df = create_fresh_data()
    result = check_freshness(df, "order_date", max_age_hours=1)
    logging.info(f"Custom threshold test result: {result}")
    # Should pass since data is within 1 hour
    assert result["passed"] == True
    assert result["age_hours"] < 1

def test_freshness_with_null_timestamps():
    """Test that freshness check fails gracefully when all timestamps are null."""
    df = create_null_timestamp_data()
    result = check_freshness(df, "order_date", max_age_hours=24)
    logging.info(f"Null timestamp test result: {result}")
    assert result["passed"] == False
    assert result["latest_timestamp"] is None
    assert result["age_hours"] is None

def test_freshness_with_empty_dataframe():
    """Test that freshness check handles empty DataFrames."""
    df = create_empty_data()
    result = check_freshness(df, "order_date", max_age_hours=24)
    logging.info(f"Empty DataFrame test result: {result}")
    assert result["passed"] == False

def test_check_all_freshness():
    """Test batch freshness check across multiple tables."""
    fresh_df = create_fresh_data()
    stale_df = create_stale_data()
    
    table_registry = {
        "orders_fresh": fresh_df,
        "orders_stale": stale_df
    }
    
    freshness_config = [
        {"table": "orders_fresh", "timestamp_column": "order_date", "max_age_hours": 24},
        {"table": "orders_stale", "timestamp_column": "order_date", "max_age_hours": 24}
    ]
    
    results = check_all_freshness(table_registry, freshness_config)
    logging.info(f"Batch freshness check results: {results}")
    
    assert len(results) == 2
    assert results[0]["table"] == "orders_fresh"
    assert results[0]["passed"] == True
    assert results[1]["table"] == "orders_stale"
    assert results[1]["passed"] == False

def test_freshness_boundary_condition():
    """Test freshness check at exact threshold boundary."""
    schema = StructType([
        StructField("order_id", IntegerType(), True),
        StructField("order_date", TimestampType(), True)
    ])
    
    # Create data exactly 24 hours old
    now = datetime.now()
    data = [(1, now - timedelta(hours=24))]
    df = spark.createDataFrame(data, schema)
    
    result = check_freshness(df, "order_date", max_age_hours=24)
    logging.info(f"Boundary condition test result: {result}")
    # At exactly 24 hours, should pass (age_hours <= max_age_hours)
    assert result["passed"] == True
