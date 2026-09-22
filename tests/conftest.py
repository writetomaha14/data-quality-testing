"""
Shared Pytest Fixtures for Data Quality Testing Framework

Fixtures for all test files in this directory.
"""

import sys
import logging
from pathlib import Path
from typing import Dict

import pytest
import yaml
from pyspark.sql import SparkSession, DataFrame


# ==============================================================================
# Path Configuration
# ==============================================================================

TEST_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = TEST_DIR.parent
TEST_DATA_DIR = TEST_DIR / "test_data"
CONFIG_DIR = PROJECT_ROOT / "src" / "config"
CHECKS_DIR = PROJECT_ROOT / "src" / "checks"

# Add checks module to path
sys.path.insert(0, str(CHECKS_DIR))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ==============================================================================
# Shared Fixtures
# ==============================================================================

@pytest.fixture(scope="session")
def spark_session():
    """
    Get or create a SparkSession for tests.
    
    Tries multiple approaches to get a Spark session in this order:
    1. Access Databricks global spark object
    2. Get active session
    3. Create new session (for local testing only)
    """
    logger.info("=" * 70)
    logger.info("Getting SparkSession for test suite")
    logger.info("=" * 70)
    
    spark = None
    
    # Try 1: Get from Databricks globals (when running in notebook context)
    try:
        import builtins
        if hasattr(builtins, 'spark'):
            spark = builtins.spark
            logger.info("Using Databricks global spark session")
    except Exception as e:
        logger.debug(f"Could not access global spark: {e}")
    
    # Try 2: Get active session (when one exists)
    if spark is None:
        spark = SparkSession.getActiveSession()
        if spark is not None:
            logger.info("Using active SparkSession")
    
    # Try 3: Use Remote Spark (Databricks serverless)
    if spark is None:
        try:
            from pyspark.sql.connect.session import SparkSession as RemoteSparkSession
            # On Databricks serverless, spark is available via remote connection
            spark = RemoteSparkSession.builder.remote("local").getOrCreate()
            logger.info("Created Databricks Remote SparkSession")
        except Exception as e:
            logger.debug(f"Could not create remote session: {e}")
    
    if spark is None:
        raise RuntimeError(
            "Could not obtain a SparkSession. "
            "Tests must be run in a Databricks environment with Spark available."
        )
    
    logger.info(f"Spark version: {spark.version}")
    
    return spark


@pytest.fixture(scope="module")
def test_data(spark_session: SparkSession) -> Dict[str, DataFrame]:
    """Load all test data CSV files."""
    logger.info("-" * 70)
    logger.info(f"Loading test data from {TEST_DATA_DIR}")
    logger.info("-" * 70)
    
    def load_csv(filename: str) -> DataFrame:
        file_path = TEST_DATA_DIR / filename
        logger.debug(f"Loading {file_path}")
        return spark_session.read \
            .option("header", True) \
            .option("inferSchema", True) \
            .csv(str(file_path))
    
    data = {
        "customers": load_csv("customers.csv"),
        "orders": load_csv("orders_fk.csv"),
        "order_line_items": load_csv("order_line_items.csv"),
        "order_products": load_csv("order_products.csv"),
        "employees": load_csv("employees_hierarchy.csv"),
        "departments": load_csv("departments_hierarchy.csv"),
    }
    
    logger.info("Test data loaded:")
    for table_name, df in data.items():
        row_count = df.count()
        logger.info(f"  {table_name}: {row_count} rows")
    logger.info("-" * 70)
    
    return data


@pytest.fixture(scope="module")
def fk_config() -> Dict:
    """Load foreign key configuration from YAML."""
    config_path = CONFIG_DIR / "config.yaml"
    logger.info(f"Loading FK configuration from {config_path}")
    
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    fk_count = len(config.get('foreign_keys', []))
    logger.info(f"  Loaded {fk_count} FK relationships from config")
    
    return config


@pytest.fixture(scope="module")
def schema_config() -> Dict:
    """Load schema configuration from YAML."""
    config_path = CONFIG_DIR / "config.yaml"
    logger.info(f"Loading schema configuration from {config_path}")
    
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    schema_count = len(config.get('expected_schemas', {}))
    logger.info(f"  Loaded {schema_count} table schemas from config")
    
    return config


@pytest.fixture(scope="module")
def schema_test_data(spark_session: SparkSession) -> Dict[str, DataFrame]:
    """
    Load test data specifically for schema validation tests.
    
    This fixture uses separate CSV files to avoid interfering with FK tests.
    Schema test files should follow naming convention:
    - Base table: <table_name>.csv (e.g., employees.csv)
    - Missing columns: <table_name>_missing_cols.csv
    - Extra columns: <table_name>_extra_cols.csv
    - Wrong types: <table_name>_wrong_types.csv
    """
    logger.info("-" * 70)
    logger.info(f"Loading schema test data from {TEST_DATA_DIR}")
    logger.info("-" * 70)
    
    def load_csv(filename: str) -> DataFrame:
        file_path = TEST_DATA_DIR / filename
        if not file_path.exists():
            logger.warning(f"Schema test file not found: {file_path}")
            return None
        logger.debug(f"Loading {file_path}")
        return spark_session.read \
            .option("header", True) \
            .option("inferSchema", True) \
            .csv(str(file_path))
    
    # Load schema test data (using employees_hierarchy for now)
    # When you create schema-specific tests, replace with employees.csv
    data = {}
    
    # Load base employees table (currently using hierarchy file)
    employees_df = load_csv("employees_hierarchy.csv")
    if employees_df:
        data["employees"] = employees_df
    
    # Load variant test files if they exist
    for suffix in ["_missing_cols", "_extra_cols", "_wrong_types"]:
        df = load_csv(f"employees{suffix}.csv")
        if df:
            data[f"employees{suffix}"] = df
    
    logger.info(f"Schema test data loaded: {len(data)} tables")
    for table_name in data.keys():
        logger.info(f"  {table_name}")
    logger.info("-" * 70)
    
    return data


@pytest.fixture
def table_registry(test_data: Dict[str, DataFrame], schema_test_data: Dict[str, DataFrame]) -> Dict[str, DataFrame]:
    """
    Provide a unified table registry for tests.
    
    Combines FK test data and schema test data.
    FK tests will use: customers, orders, order_line_items, order_products, employees, departments
    Schema tests will use: employees (and variants like employees_missing_cols)
    """
    registry = test_data.copy()
    registry.update(schema_test_data)
    return registry


@pytest.fixture(scope="module")
def consistency_rules() -> list:
    """Load consistency rules from YAML config."""
    config_path = CONFIG_DIR / "config.yaml"
    logger.info(f"Loading consistency rules from {config_path}")
    
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    rules = config.get('consistency_rules', [])
    logger.info(f"  Loaded {len(rules)} consistency rules from config")
    
    return rules


@pytest.fixture(scope="module")
def consistency_test_data(spark_session: SparkSession) -> DataFrame:
    """Load test data for consistency rule validation."""
    logger.info("-" * 70)
    logger.info(f"Loading consistency test data from {TEST_DATA_DIR}")
    logger.info("-" * 70)
    
    file_path = TEST_DATA_DIR / "orders_consistency_valid.csv"
    logger.debug(f"Loading {file_path}")
    df = spark_session.read \
        .option("header", True) \
        .option("inferSchema", True) \
        .csv(str(file_path))
    
    logger.info(f"  orders_consistency_valid: {df.count()} rows")
    logger.info("-" * 70)
    
    return df


@pytest.fixture(scope="module")
def consistency_violation_data(spark_session: SparkSession) -> DataFrame:
    """Load test data with intentional consistency rule violations."""
    logger.info("-" * 70)
    logger.info(f"Loading consistency violation test data from {TEST_DATA_DIR}")
    logger.info("-" * 70)
    
    file_path = TEST_DATA_DIR / "orders_consistency_violations.csv"
    logger.debug(f"Loading {file_path}")
    df = spark_session.read \
        .option("header", True) \
        .option("inferSchema", True) \
        .csv(str(file_path))
    
    logger.info(f"  orders_consistency_violations: {df.count()} rows")
    logger.info("-" * 70)
    
    return df


@pytest.fixture(scope="module")
def business_rules() -> list:
    """Load business rules from YAML config."""
    config_path = CONFIG_DIR / "config.yaml"
    logger.info(f"Loading business rules from {config_path}")
    
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    rules = config.get('business_rules', [])
    logger.info(f"  Loaded {len(rules)} business rules from config")
    
    return rules


@pytest.fixture(scope="module")
def business_test_data(spark_session: SparkSession) -> DataFrame:
    """Load test data for business rule validation."""
    logger.info("-" * 70)
    logger.info(f"Loading business test data from {TEST_DATA_DIR}")
    logger.info("-" * 70)
    
    file_path = TEST_DATA_DIR / "orders_business_valid.csv"
    logger.debug(f"Loading {file_path}")
    df = spark_session.read \
        .option("header", True) \
        .option("inferSchema", True) \
        .csv(str(file_path))
    
    logger.info(f"  orders_business_valid: {df.count()} rows")
    logger.info("-" * 70)
    
    return df


@pytest.fixture(scope="module")
def business_violation_data(spark_session: SparkSession) -> DataFrame:
    """Load test data with intentional business rule violations."""
    logger.info("-" * 70)
    logger.info(f"Loading business violation test data from {TEST_DATA_DIR}")
    logger.info("-" * 70)
    
    file_path = TEST_DATA_DIR / "orders_business_violations.csv"
    logger.debug(f"Loading {file_path}")
    df = spark_session.read \
        .option("header", True) \
        .option("inferSchema", True) \
        .csv(str(file_path))
    
    logger.info(f"  orders_business_violations: {df.count()} rows")
    logger.info("-" * 70)
    
    return df


@pytest.fixture(scope="module")
def reconciliation_source_data(spark_session: SparkSession) -> DataFrame:
    """Load source dataset for reconciliation tests."""
    logger.info("-" * 70)
    logger.info(f"Loading reconciliation source data from {TEST_DATA_DIR}")
    logger.info("-" * 70)
    
    file_path = TEST_DATA_DIR / "reconciliation_source.csv"
    logger.debug(f"Loading {file_path}")
    df = spark_session.read \
        .option("header", True) \
        .option("inferSchema", True) \
        .csv(str(file_path))
    
    logger.info(f"  reconciliation_source: {df.count()} rows")
    logger.info("-" * 70)
    
    return df


@pytest.fixture(scope="module")
def reconciliation_target_data(spark_session: SparkSession) -> DataFrame:
    """Load target dataset for reconciliation tests (matches source)."""
    logger.info("-" * 70)
    logger.info(f"Loading reconciliation target data from {TEST_DATA_DIR}")
    logger.info("-" * 70)
    
    file_path = TEST_DATA_DIR / "reconciliation_target.csv"
    logger.debug(f"Loading {file_path}")
    df = spark_session.read \
        .option("header", True) \
        .option("inferSchema", True) \
        .csv(str(file_path))
    
    logger.info(f"  reconciliation_target: {df.count()} rows")
    logger.info("-" * 70)
    
    return df


@pytest.fixture(scope="module")
def reconciliation_mismatch_target_data(spark_session: SparkSession) -> DataFrame:
    """Load target dataset with intentional reconciliation mismatches."""
    logger.info("-" * 70)
    logger.info(f"Loading reconciliation mismatch target data from {TEST_DATA_DIR}")
    logger.info("-" * 70)
    
    file_path = TEST_DATA_DIR / "reconciliation_mismatch_target.csv"
    logger.debug(f"Loading {file_path}")
    df = spark_session.read \
        .option("header", True) \
        .option("inferSchema", True) \
        .csv(str(file_path))
    
    logger.info(f"  reconciliation_mismatch_target: {df.count()} rows")
    logger.info("-" * 70)
    
    return df


@pytest.fixture(scope="module")
def reconciliation_config() -> list:
    """Load reconciliation configuration from YAML."""
    config_path = CONFIG_DIR / "config.yaml"
    logger.info(f"Loading reconciliation configuration from {config_path}")
    
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    recon_config = config.get('reconciliation', [])
    logger.info(f"  Loaded {len(recon_config)} reconciliation checks from config")
    
    return recon_config
