import sys
import logging
sys.path.append('/Workspace/Repos/maha.b.lakshmi@gmail.com/data-quality-testing/src/checks')
from dq_checks import check_completeness, check_completeness_all

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

test_data_path = "/Workspace/Repos/maha.b.lakshmi@gmail.com/data-quality-testing/tests/test_data/employees.csv"

def load_test_df():
    return spark.read.option("header", True).option("inferSchema", True).csv(test_data_path)

def test_completeness_salary_has_nulls():
    """Test that completeness check fails when null percentage exceeds threshold."""
    df = load_test_df()
    result = check_completeness(df, "salary", max_null_pct=10)
    logging.info(f"Salary completeness result: {result}")
    assert result["null_pct"] == 25.0
    assert result["passed"] == False

def test_completeness_id_has_no_nulls():
    """Test that completeness check passes when column has no null values."""
    df = load_test_df()
    result = check_completeness(df, "id")
    logging.info(f"ID completeness test result: {result}")
    assert result["null_pct"] == 0.0
    assert result["passed"] == True

def test_completeness_all_covers_every_column():
    """Test that check_completeness_all returns results for all DataFrame columns."""
    df = load_test_df()
    results = check_completeness_all(df)
    logging.info(f"All columns completeness test results: {results}")
    assert len(results) == 4