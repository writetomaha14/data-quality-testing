from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("MissingRecordCheck").getOrCreate()

def find_missing_customers():
    """
    Finds and displays customer IDs present in the expected list but missing from the actual data.
    """
    # Sample expected customer IDs
    expected_data = [(1,), (2,), (3,), (4,), (5,)]
    df_expected = spark.createDataFrame(expected_data, ["customer_id"])

    # Sample actual customer data (missing customer_id = 5)
    actual_data = [
        (1, "Alice"),
        (2, "Bob"),
        (3, "Charlie"),
        (4, "David")
    ]
    df_actual = spark.createDataFrame(actual_data, ["customer_id", "name"])

    # Find missing records
    missing = df_expected.join(
        df_actual,
        on="customer_id",
        how="left_anti"
    )
    print("Missing Records:")
    display(missing)
      
find_missing_customers()
