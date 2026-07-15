"""
This examples defines an expected schema with 5 fields for customer data, 
creates a DataFrame with sample data, and performs schema validation. 
It demonstrates how schema mismatches (e.g., non-integer postal codes) 
can be caught and reported.
"""
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# Define the expected schema with 5 fields
expected_schema = StructType([
    StructField("customerID", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email_address", StringType(), True),
    StructField("postal_zip_code", IntegerType(), True)
])
# Create a DataFrame with the expected schema
data = [
    ("C001", "Alice", "Smith", "alice@example.com", "ABC123")
]
try:
    df = spark.createDataFrame(data, schema=expected_schema)
    display(df)
    if df.schema == expected_schema:
        print("Schema validation passed.")
    else:
        print("Schema validation failed.")
except Exception as e:
    print("Schema validation failed due to error:", e)
