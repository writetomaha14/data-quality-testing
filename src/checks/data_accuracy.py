
from pyspark.sql.functions import col, floor, months_between, current_date

def validate_total_price():
    """
    This example creates a sample DataFrame of order records and validates that total_price equals quantity multiplied by unit_price for each row.
    It reports any failures by printing the mismatched records.
    """
    # Sample order data
    data = [
        ("O001", 2, 50, 100),    # Valid
        ("O002", 3, 30, 70),     # Invalid: 3*30=90, not 80
        ("O003", 1, 200, 200)    # Valid
    ]
    columns = ["order_id", "quantity", "unit_price", "total_price"]
    df = spark.createDataFrame(data, columns)

    # Find rows where total_price does not match quantity * unit_price
    invalid_rows = df.filter(
        col("total_price") != col("quantity") * col("unit_price")
    )

    if invalid_rows.count() == 0:
        print("All order records have accurate total_price values.")
    else:
        print("Order records with inaccurate total_price values:")
        display(invalid_rows)



def validate_customer_age():
    """
    This examples creates a sample DataFrame with customer date of birth and age, 
    and validates that the age column matches the calculated age from date_of_birth.
    It reports any failures by printing the mismatched records.
    """
    # Sample customer data
    data = [
        ("C001", "1990-01-01", 35),  # Valid if current year is 2025
        ("C002", "2000-06-15", 25),  # Valid if current year is 2024
        ("C003", "1985-12-31", 30)   # Invalid
    ]
    columns = ["customer_id", "date_of_birth", "age"]
    df = spark.createDataFrame(data, columns)

    # Calculate age from date_of_birth
    df_with_calc = df.withColumn(
        "calculated_age",
        floor(months_between(current_date(), col("date_of_birth")) / 12)
    )

    # Find rows where age does not match calculated_age
    invalid_rows = df_with_calc.filter(col("age") != col("calculated_age"))

    if invalid_rows.count() == 0:
        print("All customer ages are correctly calculated from date_of_birth.")
    else:
        print("Customers with incorrect age values:")
        display(invalid_rows)

validate_customer_age()
validate_total_price()
