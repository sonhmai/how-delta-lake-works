import pytest
from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, BooleanType
)

WAREHOUSE_DIR = "delta_warehouse"

@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder \
        .appName("DeltaLakeSCD2Test") \
        .master("local[*]") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.warehouse.dir", WAREHOUSE_DIR) \
        .getOrCreate()
    yield spark
    spark.stop()


def test_scd_type_2_merge(spark):
    # Define the path for the Delta table within the warehouse directory
    delta_table_path = f"${WAREHOUSE_DIR}/customers"

    # Define schema for customers DataFrame with snake case column names
    customers_schema = StructType([
        StructField("customer_id", IntegerType(), False),
        StructField("address", StringType(), False),
        StructField("current", BooleanType(), False),
        StructField("effective_date", StringType(), False),
        StructField("end_date", StringType(), True)  # Nullable field
    ])

    # Create initial customers DataFrame with schema
    customers_data = [
        (1, "123 Main St", True, "2023-01-01", None),
        (2, "456 Elm St", True, "2023-01-01", None)
    ]
    customers_df = spark.createDataFrame(customers_data, schema=customers_schema)
    customers_df.write.format("delta").mode("overwrite").save(delta_table_path)

    # Create updates DataFrame with snake case column names
    updates_data = [
        (1, "789 Oak St", "2023-02-01"),  # Address change for customer_id 1
        (3, "101 Pine St", "2023-02-01")  # New customer_id 3
    ]
    updates_columns = ["customer_id", "address", "effective_date"]
    updates_df = spark.createDataFrame(updates_data, updates_columns)

    # Load Delta table
    customers_table = DeltaTable.forPath(spark, delta_table_path)

    # Prepare staged updates
    new_addresses_to_insert = updates_df.alias("updates") \
        .join(customers_table.toDF().alias("customers"), "customer_id") \
        .where("customers.current = true AND updates.address <> customers.address")

    staged_updates = (
        new_addresses_to_insert
        .selectExpr("NULL as mergeKey", "updates.*")
        .union(updates_df.selectExpr("updates.customer_id as mergeKey", "*"))
    )

    # Apply SCD Type 2 operation using merge
    customers_table.alias("customer").merge(
        staged_updates.alias("staged_updates"),
        "customer.customer_id = mergeKey"
    ).whenMatchedUpdate(
        condition="customer.current = true AND customer.address <> staged_updates.address",
        set={
            "current": "false",
            "end_date": "staged_updates.effective_date"
        }
    ).whenNotMatchedInsert(
        values={
            "customer_id": "staged_updates.customer_id",
            "address": "staged_updates.address",
            "current": "true",
            "effective_date": "staged_updates.effective_date",
            "end_date": "null"
        }
    ).execute()

    # Verify the results
    result_df = customers_table.toDF().orderBy("customer_id", "effective_date")
    result_df.show()

    expected_data = [
        (1, "123 Main St", False, "2023-01-01", "2023-02-01"),
        (1, "789 Oak St", True, "2023-02-01", None),
        (2, "456 Elm St", True, "2023-01-01", None),
        (3, "101 Pine St", True, "2023-02-01", None)
    ]
    expected_df = spark.createDataFrame(expected_data, schema=customers_schema)

    assert result_df.collect() == expected_df.collect()
