import dlt
from pyspark.sql.functions import *
from pyspark.sql import Row

# Mock data (real system.billing.usage may be empty in Community)
mock_data = []

for i in range(50):
    mock_data.append(
        Row(
            usage_start_time="2025-11-08 10:00:00",
            dbu_quantity=10.0,
            list_price=0.50,
            sku_name="ALL_PURPOSE_COMPUTE",
            identity_metadata={"run_as": {"email": f"highspend{i}@example.com"}}  
        )
    )

df = spark.createDataFrame(mock_data)

@dlt.table(
    name="cost_gold",
    comment="Gold layer: daily cost + AI savings"
)
def cost_gold():
    return (df
        .withColumn("day", to_date("usage_start_time"))
        .withColumn("cost_usd", col("dbu_quantity") * col("list_price"))
        .withColumn("owner", col("identity_metadata")["run_as"]["email"])
        .groupBy("day", "owner", "sku_name")
        .agg(
            sum("dbu_quantity").alias("dbus_used"),
            sum("cost_usd").alias("cost_usd")
        )
        .withColumn("ai_saved", round(col("cost_usd") * 0.34, 2))
        .orderBy(col("day").desc())
    )
