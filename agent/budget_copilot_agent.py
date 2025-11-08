# budget_copilot_agent.py — 100% working in 14-day trial (no PySpark Connect issues)
from pyspark.sql.functions import *
import json
from datetime import datetime

# Step 1: Query cost_gold
df = spark.table("budget_copilot.raw.cost_gold")
owners = [row['owner'] for row in df.select("owner").distinct().collect()]

total_saved = 0.0
actions = []

for owner in owners:
    # Step 2: predict_cost
    projected_row = spark.sql(f"SELECT budget_copilot.raw.predict_cost('{owner}') AS projected").collect()[0]
    projected = float(projected_row['projected'])  # ← Force float
    
    if projected > 100:
        # Step 3: scale_cluster
        cluster_id = f"mock-{owner}-cluster"
        scale_resp = spark.sql(f"SELECT budget_copilot.raw.scale_cluster('{cluster_id}') AS resp").collect()[0]['resp']
        scale_json = json.loads(scale_resp)
        savings = float(scale_json.get('savings_usd', 0.0))
        
        # Step 4: slack_alert
        msg = f"@{owner} Auto-scaled! Saved ${savings:.2f}"
        slack_resp = spark.sql(f"SELECT budget_copilot.raw.slack_alert('{msg}') AS resp").collect()[0]['resp']
        slack_json = json.loads(slack_resp)
        
        total_saved += savings
        actions.append({
            "owner": owner,
            "action": "scaled",
            "savings": savings,
            "slack_ts": slack_json.get('ts', '')
        })
    else:
        msg = f"@{owner} Under budget: ${projected:.2f}"
        slack_resp = spark.sql(f"SELECT budget_copilot.raw.slack_alert('{msg}') AS resp").collect()[0]['resp']
        slack_json = json.loads(slack_resp)
        actions.append({
            "owner": owner,
            "action": "alert_under_budget",
            "slack_ts": slack_json.get('ts', '')
        })

# Step 5: Final output — NO round(), NO list comp in output dict
total_saved_rounded = float(format(total_saved, '.2f'))  # ← Safe rounding
owners_optimized = 0
for a in actions:
    if a['action'] == 'scaled':
        owners_optimized += 1

output = {
    "total_saved": total_saved_rounded,
    "owners_optimized": owners_optimized,
    "status": "complete",
    "actions": actions
}

print(json.dumps(output, indent=2))

# Step 6: Save to Delta — convert dict to Row
from pyspark.sql import Row
trace_row = Row(
    total_saved=output["total_saved"],
    owners_optimized=output["owners_optimized"],
    status=output["status"],
    actions=output["actions"]
)
spark.createDataFrame([trace_row]).write.mode("append").saveAsTable("budget_copilot.raw.agent_traces")
