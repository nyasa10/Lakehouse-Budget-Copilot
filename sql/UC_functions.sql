CREATE CATALOG IF NOT EXISTS budget_copilot;

USE CATALOG budget_copilot;
CREATE SCHEMA raw;
CREATE SCHEMA gold;

SHOW SCHEMAS IN budget_copilot;
SELECT * FROM budget_copilot.raw.cost_gold LIMIT 5;

CREATE OR REPLACE FUNCTION budget_copilot.raw.predict_cost(owner STRING)
RETURNS DOUBLE
LANGUAGE SQL
RETURN
  SELECT ROUND(SUM(cost_usd) * 30, 2)
  FROM budget_copilot.raw.cost_gold
  WHERE owner = predict_cost.owner;

SELECT budget_copilot.raw.predict_cost('highspend0@example.com');

CREATE OR REPLACE FUNCTION budget_copilot.raw.scale_cluster(cluster_id STRING)
RETURNS STRING
LANGUAGE PYTHON
AS $$
import json
from datetime import datetime

# Real-looking mock response
return json.dumps({
    "state": "RESIZED",
    "cluster_id": cluster_id,
    "num_workers_target": 2,
    "savings_dbus": 4.2,
    "savings_usd": 2.10,
    "timestamp": datetime.utcnow().isoformat() + "Z",
    "status_code": 200
})
$$;

SELECT budget_copilot.raw.scale_cluster('mock-cluster-1');

CREATE OR REPLACE FUNCTION budget_copilot.raw.scale_cluster(cluster_id STRING)
RETURNS STRING
LANGUAGE PYTHON
AS $$
import json
from datetime import datetime

return json.dumps({
    "state": "RESIZED",
    "cluster_id": cluster_id,
    "num_workers_target": 2,
    "savings_dbus": 50.0,
    "savings_usd": 24.80,  
    "timestamp": datetime.utcnow().isoformat() + "Z",
    "status_code": 200
})
$$;

SELECT budget_copilot.raw.slack_alert('Trial test!');

CREATE TABLE IF NOT EXISTS budget_copilot.raw.agent_traces (
  total_saved DOUBLE,
  owners_optimized LONG,
  status STRING,
  actions ARRAY<STRUCT<owner:STRING, action:STRING, savings:DOUBLE, slack_ts:STRING>>
) USING DELTA;

SELECT SUM(total_saved) AS ai_saved FROM budget_copilot.raw.agent_traces;
