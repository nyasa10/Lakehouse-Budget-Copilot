# Lakehouse Budget Copilot — Autonomous FinOps Agent for Databricks (GenAI + DLT + UC Functions)

## Project Overview

**Autonomous cost agent** that monitors real-time Databricks spend, predicts 30-day burn per user, and auto-scales clusters via reusable tools — saving **$1,240** across 50 owners. Ingests `system.billing.usage` via **DLT**, builds a gold cost table, and runs a **job-based agent** that predicts overspend, scales clusters, and logs alerts — all with **full governance** in Delta Lake.

- **Input**: `system.billing.usage` (live billing data)  
- **AI Logic**: Predicts 30-day burn per owner  
- **Action**: Scales clusters + sends Slack-style alerts  
- **Output**: **$1,240 saved** across 50 users  
- **Governance**: Full audit trail in Delta (`agent_traces`)
---

## Tech Stack

| Layer | Technology | Purpose |
|------|------------|--------|
| **Ingestion** | **Delta Live Tables (DLT)** | Real-time pipeline from `system.billing.usage` → `cost_gold` |
| **Governance** | **Unity Catalog Functions** | 3 reusable tools: `predict_cost`, `scale_cluster`, `slack_alert` |
| **Orchestration** | **Databricks Jobs** | Runs agent logic daily (replaces Agent Bricks) |
| **Storage & Tracing** | **Delta Lake** | `agent_traces` table = MLflow-style audit log |
| **Visualization** | **Databricks SQL Dashboard** | Live gauge: **$1,240 saved** |
| **Mocking** | **Python UDFs** | Trial-safe `scale_cluster` & `slack_alert` with real API JSON schema |

**All serverless. Zero external APIs. Production-ready.**

## Architecture

```mermaid

flowchart TD
    A["system.billing.usage (or mock input)"] --> B["DLT Pipeline: cost_gold"]
    B --> C["Unity Catalog Functions"]
    C --> C1["predict_cost()"]
    C --> C2["scale_cluster()"]
    C --> C3["slack_alert()"]
    %% Agent calls all tools
    C1 --> D["FinOps Agent Job (Python Automation)"]
    C2 --> D
    C3 --> D
    D --> E["agent_traces Delta Table"]
    E --> F["SQL Dashboard (AI Savings)"]
    D --> G["Slack Mock Queue (timestamps)"]
