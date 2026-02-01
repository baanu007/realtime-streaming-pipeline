# ⚡ Real-Time Streaming Pipeline

A production-grade streaming data pipeline using **Apache Kafka**, **PySpark Structured Streaming**, and **AWS** for real-time inventory and order processing.

![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-231F20?style=for-the-badge&logo=apache-kafka&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?style=for-the-badge&logo=apache-spark&logoColor=white)
![AWS](https://img.shields.io/badge/AWS-232F3E?style=for-the-badge&logo=amazon-aws&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)

## 📋 Overview

This project implements a real-time streaming pipeline that processes e-commerce events (orders, inventory updates, user activity) with sub-second latency. Key features:

- **Event-Driven Architecture**: Kafka as the central nervous system
- **Real-Time Processing**: Spark Structured Streaming with exactly-once semantics
- **Change Data Capture (CDC)**: Capture and process database changes in real-time
- **Multi-Sink Output**: Write to data warehouse, cache, and alerting systems
- **Auto-Scaling**: Kubernetes-ready deployment

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              DATA SOURCES                                    │
├─────────────────┬─────────────────┬─────────────────┬───────────────────────┤
│   E-Commerce    │   POS System    │    Warehouse    │      Mobile App       │
│      API        │    (CDC)        │    Scanners     │       Events          │
└────────┬────────┴────────┬────────┴────────┬────────┴───────────┬───────────┘
         │                 │                 │                    │
         ▼                 ▼                 ▼                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           KAFKA CLUSTER                                      │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐        │
│  │orders-topic │  │inventory-   │  │user-events  │  │cdc-changes  │        │
│  │             │  │updates      │  │             │  │             │        │
│  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘        │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                    SPARK STRUCTURED STREAMING                                │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │                        Stream Processing Jobs                         │   │
│  │  • Order Enrichment (join with customer/product data)                │   │
│  │  • Inventory Aggregation (real-time stock levels)                    │   │
│  │  • Fraud Detection (ML scoring in real-time)                         │   │
│  │  • Session Analytics (user behavior windows)                         │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                    ┌─────────────────┼─────────────────┐
                    ▼                 ▼                 ▼
┌─────────────────────────┐ ┌─────────────────┐ ┌─────────────────────────┐
│      DATA WAREHOUSE     │ │    REDIS CACHE   │ │    ALERTING SYSTEM      │
│      (Snowflake)        │ │  (Real-time KV)  │ │   (SNS/PagerDuty)       │
│  • Fact tables          │ │  • Stock levels  │ │  • Low stock alerts     │
│  • Aggregated metrics   │ │  • Order status  │ │  • Fraud alerts         │
│  • Historical data      │ │  • Session data  │ │  • SLA breaches         │
└─────────────────────────┘ └─────────────────┘ └─────────────────────────┘
```

## 📁 Project Structure

```
realtime-streaming-pipeline/
├── src/
│   ├── producers/              # Kafka producers
│   │   ├── order_producer.py
│   │   ├── inventory_producer.py
│   │   └── cdc_producer.py
│   ├── consumers/              # Spark streaming jobs
│   │   ├── order_processor.py
│   │   ├── inventory_aggregator.py
│   │   ├── fraud_detector.py
│   │   └── session_analyzer.py
│   ├── schemas/                # Avro/JSON schemas
│   │   ├── order_schema.avsc
│   │   └── inventory_schema.avsc
│   └── utils/
│       ├── kafka_utils.py
│       ├── spark_utils.py
│       └── config.py
├── infrastructure/
│   ├── docker-compose.yml      # Local Kafka cluster
│   ├── kubernetes/             # K8s manifests
│   └── terraform/              # AWS infrastructure
├── tests/
│   ├── unit/
│   └── integration/
├── data/                       # Sample data for testing
├── config/
│   └── application.yaml
├── requirements.txt
└── README.md
```

## 🚀 Quick Start

### Prerequisites

- Python 3.9+
- Docker & Docker Compose
- Java 11+ (for Spark)
- AWS CLI (optional, for cloud deployment)

### Local Development

```bash
# Clone repository
git clone https://github.com/baanu007/realtime-streaming-pipeline.git
cd realtime-streaming-pipeline

# Start Kafka cluster
docker-compose up -d

# Create virtual environment
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Create Kafka topics
python scripts/create_topics.py

# Start a producer (in terminal 1)
python src/producers/order_producer.py

# Start a consumer (in terminal 2)
python src/consumers/order_processor.py
```

## 📊 Stream Processing Jobs

### 1. Order Enrichment
Joins streaming orders with customer and product dimensions:

```python
# Enriched order stream with customer and product details
enriched_orders = (
    orders_stream
    .join(customers_df, "customer_id")
    .join(products_df, "product_id")
    .withColumn("order_value", col("quantity") * col("unit_price"))
    .withColumn("processing_time", current_timestamp())
)
```

### 2. Real-Time Inventory
Maintains running inventory counts with windowed aggregations:

```python
# 5-minute tumbling windows for inventory snapshots
inventory_updates = (
    inventory_stream
    .withWatermark("event_time", "1 minute")
    .groupBy(
        window("event_time", "5 minutes"),
        "product_id",
        "warehouse_id"
    )
    .agg(
        sum("quantity_change").alias("net_change"),
        count("*").alias("transaction_count")
    )
)
```

### 3. Fraud Detection
Real-time ML scoring for suspicious orders:

```python
# Score each order against fraud model
fraud_scored = orders_stream.transform(
    lambda df: apply_fraud_model(df, model_path)
).filter(col("fraud_score") > 0.8)
```

## 🔧 Configuration

```yaml
# config/application.yaml
kafka:
  bootstrap_servers: "localhost:9092"
  consumer_group: "streaming-pipeline"
  topics:
    orders: "ecom.orders.v1"
    inventory: "ecom.inventory.v1"
    
spark:
  master: "local[*]"
  app_name: "RealTimeStreamingPipeline"
  checkpoint_location: "s3://bucket/checkpoints/"
  
sinks:
  snowflake:
    account: "${SNOWFLAKE_ACCOUNT}"
    warehouse: "STREAMING_WH"
    database: "REALTIME"
  redis:
    host: "localhost"
    port: 6379
```

## 📈 Monitoring

- **Kafka**: Confluent Control Center / Kafka Manager
- **Spark**: Spark UI (port 4040)
- **Metrics**: Prometheus + Grafana dashboards
- **Alerting**: CloudWatch / PagerDuty integration

## 🧪 Testing

```bash
# Unit tests
pytest tests/unit/ -v

# Integration tests (requires running Kafka)
pytest tests/integration/ -v

# Load testing
python scripts/load_test.py --events-per-sec 10000 --duration 60
```

## 🛠️ Technologies

| Component | Technology |
|-----------|------------|
| Message Broker | Apache Kafka |
| Stream Processing | PySpark Structured Streaming |
| Serialization | Apache Avro / JSON |
| Cache | Redis |
| Data Warehouse | Snowflake |
| Orchestration | Kubernetes |
| Infrastructure | Terraform / AWS |
| Monitoring | Prometheus + Grafana |

## 📄 License

MIT License

---

*Built for high-throughput, low-latency data engineering*
