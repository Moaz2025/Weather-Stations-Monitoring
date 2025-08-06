# 🌦️ Weather Stations Monitoring System

This project is a full-stack **data-intensive application** designed to simulate and monitor weather data streams in real-time. It models a network of IoT-based weather stations and provides complete end-to-end handling of data — from ingestion and processing to indexing and querying.

Built as part of the **Designing Data Intensive Applications** course at **Alexandria University**, this system demonstrates key patterns in streaming architectures, distributed systems, and big data infrastructure.

---

## 📌 Project Objectives

- Simulate **10+ IoT weather stations** generating real-time weather readings
- Stream messages using **Apache Kafka**
- Archive readings into **Parquet files**
- Maintain the latest readings using a custom **BitCask key-value store**
- Index weather data using **Elasticsearch**, visualize with **Kibana**
- Detect specific weather conditions (e.g., high humidity for rain alerts)
- Analyze historical patterns via batch queries on Parquet data
- Deploy all services using **Docker** and **Kubernetes**
- Monitor performance using **Java Flight Recorder (JFR)**

---

## ⚙️ Key Features

- **Data Acquisition**: Simulated weather stations emitting JSON readings to Kafka.
- **Data Processing**:
  - Kafka processors detect rain (humidity > 70%).
  - Central Station archives all data into Parquet and updates BitCask store.
- **Indexing**:
  - Elasticsearch stores indexed weather data for advanced querying.
  - Kibana dashboards for real-time and historical analysis.
- **Custom BitCask Store**:
  - Optimized key-value store to persist the latest status of each station.
  - Includes recovery hint files, compaction, and client CLI support.
- **Batch Archiving**:
  - Weather readings stored in Parquet files partitioned by station and time.
- **Historical Analysis**:
  - Analyze dropped messages and battery status distribution.
- **Deployment**:
  - Dockerized microservices deployed in Kubernetes with shared storage.
- **Profiling**:
  - Central server performance profiled using JFR to identify bottlenecks.

---

## 🧪 Use Cases

- Track battery levels across stations.
- Visualize message loss percentages per station.
- Real-time rain detection using streaming processors.
- Query archived Parquet datasets for long-term trends.

---

## 📦 Technologies Used

- Java 21 / Spring Boot
- Apache Kafka
- Apache Parquet
- Elasticsearch & Kibana
- BitCask (custom implementation)
- Docker & Kubernetes
- Java Flight Recorder (JFR)
## Other Repositories
[Parquet Reader](https://github.com/AhmedAli3011/ParquetIngestor)
