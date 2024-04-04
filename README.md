# REAL-TIME DATA STREAMING PROJECT 
**Author** ~Thanh Dinh  
**Start date**: 02/04/2023  
**Description**: ...  
<!-- https://www.youtube.com/watch?v=GqAcTrqKcrY&ab_channel=CodeWithYu -->


### Architecture
![Data Engineering Architecture](./img/DE_Architecture.png)


## Components:
- **Data Source**: We use randomuser.me API to generate random user data for our pipeline.
- **Apache Airflow**: Responsible for orchestrating the pipeline and storing fetched data in a PostgreSQL database.
- **Apache Kafka and Zookeeper**: Used for streaming data from PostgreSQL to the processing engine.
- **Control Center and Schema Registry**: Helps in monitoring and schema management of our Kafka streams.
- **Apache Spark**: For data processing with its master and worker nodes.
- **Cassandra**: Where the processed data will be stored.

## What I've learned
- Setting up a data pipeline with Apache Airflow
- Real-time data streaming with Apache Kafka
- Distributed synchronization with Apache Zookeeper
- Data processing techniques with Apache Spark
- Data storage solutions with Cassandra and PostgreSQL
- Containerizing your entire data engineering setup with Docker





## Getting Started

1. Install required dependencies:
```
pip install apache-airflow kafka-python requests pyspark
```

2. Run Docker Compose to spin up the services:
```
docker-compose up
```