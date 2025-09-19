End-to-End Data Pipeline Orchestration with Airflow, Spark, Iceberg & Docker Compose
Overview
This project demonstrates an end-to-end data pipeline setup utilizing Apache Airflow for orchestration, Apache Spark for distributed computation, Apache Iceberg for modern data lake storage, Minio as an S3-compatible object storage, Nessie as an Iceberg catalog, and Jupyter Lab for interactive analysis. All services are deployed together using Docker Compose for ease of development and reproducibility.

Main Components
Apache Airflow: Workflow scheduler and orchestrator

Apache Spark: Distributed data processing engine, executing the Spark-load-to-Iceberg script

Apache Iceberg: Table format for large analytic datasets

Minio: Lightweight S3-compatible storage backend

Nessie: Versioned catalog for Iceberg tables

Jupyter Lab: Notebook interface for interactive data analysis

Prerequisites
Docker & Docker Compose installed

Python 3.x (optional, for scripts or local testing)

How to Run
Clone this repository:

text
git clone https://github.com/your-username/your-repository.git
cd your-repository
Start all services with Docker Compose:

text
docker-compose up
Open Airflow UI at http://localhost:8080 and trigger/run the pipeline DAG.

(Optional) Access Jupyter Lab at http://localhost:8888 for data exploration.

Repository Structure
docker-compose.yaml: Service orchestration for all required components

dags/: Airflow DAG definitions, including orchestration for the Spark-to-Iceberg workflow

scripts/: ETL/ELT scripts such as Spark-load-to-Iceberg

notebooks/: Example Jupyter notebooks for data exploration

Pipeline Execution Example
Trigger the DAG in Airflow UI

Monitor execution and review logs/images in each service

Inspect processed data in Minio, Iceberg, or through Jupyter Lab

References
Inspired by SparkingFlow and official docs for Airflow, Spark, Iceberg

