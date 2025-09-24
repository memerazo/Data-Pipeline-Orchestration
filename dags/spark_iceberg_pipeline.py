from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# =========================
# Funciones para cada paso
# =========================

def setup_minio():
    import boto3
    minio_client = boto3.client(
        's3',
        endpoint_url='http://minio:9000',
        aws_access_key_id='admin',
        aws_secret_access_key='password',
        region_name='us-east-1'
    )
    # Aquí podrías validar la conexión o crear buckets si es necesario

def start_spark():
    global spark, conf
    import pyspark
    from pyspark.sql import SparkSession

    CATALOG_URI = "http://nessie:19120/api/v1"
    WAREHOUSE = "s3://gold/"
    STORAGE_URI = "http://172.18.0.2:9000"
    AWS_ACCESS_KEY = 'admin'
    AWS_SECRET_KEY = 'password'

    conf = (
        pyspark.SparkConf()
        .setAppName('combined_spark_app')
        .set('spark.jars.packages', 'org.postgresql:postgresql:42.7.3,'
                                    'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,'
                                    'org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.77.1,'
                                    'software.amazon.awssdk:bundle:2.24.8,'
                                    'software.amazon.awssdk:url-connection-client:2.24.8,'
                                    'org.apache.hadoop:hadoop-aws:3.2.0,'
                                    'com.amazonaws:aws-java-sdk-bundle:1.11.534')
        .set('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,'
                                     'org.projectnessie.spark.extensions.NessieSparkSessionExtensions')
        .set('spark.sql.catalog.nessie', 'org.apache.iceberg.spark.SparkCatalog')
        .set('spark.sql.catalog.nessie.uri', CATALOG_URI)
        .set('spark.sql.catalog.nessie.ref', 'main')
        .set('spark.sql.catalog.nessie.authentication.type', 'NONE')
        .set('spark.sql.catalog.nessie.catalog-impl', 'org.apache.iceberg.nessie.NessieCatalog')
        .set('spark.sql.catalog.nessie.s3.endpoint', STORAGE_URI)
        .set('spark.sql.catalog.nessie.warehouse', WAREHOUSE)
        .set('spark.sql.catalog.nessie.io-impl', 'org.apache.iceberg.aws.s3.S3FileIO')
        .set('spark.hadoop.fs.s3a.endpoint', STORAGE_URI)
        .set('spark.hadoop.fs.s3a.access.key', AWS_ACCESS_KEY)
        .set('spark.hadoop.fs.s3a.secret.key', AWS_SECRET_KEY)
        .set('spark.hadoop.fs.s3a.path.style.access', 'true')
        .set("spark.hadoop.fs.s3a.aws.credentials.provider",
             "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    )
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    print("Spark Session Started")

def read_parquet():
    from pyspark.sql import SparkSession
    spark = SparkSession.getActiveSession()
    global df_taxis
    df_taxis = spark.read.parquet("s3a://bronze/yellow_tripdata_2025-01.parquet")
    df_taxis.printSchema()
    df_taxis.show(5)

def create_namespace():
    from pyspark.sql import SparkSession
    spark = SparkSession.getActiveSession()
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.gold;").show()

def write_to_iceberg():
    from pyspark.sql import SparkSession
    spark = SparkSession.getActiveSession()
    global df_taxis
    df_taxis.writeTo("nessie.gold.yellowtrip").createOrReplace()

# =========================
# Definición del DAG
# =========================

with DAG(
    dag_id='spark_iceberg_dag',
    default_args=default_args,
    description='Carga de datos Parquet a Iceberg usando Spark, Nessie y MinIO',
    schedule_interval=None,
    start_date=datetime(2025, 9, 24),
    catchup=False,
    tags=['spark', 'iceberg', 'nessie', 'minio'],
) as dag:

    t1 = PythonOperator(
        task_id='setup_minio',
        python_callable=setup_minio,
    )

    t2 = PythonOperator(
        task_id='start_spark',
        python_callable=start_spark,
    )

    t3 = PythonOperator(
        task_id='read_parquet',
        python_callable=read_parquet,
    )

    t4 = PythonOperator(
        task_id='create_namespace',
        python_callable=create_namespace,
    )

    t5 = PythonOperator(
        task_id='write_to_iceberg',
        python_callable=write_to_iceberg,
    )

    t1 >> t2 >> t3 >> t4 >> t5