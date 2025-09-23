from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

spark_params = {
    "conn_id": "spark_default",
    "application": "/opt/airflow/jobs/process_taxi_data.py",  # tu script PySpark
    "deploy_mode": "client",
    "packages": (
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,"
        "org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.77.1,"
        "org.apache.iceberg:iceberg-aws-bundle:1.5.0"
    ),
    "verbose": True,
    "conf": {
        "spark.master": "spark://spark-master:7077"  # 🔹 aquí fuerzas el master
    }
}

process_taxi_data = SparkSubmitOperator(
    task_id='process_taxi_data',
    application='/opt/airflow/jobs/process_taxi_data.py',
    conn_id='spark_default',
    verbose=True,
    conf={
        'spark.master': 'spark://spark-master:7077',   # Fuerza Spark standalone
        'spark.jars.packages': 'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.77.1,org.apache.iceberg:iceberg-aws-bundle:1.5.0',
        'spark.sql.extensions': 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions',
        'spark.sql.catalog.nessie': 'org.apache.iceberg.spark.SparkCatalog',
        'spark.sql.catalog.nessie.uri': 'http://nessie:19120/api/v1',
        'spark.sql.catalog.nessie.ref': 'main',
        'spark.sql.catalog.nessie.authentication.type': 'NONE',
        'spark.sql.catalog.nessie.catalog-impl': 'org.apache.iceberg.nessie.NessieCatalog',
        'spark.sql.catalog.nessie.s3.endpoint': 'http://minio:9000',
        'spark.sql.catalog.nessie.warehouse': 's3://gold/',
        'spark.sql.catalog.nessie.io-impl': 'org.apache.iceberg.aws.s3.S3FileIO',
        'spark.hadoop.fs.s3a.endpoint': 'http://minio:9000',
        'spark.hadoop.fs.s3a.access.key': 'admin',
        'spark.hadoop.fs.s3a.secret.key': 'password',
        'spark.hadoop.fs.s3a.path.style.access': 'true'
    }
)


with DAG(
    'spark_iceberg_pipeline',
    default_args=default_args,
    description='Orquestacion del pipeline de Spark-iceberg',
    schedule_interval=timedelta(days=1),
    catchup=False
) as dag:

    def check_nessie_connection():
        ''' Verifica que nessie este disponible '''
        import requests
        import logging

        logger = logging.getLogger(__name__)

        try:
            response = requests.get("http://nessie:19120/api/v1/config", timeout=30)
            if response.status_code == 200:
                print("Nessie check")
                return True
            else:
                logger.error(f"Nessie respondió con código HTTP {response.status_code}")
                logger.error(f"Respuesta: {response.text[:200]}...")
                return False
                           
        except requests.exceptions.ConnectionError:
            logger.error("No se puede conectar a Nessie. Verifica:")
            logger.error("1. Que el servicio 'nessie' esté ejecutándose")
            logger.error("2. Que el puerto 19120 esté expuesto")
            logger.error("3. Que la red Docker esté configurada correctamente")
            return False

    def check_minio_connection():

        '''Verificar que MinIO este disponible''' 
        import boto3
        from botocore.exceptions import ClientError
        try:
            minio_client = boto3.client(
                's3',
                endpoint_url='http://minio:9000',
                aws_access_key_id='admin',
                aws_secret_access_key='password',
                region_name='us-east-1'
                )
            
            buckets = minio_client.list_buckets()
            print("Minio Disponible")
            return True

        except ClientError as e:
            raise Exception(f"error conectando a MinIO: {str(e)}")

    check_connections = PythonOperator(
        task_id='check_connections',
        python_callable=lambda: check_nessie_connection() and check_minio_connection(),
    )

    # Proceso los datos de los taxis
    process_taxi_data = SparkSubmitOperator(
        task_id='process_taxi_data',
        application='/opt/airflow/jobs/process_taxi_data.py',
        conn_id='spark_default',
        verbose=True,
        master='spark://spark-master:7077',
        conf={
            'spark.jars.packages': (
                'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,'
                'org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.90.1,'
                'org.apache.hadoop:hadoop-aws:3.3.4,'
                'com.amazonaws:aws-java-sdk-bundle:1.12.262'
            ),
            'spark.sql.extensions': 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions',
            'spark.sql.catalog.nessie': 'org.apache.iceberg.spark.SparkCatalog',
            'spark.sql.catalog.nessie.uri': 'http://nessie:19120/api/v1',
            'spark.sql.catalog.nessie.ref': 'main',
            'spark.sql.catalog.nessie.authentication.type': 'NONE',
            'spark.sql.catalog.nessie.catalog-impl': 'org.apache.iceberg.nessie.NessieCatalog',
            'spark.sql.catalog.nessie.s3.endpoint': 'http://minio:9000',
            'spark.sql.catalog.nessie.warehouse': 's3://gold/',
            'spark.sql.catalog.nessie.io-impl': 'org.apache.iceberg.aws.s3.S3FileIO',
            'spark.hadoop.fs.s3a.endpoint': 'http://minio:9000',
            'spark.hadoop.fs.s3a.access.key': 'admin',
            'spark.hadoop.fs.s3a.secret.key': 'password',
            'spark.hadoop.fs.s3a.path.style.access': 'true'
        }
    )

end = EmptyOperator(task_id='end')
check_connections >> process_taxi_data >> end