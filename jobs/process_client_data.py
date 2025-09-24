from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, current_date

def main():
    spark = SparkSession.builder \
        .appName("ProcessClientData") \
        .getOrCreate()

    # Aquí iría el código de procesamiento de datos de clientes
    # similar al que tienes en Ext-Load-Data-Clientes-Spark-Iceberg-Minio.ipynb
    
    print("✅ Datos de clientes procesados exitosamente")
    spark.stop()

if __name__ == "__main__":
    main()
