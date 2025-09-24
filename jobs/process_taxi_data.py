from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
        .appName("ProcessTaxiData") \
        .getOrCreate()

    # Leer datos de taxis
    df_taxis = spark.read.parquet("s3a://bronze/yellow_tripdata_2025-01.parquet")
    
    # Crear namespace si no existe
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.gold").show()
    
    # Escribir a Iceberg
    df_taxis.writeTo("nessie.gold.yellowtrip").createOrReplace()
    
    print("✅ Datos de taxis procesados exitosamente")
    spark.stop()

if __name__ == "__main__":
    main()
