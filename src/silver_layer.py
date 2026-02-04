import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

# Esta es la función que el MAIN está intentando llamar
def procesar_silver():
    # 1. FIX ENTORNO
    if "SPARK_HOME" in os.environ: del os.environ["SPARK_HOME"]
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("Capa_Silver") \
        .config("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY") \
        .getOrCreate()

    try:
        print("✨ Leyendo Capa Bronze para transformación...")
        df_bronze = spark.read.parquet("data/bronze")
        
        # Transformaciones
        df_silver = df_bronze.dropDuplicates(["event_id"])
        df_silver = df_silver.withColumn(
            "loan_status", 
            when(col("loan_status").isNull(), "UNKNOWN").otherwise(col("loan_status"))
        )

        df_silver.write.mode("overwrite").parquet("data/silver")
        print("✅ Capa Silver procesada con éxito.")
        df_silver.show(5)

    finally:
        spark.stop() # CRUCIAL: Cerrar para que Gold pueda abrir su propia sesión

# Esto permite que el script siga funcionando solo si le das PLAY directamente
if __name__ == "__main__":
    procesar_silver()