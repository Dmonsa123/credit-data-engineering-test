import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, avg, round

def procesar_gold():
    # Fix entorno
    if "SPARK_HOME" in os.environ: del os.environ["SPARK_HOME"]
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("Capa_Gold") \
        .config("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY") \
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem") \
        .getOrCreate()

    try:
        print("🏆 Iniciando proceso Capa Gold...")
        
        # IMPORTANTE: Verifica que esta ruta coincida con la salida de Silver
        path_silver = "data/silver"
        if not os.path.exists(path_silver):
            print(f"⚠️ La carpeta {path_silver} no existe. Creando dummy o revisando...")
            return

        df_silver = spark.read.parquet(path_silver)

        print("📊 Calculando métricas por región...")
        reporte_region = df_silver.groupBy("region").agg(
            count("loan_id").alias("total_prestamos"),
            round(sum("outstanding_balance"), 2).alias("deuda_total"),
            round(avg("interest_rate"), 4).alias("tasa_promedio")
        ).orderBy(col("deuda_total").desc())

        reporte_region.write.mode("overwrite").parquet("data/gold/reporte_regional")
        
        print("✅ Capa Gold completada exitosamente!")
        reporte_region.show()

    except Exception as e:
        print(f"❌ Error en Gold: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    procesar_gold()