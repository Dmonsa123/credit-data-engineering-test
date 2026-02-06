import os
import sys
from pyspark.sql import SparkSession
# Importamos funciones matemáticas específicas de Spark
from pyspark.sql.functions import col, sum, count, avg, round

def procesar_gold():
    # --- 1. CONFIGURACIÓN DE ENTORNO (Hacks para Windows) ---
    if "SPARK_HOME" in os.environ: del os.environ["SPARK_HOME"]
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("Capa_Gold") \
        .config("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY") \
        # ESTA LÍNEA ES CLAVE: Evita errores con el sistema de archivos de Hadoop en Windows
        # Obliga a Spark a usar el sistema de archivos local de forma "cruda" (RawLocal)
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem") \
        .getOrCreate()

    try:
        print("🏆 Iniciando proceso Capa Gold...")
        
        # --- 2. VALIDACIÓN DE ORIGEN ---
        path_silver = "data/silver"
        if not os.path.exists(path_silver):
            # Si Silver no corrió, Gold no tiene nada que hacer. 
            # Esto es un control de seguridad básico.
            print(f"⚠️ La carpeta {path_silver} no existe. Creando dummy o revisando...")
            return

        # Leemos los datos limpios de la capa anterior.
        df_silver = spark.read.parquet(path_silver)

        # --- 3. EL CORAZÓN DEL NEGOCIO (Agregaciones) ---
        print("📊 Calculando métricas por región...")
        
        # groupBy("region"): Junta todos los registros por su zona geográfica.
        reporte_region = df_silver.groupBy("region").agg(
            # count: ¿Cuántos créditos otorgamos en esa región?
            count("loan_id").alias("total_prestamos"),
            
            # sum: ¿Cuánto dinero nos deben en total? (Redondeado a 2 decimales)
            round(sum("outstanding_balance"), 2).alias("deuda_total"),
            
            # avg: ¿Cuál es el interés promedio que estamos cobrando ahí?
            round(avg("interest_rate"), 4).alias("tasa_promedio")
        ).orderBy(col("deuda_total").desc()) # Ponemos arriba la región que más nos debe.

        # --- 4. ALMACENAMIENTO DEL PRODUCTO FINAL ---
        # Guardamos el reporte final en la carpeta Gold.
        # mode("overwrite"): Si ya existía un reporte anterior, lo reemplaza por el más reciente.
        reporte_region.write.mode("overwrite").parquet("data/gold/reporte_regional")
        
        print("✅ Capa Gold completada exitosamente!")
        
        # Imprime el resumen final en la consola (formato tabla).
        reporte_region.show()

    except Exception as e:
        print(f"❌ Error en Gold: {e}")
    finally:
        # IMPORTANTE: Apagar el motor Spark para liberar memoria RAM.
        spark.stop()

if __name__ == "__main__":
    procesar_gold()