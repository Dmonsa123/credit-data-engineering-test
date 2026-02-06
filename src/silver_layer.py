import os
import sys
from pyspark.sql import SparkSession      # El cerebro de Spark que coordina todo.
from pyspark.sql.functions import col, when # Herramientas para manipular columnas.

def procesar_silver():
    # --- 1. FIX ENTORNO (Configuración de "fontanería") ---
    # Elimina configuraciones previas de Spark para evitar conflictos en Windows.
    if "SPARK_HOME" in os.environ: del os.environ["SPARK_HOME"]
    
    # Asegura que Spark use el mismo ejecutable de Python que estás usando tú.
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

    # Crea la SparkSession. 
    # .master("local[*]") le dice que use todos los núcleos de tu procesador.
    # .config("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY") es un parche 
    # de compatibilidad para leer fechas antiguas o creadas con otros sistemas.
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("Capa_Silver") \
        .config("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY") \
        .getOrCreate()

    try:
        print("✨ Leyendo Capa Bronze para transformación...")
        
        # Spark no lee archivos uno por uno, lee CARPETAS enteras.
        # Aquí toma el Parquet unificado que creaste en la capa Bronze.
        df_bronze = spark.read.parquet("data/bronze")
        
        # --- TRANSFORMACIONES (Limpieza de datos) ---
        
        # A) ELIMINAR DUPLICADOS: Si un evento de crédito (event_id) aparece dos veces,
        # Spark solo se queda con uno. Esto garantiza la integridad.
        df_silver = df_bronze.dropDuplicates(["event_id"])
        
        # B) TRATAMIENTO DE NULOS:
        # Aquí dice: "Cuando (when) la columna 'loan_status' sea nula (isNull), 
        # ponle 'UNKNOWN'. En caso contrario (otherwise), deja el valor que ya tenía".
        df_silver = df_silver.withColumn(
            "loan_status", 
            when(col("loan_status").isNull(), "UNKNOWN").otherwise(col("loan_status"))
        )

        # C) ESCRITURA:
        # Guardamos el resultado en la carpeta 'data/silver'.
        # .mode("overwrite") borra lo que hubiera antes y escribe lo nuevo.
        df_silver.write.mode("overwrite").parquet("data/silver")
        
        print("✅ Capa Silver procesada con éxito.")
        
        # Muestra una pequeña vista previa (5 filas) en la consola para verificar.
        df_silver.show(5)

    finally:
        # CRUCIAL: Detenemos la sesión de Spark para liberar la memoria RAM.
        # Si no lo haces, el siguiente script (Gold) podría fallar por falta de recursos.
        spark.stop()

if __name__ == "__main__":
    procesar_silver()