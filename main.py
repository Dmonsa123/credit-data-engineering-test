import sys
import os
import time

# Aseguramos que el sistema encuentre la carpeta src
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), 'src')))

# 1. FIX DE ENTORNO GLOBAL (Evita el error de winutils y JVM desde el inicio)
if "SPARK_HOME" in os.environ:
    del os.environ["SPARK_HOME"]

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
# Ajusta esta ruta a tu instalación de Hadoop
os.environ["HADOOP_HOME"] = r"C:\Program Files\hadoop"
os.environ["PATH"] += os.pathsep + r"C:\Program Files\hadoop\bin"

# Importamos las funciones de tus capas
try:
    from ingestion import ejecutar_ingesta
    from bronze_layer import procesar_bronze
    from silver_layer import procesar_silver
    from gold_layer import procesar_gold
    from report_viewer import generar_grafico
except ImportError as e:
    print(f"❌ Error al importar módulos: {e}")
    sys.exit(1)

def run_pipeline():
    start_time = time.time()
    
    print("\n" + "="*60)
    print("🚀 SISTEMA DE PROCESAMIENTO DE CRÉDITO - PIPELINE COMPLETO")
    print("="*60)
    
    try:
        # Paso 1: Ingestión (Pandas - División de CSVs)
        print("\n📦 [PASO 1/5] Ingestando datos (Pandas)...")
        ejecutar_ingesta()
        
        # Paso 2: Capa Bronze (Spark - CSV a Parquet)
        print("\n🥉 [PASO 2/5] Procesando Capa Bronze (Raw a Parquet)...")
        procesar_bronze()
        
        # Paso 3: Capa Silver (Spark - Limpieza y Deduplicación)
        print("\n🥈 [PASO 3/5] Procesando Capa Silver (Validación)...")
        procesar_silver()
        
        # Paso 4: Capa Gold (Spark - KPIs y Agregaciones)
        print("\n🥇 [PASO 4/5] Procesando Capa Gold (Negocio)...")
        procesar_gold()
        
        # Paso 5: Reporte Visual (Matplotlib/Seaborn)
        print("\n📊 [PASO 5/5] Generando reporte visual...")
        generar_grafico()
        
        end_time = time.time()
        duracion = round(end_time - start_time, 2)
        
        print("\n" + "="*60)
        print(f"✅ PIPELINE FINALIZADO CON ÉXITO EN {duracion} SEG")
        print(f"📂 Estructura generada: /bronze, /silver, /gold")
        print("="*60 + "\n")
        
    except Exception as e:
        print(f"\n❌ ERROR CRÍTICO DURANTE EL PIPELINE: {str(e)}")
        # Log del error detallado para depuración
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    run_pipeline()