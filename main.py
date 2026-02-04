import sys
import os
import time

# Aseguramos que el sistema encuentre la carpeta src
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), 'src')))

# Importamos las funciones de tus capas
from ingestion import ejecutar_ingesta
from bronze_layer import procesar_bronze
from silver_layer import procesar_silver
from gold_layer import procesar_gold
from report_viewer import generar_grafico

def run_pipeline():
    start_time = time.time()
    
    print("\n" + "="*50)
    print("🚀 SISTEMA DE PROCESAMIENTO DE CRÉDITO - PIPELINE")
    print("="*50)
    
    try:
        # Paso 1: Ingestión (Mover CSVs a Input)
        print("\n[PASO 1/5] Ingestando datos...")
        ejecutar_ingesta()
        
        # Paso 2: Capa Bronze (Raw a Parquet)
        print("\n[PASO 2/5] Procesando Capa Bronze...")
        procesar_bronze()
        
        # Paso 3: Capa Silver (Limpieza y Validación)
        print("\n[PASO 3/5] Procesando Capa Silver...")
        procesar_silver()
        
        # Paso 4: Capa Gold (Join y Agregaciones)
        print("\n[PASO 4/5] Procesando Capa Gold...")
        procesar_gold()
        
        # Paso 5: Generación de Reporte Visual
        print("\n[PASO 5/5] Generando reporte visual...")
        generar_grafico()
        
        end_time = time.time()
        duracion = round(end_time - start_time, 2)
        
        print("\n" + "="*50)
        print(f"✅ PIPELINE FINALIZADO CON ÉXITO EN {duracion} SEG")
        print(f"📁 Resultados disponibles en: data/gold/ y report/")
        print("="*50 + "\n")
        
    except Exception as e:
        print(f"\n❌ ERROR CRÍTICO DURANTE EL PIPELINE: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    run_pipeline()