import pandas as pd
import os
import shutil

# Rutas
ORIGEN = "data/raw_source/credit_events.csv"
DESTINO_FOLDER = "data/input"

def ejecutar_ingesta():
    # Crear carpeta de destino si no existe
    if not os.path.exists(DESTINO_FOLDER):
        os.makedirs(DESTINO_FOLDER)
    
    print("--- Iniciando Ingestión ---")
    
    try:
        # Cargamos el archivo original
        df = pd.read_csv(ORIGEN)
        
        # Para el ejercicio, vamos a crear 3 archivos pequeños (micro-batches)
        # Esto simula que los datos llegan en tres momentos distintos
        tercio = len(df) // 3
        
        for i in range(3):
            inicio = i * tercio
            fin = (i + 1) * tercio if i < 2 else len(df)
            
            lote = df.iloc[inicio:fin]
            nombre_archivo = f"batch_{i}.csv"
            lote.to_csv(os.path.join(DESTINO_FOLDER, nombre_archivo), index=False)
            print(f"✅ Archivo creado: {nombre_archivo} con {len(lote)} registros.")
            
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    ejecutar_ingesta()