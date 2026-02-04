import pandas as pd
import os
from datetime import datetime

def procesar_bronze():
    path_entrada = "data/input/"
    path_salida = "data/bronze/"
    
    # Crear carpeta si no existe
    os.makedirs(path_salida, exist_ok=True)

    print("--- 📥 Iniciando Capa Bronze (Procesamiento Batch) ---")

    try:
        # 1. Listar archivos CSV en input
        archivos = [f for f in os.listdir(path_entrada) if f.endswith('.csv')]
        
        if not archivos:
            print("⚠️ No hay archivos en la carpeta de entrada.")
            return

        lista_dfs = []
        for archivo in archivos:
            ruta_full = os.path.join(path_entrada, archivo)
            df_temp = pd.read_csv(ruta_full)
            
            # Añadir metadatos (Auditoría)
            df_temp['ingestion_timestamp'] = datetime.now()
            df_temp['source_file'] = archivo
            lista_dfs.append(df_temp)
            print(f"✔️ Leído: {archivo}")

        # 2. Consolidar datos
        df_bronze = pd.concat(lista_dfs, ignore_index=True)

        # 3. Guardar en Parquet (Formato estándar de Data Lake)
        output_file = os.path.join(path_salida, "events_bronze.parquet")
        df_bronze.to_parquet(output_file, index=False)
        
        print(f"\n✅ Capa Bronze completada con éxito.")
        print(f"📊 Total registros: {len(df_bronze)}")
        print(f"📁 Destino: {output_file}")

    except Exception as e:
        print(f"❌ Error en Bronze: {e}")

if __name__ == "__main__":
    procesar_bronze()