import pandas as pd
import os

def procesar_silver():
    path_bronze = "data/bronze/events_bronze.parquet"
    path_silver = "data/silver/"
    path_quarantine = "data/quarantine/"
    
    os.makedirs(path_silver, exist_ok=True)
    os.makedirs(path_quarantine, exist_ok=True)

    print("--- 🧼 Iniciando Capa Silver (Limpieza y Calidad) ---")

    try:
        # 1. Leer de Bronze
        df = pd.read_parquet(path_bronze)
        total_inicial = len(df)

        # 2. Eliminar duplicados por event_id
        df = df.drop_duplicates(subset=['event_id'])
        duplicados = total_inicial - len(df)

        # 3. Aplicar Reglas de Calidad
        # Regla: Monto principal > 0 y loan_id presente
        mask_valido = (df['principal_amount'] > 0) & (df['loan_id'].notna())
        
        df_silver = df[mask_valido].copy()
        df_quarantine = df[~mask_valido].copy()

        # 4. Guardar resultados
        df_silver.to_parquet(os.path.join(path_silver, "events_silver.parquet"), index=False)
        df_quarantine.to_parquet(os.path.join(path_quarantine, "failed_records.parquet"), index=False)

        print(f"✅ Capa Silver completada.")
        print(f"📊 Resumen de Calidad:")
        print(f"   - Registros Válidos: {len(df_silver)}")
        print(f"   - En Cuarentena: {len(df_quarantine)}")
        print(f"   - Duplicados eliminados: {duplicados}")

    except Exception as e:
        print(f"❌ Error en Silver: {e}")

if __name__ == "__main__":
    procesar_silver()