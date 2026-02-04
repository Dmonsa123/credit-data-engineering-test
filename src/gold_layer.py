import pandas as pd
import os

def procesar_gold():
    path_silver = "data/silver/events_silver.parquet"
    path_regions = "data/raw_source/region_reference.csv"
    path_gold = "data/gold/"
    
    os.makedirs(path_gold, exist_ok=True)

    print("--- ✨ Iniciando Capa Gold (Enriquecimiento y KPIs) ---")

    try:
        # 1. Cargar datos de Silver y el Catálogo de Regiones
        df_silver = pd.read_parquet(path_silver)
        df_regions = pd.read_csv(path_regions)

        # 2. Enriquecimiento: Join entre transacciones y regiones
        # Usamos la columna 'region' que está en ambos archivos
        df_gold = pd.merge(df_silver, df_regions, on='region', how='left')

        # 3. Cálculo de KPIs Rápidos
        # Por ejemplo: Exposición financiera por segmento de riesgo
        kpi_riesgo = df_gold.groupby('risk_segment')['outstanding_balance'].sum().reset_index()
        
        # 4. Guardar Tabla Maestra Final
        df_gold.to_parquet(os.path.join(path_gold, "master_credit_data.parquet"), index=False)
        
        # Guardar un CSV de resumen para facilitar la lectura del evaluador
        kpi_riesgo.to_csv(os.path.join(path_gold, "resumen_riesgo.csv"), index=False)

        print(f"✅ Capa Gold completada.")
        print("\n📊 VISTA PREVIA DEL KPI DE RIESGO:")
        print(kpi_riesgo)

    except Exception as e:
        print(f"❌ Error en Gold: {e}")

if __name__ == "__main__":
    procesar_gold()