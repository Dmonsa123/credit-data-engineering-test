import pandas as pd
import matplotlib.pyplot as plt
import os
import glob

def generar_grafico():
    print("📊 Generando visualización final...")
    path_gold = "data/gold/reporte_regional"
    
    if not os.path.exists(path_gold):
        print("❌ No hay datos en Gold para graficar.")
        return

    try:
        # Spark guarda archivos repartidos, buscamos el archivo .parquet real
        parquet_files = glob.glob(f"{path_gold}/*.parquet")
        if not parquet_files:
            print("❌ No se encontraron archivos parquet en Gold.")
            return

        # Leemos con Pandas (requiere pip install pyarrow)
        df = pd.read_parquet(parquet_files[0])
        
        # Crear gráfico
        plt.figure(figsize=(10, 6))
        df.plot(kind='bar', x='region', y='total_prestamos', color='skyblue')
        plt.title('Préstamos por Región')
        plt.xlabel('Región')
        plt.ylabel('Cantidad')
        plt.xticks(rotation=45)
        plt.tight_layout()
        
        # Guardar y mostrar
        os.makedirs("report", exist_ok=True)
        plt.savefig("report/reporte_regional.png")
        print("✅ Gráfico guardado en report/reporte_regional.png")
        # plt.show() # Opcional: muestra la ventana si estás en local
        
    except Exception as e:
        print(f"❌ Error al generar gráfico: {e}")

if __name__ == "__main__":
    generar_grafico()