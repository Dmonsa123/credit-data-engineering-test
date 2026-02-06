import pandas as pd         # Para leer el resultado final.
import matplotlib.pyplot as plt # La librería estándar para crear gráficas.
import os                   # Para manejar carpetas.
import glob                 # MUY IMPORTANTE: Para buscar archivos con patrones (como *.parquet).

def generar_grafico():
    print("📊 Generando visualización final...")
    path_gold = "data/gold/reporte_regional"
    
    # Verificamos que la capa Gold haya hecho su trabajo.
    if not os.path.exists(path_gold):
        print("❌ No hay datos en Gold para graficar.")
        return

    try:
        # --- EL TRUCO DE GLOB ---
        # Spark no guarda un archivo llamado "datos.parquet".
        # Spark guarda una CARPETA y adentro archivos con nombres raros como "part-0000...parquet".
        # glob.glob busca cualquier archivo que termine en .parquet dentro de esa carpeta.
        parquet_files = glob.glob(f"{path_gold}/*.parquet")
        
        if not parquet_files:
            print("❌ No se encontraron archivos parquet en Gold.")
            return

        # Leemos el primer archivo parquet que encuentre la lista.
        # Como nuestro reporte Gold es pequeño (pocas regiones), con un solo archivo basta.
        df = pd.read_parquet(parquet_files[0])
        
        # --- CONFIGURACIÓN DE LA GRÁFICA ---
        # figsize: Define el tamaño de la imagen (10 pulgadas de ancho, 6 de alto).
        plt.figure(figsize=(10, 6))
        
        # kind='bar': Crea un gráfico de barras.
        # x='region', y='total_prestamos': Ejes de la gráfica.
        df.plot(kind='bar', x='region', y='total_prestamos', color='skyblue')
        
        # Etiquetas y Títulos
        plt.title('Préstamos por Región')
        plt.xlabel('Región')
        plt.ylabel('Cantidad')
        
        # rotation=45: Inclina los nombres de las regiones para que no se amontonen.
        plt.xticks(rotation=45)
        
        # tight_layout: Ajusta automáticamente los márgenes para que nada se corte.
        plt.tight_layout()
        
        # --- GUARDADO ---
        # Creamos una carpeta nueva llamada 'report' para separar las imágenes de los datos.
        os.makedirs("report", exist_ok=True)
        plt.savefig("report/reporte_regional.png")
        
        print("✅ Gráfico guardado en report/reporte_regional.png")
        
    except Exception as e:
        print(f"❌ Error al generar gráfico: {e}")

if __name__ == "__main__":
    generar_grafico()