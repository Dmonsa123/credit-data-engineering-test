import pandas as pd             # Librería para manejar tablas de datos (DataFrames).
import os                      # Librería para interactuar con carpetas y rutas de tu PC.
from datetime import datetime   # Para capturar la fecha y hora exacta del sistema.

def procesar_bronze():
    # Definimos dónde están los CSV (input) y dónde guardaremos el Parquet (bronze).
    path_entrada = "data/input/"
    path_salida = "data/bronze/"
    
    # os.makedirs crea la carpeta 'data/bronze'. 
    # exist_ok=True evita que el programa falle si la carpeta ya estaba creada.
    os.makedirs(path_salida, exist_ok=True)

    print("--- 📥 Iniciando Capa Bronze (Procesamiento Batch) ---")
    #para sacar el icono windows + v

    try:
        # Verificamos si la carpeta de entrada existe antes de intentar leerla.
        if not os.path.exists(path_entrada):
            print(f"⚠️ La carpeta {path_entrada} no existe.")
            return # Detiene la función si no hay nada que leer.

        # LIST COMPREHENSION: Crea una lista con los nombres de archivos que terminan en '.csv'.
        # f es cada nombre de archivo que os.listdir encuentra en la carpeta.
        archivos = [f for f in os.listdir(path_entrada) if f.endswith('.csv')]
        
        # Si la lista está vacía, avisamos y salimos.
        if not archivos:
            print("⚠️ No hay archivos en la carpeta de entrada.")
            return

        # Preparamos una lista vacía para ir guardando los pedazos de datos.
        lista_dfs = []
        
        # Iniciamos un ciclo para procesar cada archivo CSV encontrado.
        for archivo in archivos:
            # Construimos la ruta completa (ej: data/input/batch_0.csv).
            ruta_full = os.path.join(path_entrada, archivo)
            
            # Cargamos el CSV actual en una tabla temporal (df_temp).
            df_temp = pd.read_csv(ruta_full)
            
            # --- AÑADIR METADATOS (Auditoría) ---
            # Creamos una columna con la fecha y hora de este preciso instante.
            # .floor('us') redondea a microsegundos para que Spark no tenga problemas de precisión.
            df_temp['ingestion_timestamp'] = pd.to_datetime(datetime.now()).floor('us')
            
            # Guardamos el nombre del archivo de origen para saber de dónde vino el dato.
            df_temp['source_file'] = archivo
            
            # Agregamos este DataFrame con sus nuevas columnas a nuestra lista colectora.
            lista_dfs.append(df_temp)
            print(f"✔️ Leído: {archivo}")

        # 2. CONSOLIDAR DATOS
        # pd.concat toma todos los DataFrames de la lista y los "pega" uno debajo del otro.
        # ignore_index=True hace que la numeración de filas sea continua (0, 1, 2... hasta el final).
        df_bronze = pd.concat(lista_dfs, ignore_index=True)

        # 3. GUARDAR EN PARQUET
        # Definimos el nombre del archivo final unificado.
        output_file = os.path.join(path_salida, "events_bronze.parquet")
        
        # EL "FIX" PARA SPARK:
        # Guardamos la tabla consolidada en formato binario Parquet.
        df_bronze.to_parquet(
            output_file, 
            index=False,               # No guardamos la columna de índices de filas.
            engine='pyarrow',          # Usamos el motor PyArrow (necesario para manejar tipos complejos).
            coerce_timestamps='us',    # TRUCO: Convierte tiempos a microsegundos (formato nativo de Spark).
            allow_truncated_timestamps=True # Permite eliminar los nanosegundos sobrantes sin dar error.
        )
        
        # Mensajes finales de éxito con estadísticas.
        print(f"\n✅ Capa Bronze completada con éxito.")
        print(f"📊 Total registros: {len(df_bronze)}")
        print(f"📁 Destino: {output_file}")

    except Exception as e:
        # Si algo en el bloque 'try' falla, se ejecuta esto y te dice qué salió mal.
        print(f"❌ Error en Bronze: {e}")

# Punto de entrada del script.
if __name__ == "__main__":
    procesar_bronze()