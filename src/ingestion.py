import pandas as pd  # Importa la librería Pandas para manipular tablas de datos.
import os           # Importa funciones del sistema operativo (crear carpetas, rutas).
import shutil       # (Opcional aquí) Sirve para operaciones de archivos como copiar o borrar.

# --- CONFIGURACIÓN DE RUTAS ---
# Definimos dónde está el archivo original que queremos procesar.
ORIGEN = "data/raw_source/credit_events.csv"

# Definimos la carpeta donde queremos soltar los pedazos (batches).
DESTINO_FOLDER = "data/input"

def ejecutar_ingesta():
    """
    Función principal que simula la llegada de datos en partes (micro-batches).
    """
    # os.path.exists revisa si la carpeta 'data/input' ya existe en tu computadora.
    if not os.path.exists(DESTINO_FOLDER):
        # Si no existe, os.makedirs la crea (incluyendo carpetas padre si faltan).
        os.makedirs(DESTINO_FOLDER)
    
    print("--- Iniciando Ingestión ---")
    
    try:
        # pd.read_csv lee el archivo original y lo convierte en un DataFrame (una tabla en memoria).
        df = pd.read_csv(ORIGEN)
        
        # len(df) cuenta cuántas filas tiene la tabla.
        # // 3 hace una división entera por 3 (si hay 100 filas, el resultado es 33).
        tercio = len(df) // 3
        
        # Iniciamos un ciclo que se repetirá 3 veces (para i = 0, i = 1, i = 2).
        for i in range(3):
            # Calculamos desde qué fila empezamos a cortar este lote.
            inicio = i * tercio
            
            # Calculamos hasta qué fila cortamos.
            # 'if i < 2' significa: si es el lote 0 o 1, usa el cálculo estándar.
            # 'else len(df)' significa: si es el último lote, vete hasta el final real del archivo.
            fin = (i + 1) * tercio if i < 2 else len(df)
            
            # iloc[inicio:fin] realiza el "corte" físico de las filas seleccionadas.
            lote = df.iloc[inicio:fin]
            
            # Creamos un nombre dinámico: "batch_0.csv", "batch_1.csv", etc.
            nombre_archivo = f"batch_{i}.csv"
            
            # os.path.join junta 'data/input' con 'batch_x.csv' de forma segura para Windows o Linux.
            ruta_final = os.path.join(DESTINO_FOLDER, nombre_archivo)
            
            # Guarda ese pequeño pedazo en un nuevo archivo CSV.
            # index=False evita que se guarde una columna extra con los números de fila.
            lote.to_csv(ruta_final, index=False)
            
            # Mensaje de confirmación en la consola.
            print(f"✅ Archivo creado: {nombre_archivo} con {len(lote)} registros.")
            
    except Exception as e:
        # Si algo falla (ej: no encuentra el archivo), captura el error y lo muestra.
        print(f"❌ Error: {e}")

# Este bloque asegura que la función solo corra si ejecutas este script directamente.
if __name__ == "__main__":
    ejecutar_ingesta()