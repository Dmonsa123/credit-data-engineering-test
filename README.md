
# Credit Data Engineering Test - Medallion Architecture

Proyecto de ingeniería de datos que implementa una arquitectura Medallion para el procesamiento de eventos de crédito.

## Arquitectura
1. **Ingestion**: Simulación de micro-batches desde CSV original.
2. **Bronze**: Almacenamiento raw en formato Parquet con metadatos de auditoría.
3. **Silver**: Limpieza de duplicados y validación de integridad (montos > 0).
4. **Gold**: Enriquecimiento con datos de regiones y cálculo de KPIs de riesgo.

├── data/               # Repositorio de datos (Bronze, Silver, Gold)
├── report/             # Salida de visualizaciones finales
├── src/
│   ├── ingestion.py    # Generación de datos iniciales
│   ├── bronze_layer.py # Transformación CSV a Parquet
│   ├── silver_layer.py # Limpieza y validación con Spark
│   ├── gold_layer.py   # Agregaciones de negocio
│   └── report_viewer.py# Generación de gráficos (Matplotlib/Pandas)
├── main.py             # Orquestador principal del pipeline
└── requirements.txt    # Dependencias del proyecto
## Cómo ejecutar
1. Instalar dependencias: `pip install -r requirements.txt`
2. Ejecutar pipeline en orden:
   - `python main.py`
3. Ver reporte: `python src/report_viewer.py`