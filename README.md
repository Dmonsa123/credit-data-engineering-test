
# Credit Data Engineering Test - Medallion Architecture

Proyecto de ingeniería de datos que implementa una arquitectura Medallion para el procesamiento de eventos de crédito.

## Arquitectura
1. **Ingestion**: Simulación de micro-batches desde CSV original.
2. **Bronze**: Almacenamiento raw en formato Parquet con metadatos de auditoría.
3. **Silver**: Limpieza de duplicados y validación de integridad (montos > 0).
4. **Gold**: Enriquecimiento con datos de regiones y cálculo de KPIs de riesgo.

## Cómo ejecutar
1. Instalar dependencias: `pip install -r requirements.txt`
2. Ejecutar pipeline en orden:
   - `python src/main.py`
3. Ver reporte: `python src/report_viewer.py`