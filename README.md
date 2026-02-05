# 💳 Credit Data Engineering Test - Medallion Architecture

Proyecto de ingeniería de datos que implementa una arquitectura Medallion para el procesamiento de eventos de crédito.

## 🛠️ Desafíos Técnicos Resueltos

* **Compatibilidad de Esquemas (Pandas ↔ Spark)**: Se resolvió el error de tipos `INT64 (TIMESTAMP_NANOS)` forzando la conversión a microsegundos (`coerce_timestamps='us'`) en la capa Bronze, asegurando que el motor de Spark en la JVM pueda procesar los archivos sin errores de esquema.
* **Optimización en Windows**: Configuración dinámica de variables de entorno para una ejecución fluida de PySpark en entornos locales, gestionando correctamente el `SPARK_HOME` y la compatibilidad con Python 3.10.

## 📂 Estructura del Proyecto

```text
├── data/               # Repositorio local de datos (Ignorado por Git)
├── report/             # Salida de visualizaciones (Gráficos PNG)
├── src/
│   ├── ingestion.py    # Simulación de ingesta (CSV)
│   ├── bronze_layer.py # Transformación Raw a Parquet (Fix de Timestamps)
│   ├── silver_layer.py # Limpieza y validación con PySpark
│   ├── gold_layer.py   # Agregaciones de negocio y KPIs
│   └── report_viewer.py# Generación de reporte visual
├── main.py             # Orquestador principal del pipeline completo
├── requirements.txt    # Dependencias del proyecto
└── .gitignore          # Exclusión de archivos temporales y datos pesados
```
# 🚀 Cómo Ejecutar

## 1. Preparar el entorno
Se recomienda usar un entorno virtual de Python 3.10 o superior:

```pip install -r requirements.txt```

## 2. Ejecutar el Pipeline Completo
El proyecto incluye un orquestador ```(main.py)``` que ejecuta todas las capas de forma secuencial:

```python main.py```

## 3. Resultados

Los logs de la terminal mostrarán el progreso de cada capa (Bronze -> Silver -> Gold).
El análisis visual final se generará en la ruta: ```report/reporte_regional.png```.

---
**Desarrollado por:** David Fernando Monsalve

 **Tecnologías:**  Python | PySpark | Pandas | Parquet | Matplotlib