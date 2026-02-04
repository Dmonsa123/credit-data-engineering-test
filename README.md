medallion-financial-pipeline/
├── data/                   # Carpeta raíz de datos (Local)
│   ├── raw_source/         # AQUÍ guarda los 2 archivos que te dieron (originales)
│   ├── input/              # Simulación de llegada de archivos (Streaming)
│   ├── bronze/             # Datos crudos en formato Parquet
│   ├── silver/             # Datos limpios y validados
│   ├── gold/               # Agregaciones y KPIs finales
│   └── quarantine/         # Registros que fallaron las reglas de calidad
├── src/                    # Código fuente del pipeline
│   ├── utils/              # Funciones auxiliares (configuración de Spark)
│   ├── ingestion.py        # Simulador de streaming
│   ├── bronze_layer.py     # Procesamiento Raw -> Bronze
│   ├── silver_layer.py     # Procesamiento Bronze -> Silver
│   └── gold_layer.py       # Procesamiento Silver -> Gold
├── report/                 # Visualización de resultados
│   └── dashboard.py        # Aplicación Streamlit o Notebook
├── docs/                   # Documentación adicional
│   └── analisis_tecnico.md # Tu análisis técnico (1-2 páginas)
├── .gitignore              # Para no subir carpetas de datos o temporales
├── README.md               # Guía de uso del proyecto
└── requirements.txt        # Librerías necesarias