# Organizacion

app/
    ├── README.md
    ├── main.py
    ├── requirements.txt
    ├── .env
    ├── .gitignore
    ├──__init__.py
    ├── config/
    │   ├── __init__.py
    │   └── database_config.py  # Configuración de la base de datos (puede ser compartida con otros scripts)
    ├── data/
    │   ├── __init__.py
    │   ├── fetch_data.py       # Módulo para obtener datos de la base de datos (puede ser compartido con otros scripts)
    │   └──process_data.py    # Módulo para procesar los datos extraídos (puede ser compartido con otros scripts)
    ├── src/
    │   ├── script1.py         # Script para una tarea específica (por ejemplo, extracción de datos)
    │   ├── script2.py         # Script para otra tarea específica (por ejemplo, procesamiento de datos)
    │   ├── __init__.py         # Script para otra tarea (por ejemplo, carga de datos en Vertica)
    │   ├── complemento_pagos
    │   │   ├──__init__.py
    │   │   ├──retencionDR.py
    │   │   └── ...
    │   └── ...
    ├── temp/
    ├── tests/
    ├── utils/
    │   ├── extract_keys_from_dicts.py
    │   ├── find_dicts_with_keys.py
    │   └── ...
    └── ...
