import os
from data.fetch_data import fetch_and_save_data
from src.complemento_pagos.retencionDR import r

# Obtener el directorio principal del proyecto (pp/)
project_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(project_dir)
print(project_dir)

