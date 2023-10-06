import sys
import os
#os.chdir("../../") <=Descomentar
os.chdir("E:\\Desarrollos\\extract_json_vertica_python\\app_to_server\\cfdi_app\\")######<====Por comentar
# Obtener la ruta absoluta del directorio actual donde se encuentra retencionDR.py
directorio_actual = os.path.dirname(os.path.abspath(__file__))
# Obtener la ruta absoluta del directorio principal (un nivel arriba)
#directorio_principal = os.path.abspath(os.path.join(directorio_actual, './')) <== Descomentar
directorio_principal = os.path.abspath(os.path.join(directorio_actual, '../..'))###########<===Por comentar

# Agregar la ruta al directorio principal al sys.path
sys.path.append(directorio_principal)
print("directorio actual")
print(directorio_actual)
print("directorio principal")
print(directorio_principal)

import pandas as pd
from tqdm import tqdm
tqdm.pandas()
from datetime import datetime

from data.fetch_data import fetch_and_save_data
from config.constant.cfdi import gvars as gvc
from src.cfdi.cfdiRelacionados import cfdiRel

#!##########################################Llamado a la base de datos############################################
#!########################################## Fuente de los datos  ################################################
#Variables
query = gvc.query
cols_= gvc.cols_
output_file = gvc.output_file
start = datetime.now()

nueva_carpeta = start.strftime("%d-%m-%Y_%H-%M-%S" )
nueva_carpeta = "temp/"+nueva_carpeta

print("*************************************************************************************")
print("<<<<<<<<<<<<<<<<<<<<<<<<<| Getting Data |>>>>>>>>>>>>>>>>>>>>>>>>>")
print("<<<<<<<<<<<<<<<<<<<<<<<<<| ", start," |>>>>>>>>>>>>>>>>>>>>>>>>>" )
print("*************************************************************************************")

if not os.path.exists(nueva_carpeta):
    os.makedirs(nueva_carpeta)
    print(f"Carpeta '{nueva_carpeta}' creada con éxito.")
else:
    print(f"La carpeta '{nueva_carpeta}' ya existe.")
    
cfdi = fetch_and_save_data(query=query)
end_fetch = datetime.now()
print("*************************************************************************************")
print(f"Time taken in (hh:mm:ss.ms) to get data is {end_fetch - start}")
print("*************************************************************************************")
    
#cfdi = pd.read_csv('E:\\Desarrollos\\extract_json_vertica_python\\app\\temp\\retencionDR.csv')
if cfdi is not None:
    cfdi_df = pd.DataFrame(cfdi, columns=cols_)
    end_parallel = datetime.now()
    num_data=(cfdi_df.shape[0])

    
    output_file = nueva_carpeta+"/"+output_file
    cfdi_df.to_csv(output_file, index=False)
    next_step = (cfdi_df.shape[0] != 0)
else:
    next_step = False
    print("====================| No data in query |====================")

if next_step:
    procesados = []
# docRelacionado
    try:
        cfdiRel(next_step, cfdi_df, nueva_carpeta)
        f1="cfdi"
        procesados.append(f1)
    except Exception as e1:
        print("ERROR ON cfdi:")
        print(e1)
        

    for i,f in enumerate(procesados):
        if i < len(procesados)-1:
            print(f, end=", ")
        else:
            print(f)
else:
    num_data = 0
    
end = datetime.now()
print("_"*80)        
print(f"===================|Time taken in (hh:mm:ss.ms) in ALL PROCCESS, to  {num_data} datas, was {end - start} |===================")
print("^"*80)
print("_"*80)
    
