import sys
import os
#os.chdir("../../") <=Descomentar
os.chdir("E:\\Desarrollos\\extract_json_vertica_python\\app_to_server\\app\\")######<====Por comentar
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
from utils.parallel_to_dataframe import to_dataframe_parallel
from config.constant.complemento_pagos import gvars as gvc
from src.complemento_pagos.docRelacionado import docRelacionado
from src.complemento_pagos.pago import pago
from src.complemento_pagos.totales import totales
from src.complemento_pagos.trasladoP import trasladoP
from src.complemento_pagos.trasladoDR import trasladoDR
from src.complemento_pagos.retencionDR import retencionDR
from src.complemento_pagos.retencionP import retencionP

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
    
pagos = fetch_and_save_data(query=query)
end_fetch = datetime.now()
print("*************************************************************************************")
print(f"Time taken in (hh:mm:ss.ms) to get data is {end_fetch - start}")
print("*************************************************************************************")
    
#pagos = pd.read_csv('E:\\Desarrollos\\extract_json_vertica_python\\app\\temp\\retencionDR.csv')
if pagos is not None:
    pagos_df = pd.DataFrame(pagos, columns=cols_)
    end_parallel = datetime.now()
    num_data=(pagos_df.shape[0])
    print("*************************************************************************************")
    print(f"Time taken in (hh:mm:ss.ms) to parallel data is {end_parallel - end_fetch}")
    print("*************************************************************************************")
    
    output_file = nueva_carpeta+"/"+output_file
    
    pagos_df.to_csv(output_file, index=False)
    next_step = (pagos_df.shape[0] != 0)
else:
    next_step = False
    print("====================| No data in query |====================")

if next_step:
    procesados = []
# docRelacionado
    try:
        docRelacionado(next_step, pagos_df, nueva_carpeta)
        f1="docRelacionado"
        procesados.append(f1)
    except Exception as e1:
        print("ERROR ON docRelacionado:")
        print(e1)
    # Pago
    try:
        pago(next_step, pagos_df, nueva_carpeta)
        f2="pago"
        procesados.append(f2)
    except Exception as e2:
        print("ERROR ON pago:")
        print(e2)
    # Totales
    try:
        totales(next_step, pagos_df, nueva_carpeta)
        f3="totales"
        procesados.append(f3)
    except Exception as e3:
        print("ERROR ON totales:")
        print(e3)
    #trasladoDR
    try:
        trasladoDR(next_step, pagos_df, nueva_carpeta)
        f4="trasladoDR"
        procesados.append(f4)
    except Exception as e4:
        print("ERROR ON funcion4:")
        print(e4)
    #trasladoP
    try:
        trasladoP(next_step, pagos_df, nueva_carpeta)
        f5="trasladoP"
        procesados.append(f5)
    except Exception as e5:
        print("ERROR ON trasladoP:")
        print(e5)

    # #retencionDR
    # try:
    #     retencionDR(next_step, pagos_df, nueva_carpeta)
    #     f6="retencionDR"
    #     procesados.append(f6)
    # except Exception as e6:
    #     print("ERROR ON funcion6:")
    #     print(e6)

    # #retencionP
    # try:
    #     retencionP(next_step, pagos_df, nueva_carpeta)
    #     f7="retencionP"
    #     procesados.append(f7)
    # except Exception as e7:
    #     print("ERROR ON funcion7:")
    #     print(e7)
        

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
    
