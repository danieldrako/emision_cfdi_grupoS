import sys
import os
os.chdir("E:\Desarrollos\extract_json_vertica_python\\app")
# Obtener la ruta absoluta del directorio actual donde se encuentra retencionDR.py
directorio_actual = os.path.dirname(os.path.abspath(__file__))
# Obtener la ruta absoluta del directorio principal (un nivel arriba)
directorio_principal = os.path.abspath(os.path.join(directorio_actual, '../..'))
# Agregar la ruta al directorio principal al sys.path
sys.path.append(directorio_principal)

import pandas as pd
from tqdm import tqdm
tqdm.pandas()
from datetime import datetime

from data.fetch_data import fetch_and_save_data
from data.post_data import post_result
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


start = datetime.now()
print("*************************************************************************************")
print("<<<<<<<<<<<<<<<<<<<<<<<<<| Getting Data |>>>>>>>>>>>>>>>>>>>>>>>>>")
print("<<<<<<<<<<<<<<<<<<<<<<<<<| ", start," |>>>>>>>>>>>>>>>>>>>>>>>>>" )
print("*************************************************************************************")

#pagos = pd.read_csv('temp/FROM_Vertica_Complemento_pago.csv', index=False)

##########################################################################################################    
def process_and_post(pagos):
    if pagos is not None:
        pagos_df = pagos.copy()
        num_data = pagos_df.shape[0]
        next_step = (pagos_df.shape[0] != 0)
    else:
        next_step = False
        print("====================| No data in query |====================")

    if next_step:
        procesados = []
    # # docRelacionado
    #     try:
    #         docRelacionado(next_step, pagos_df)
    #         f1="docRelacionado"
    #         procesados.append(f1)
    #     except Exception as e1:
    #         print("ERROR ON docRelacionado:")
    #         print(e1)
    #     # Pago
    #     try:
    #         pago(next_step, pagos_df)
    #         f2="pago"
    #         procesados.append(f2)
    #     except Exception as e2:
    #         print("ERROR ON pago:")
    #         print(e2)
    #     # Totales
    #     try:
    #         totales(next_step, pagos_df)
    #         f3="totales"
    #         procesados.append(f3)
    #     except Exception as e3:
            
    #         print("ERROR ON totales:")
    #         print(e3)
    #     #retencionDR
    #     try:
    #         retencionDR(next_step, pagos_df)
    #         f6="retencionDR"
    #         procesados.append(f6)
    #     except Exception as e6:
    #         print("ERROR ON funcion6:")
    #         print(e6)
    #     #retencionP
    #     try:
    #         retencionP(next_step, pagos_df)
    #         f7="retencionP"
    #         procesados.append(f7)
    #     except Exception as e7:
    #         print("ERROR ON funcion7:")
    #         print(e7)
    #     #trasladoDR
    #     try:
    #         trasladoDR(next_step, pagos_df)
    #         f5="trasladoDR"
    #         procesados.append(f5)
    #     except Exception as e5:
    #         print("ERROR ON funcion5:")
    #         print(e5)
    #     #trasladoP
    #     try:
    #         trasladoP(next_step, pagos_df)
    #         f4="trasladoP"
    #         procesados.append(f4)
    #     except Exception as e4:
    #         print("ERROR ON trasladoP:")
    #         print(e4)  
              
    end = datetime.now()
    for i,f in enumerate(procesados):
        if i < len(procesados)-1:
            print(f, end=", ")
        else:
            print(f)
    print("_"*80)        
    print(f"===================|Time taken in (hh:mm:ss.ms) in ALL PROCCESS, to  {num_data} datas, was {end - start} |===================")
    print("^"*80)
    print("_"*80)
############################################################
def just_post():
    print("Loading on VERTICA")
    #docRelacionado
    # try:
    #     post_result("DEV_FACTURACION","new_docRelacionado", "temp/docRelacionado_25-09-2023_16-35-45.csv")
    # except Exception as e1:
    #     print("ERROR ON docRelacionado:")
    #     print(e1)
        
    # #Pago
    # try:
    #     post_result("DEV_FACTURACION","new_pago", "temp/pago____.csv")
    # except Exception as e2:
    #     print("ERROR ON pago:")
    #     print(e2)
        
    # #Totales
    # try:
    #     post_result("DEV_FACTURACION","new_totales", "temp/pago____.csv")
    # except Exception as e3:
    #     print("ERROR ON totales:")
    #     print(e3)
        
    # #retencionDR
    # try:
    #     post_result("DEV_FACTURACION","new_retencionDR", "temp/retencionDR____.csv")
    # except Exception as e6:
    #     print("ERROR ON retencionDR:")
    #     print(e6)
        
    # #retencionP
    # try:
    #     post_result("DEV_FACTURACION","new_retencionP", "temp/retencionP____.csv")
    # except Exception as e7:
    #     print("ERROR ON retencionP:")
    #     print(e7)
    
    # #trasladoDR
    # try:
    #     post_result("DEV_FACTURACION","new_trasladoDR", "temp/trasladoDR____.csv")
    # except Exception as e5:
    #     print("ERROR ON trasladoDR:")
    #     print(e5)
        
    # #trasladoP
    # try:
    #     post_result("DEV_FACTURACION","new_trasladoP", "temp/trasladoP____.csv")
    # except Exception as e4::\desarrollos\extract_json_vertica_python\app\src\complemento_pagos\aux_main.py:185: DtypeWarning: Columns (3,4) have mixed types. Specify dtype option on import or set low_memory=False.
    #     print("ERROR ON trasladoP:")
    #     print(e4)  

deci = "just_post"

def just_or_procces(decition):
    if decition == "just_post":
        try:
            just_post()
            print("finished")
        except Exception as e:
            print("ERROR")
            print(e)
        
    elif decition == "procces_and_post":
        process_and_post()
    else:
        pagos = pd.read_csv("temp/docRelacionado_25-09-2023_16-35-45V2.csv")
        print(pagos.head())

just_or_procces(deci)


