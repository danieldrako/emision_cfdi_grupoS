import sys
import os
#os.chdir("E:\Desarrollos\extract_json_vertica_python\\app")
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
from utils.parallel_to_dataframe import to_dataframe_parallel
from utils.re_build_df import re_build_df
from tools.complemento_pagos.trasladoDR import transform_jsonrow as tjr
from config.constant.complemento_pagos import trasladoDR_vars as tdrv
from config.constant.complemento_pagos import gvars as gv
from utils.search_key import function_existe
from utils.mytime import get_my_time
from data.post_data import post_result
#!##########################################Llamado a la base de datos############################################
#!########################################## Fuente de los datos  ################################################
#Variables
query = tdrv.query
cols_= tdrv.cols_
output_file = tdrv.output_file
regular_exp = tdrv.regular_exp
table_name = gv.tables_dest["trasladoDR"]
schema = gv.schema

#!#################################################################################################################

# #*##################################GuardarData frame procesado en local##########################################
def trasladoDR(next_step, pagos_df, carpeta):
    print("_"*80)
    print("\/"*60)
    print("="*80)
    print("")
    print("==============================| TrasladoDR |=============================")
    print("==============================| Processing |======================================")
    print("_"*80)
    print("/\\"*60)
    print("="*80)
    if next_step:
        start = datetime.now()
        start_proces =datetime.now()
        cpagos_df=pagos_df.copy()
        cpagos_df = function_existe(cpagos_df, regular_exp) 
        traslados_df = tjr.functionThree_ImpuestosDRCF(cpagos_df)
        traslado = tjr.functionTwo_TrasladoDRCF(traslados_df)
        traslado_df = pd.DataFrame(traslado)
        print(traslado_df.shape)
        
        traslado_reb_df = re_build_df(traslado_df, tdrv.ordered_cols, tdrv.float_cols,
                                    tdrv.str_cols, tdrv.int_cols, new_name_tfduuid=tdrv.new_name_tfduuid,
                                    to_delete=tdrv.to_delete)
        print(traslado_reb_df.shape)
        print(traslado_reb_df.head(3))
        
        name_path = tdrv.csv_file_path_to_post
        
        name_path = carpeta+"/"+name_path
        
        my_date = get_my_time()
        exten = ".csv"
        file_to_post = name_path+"_"+my_date+exten
        traslado_reb_df.to_csv(file_to_post, index=False, sep='|')
        end_proces = datetime.now()
        print("*************************************************************************************")
        print(f"Time taken in (hh:mm:ss.ms) to process and save as {file_to_post} data is {end_proces - start_proces}")
        print("*************************************************************************************")
        
    else:
        print("==> ==> ==> No data to process on TrasladoDR <== <== <==")

    # # #*###################################*###################################*###################################*####

    # # ###########################?

    if next_step: 
        print(f"==============================| Uploading on  {schema}.{table_name} |=============================")
        post_result(schema, table_name, file_to_post)#Subir datos a vertica 
        end = datetime.now()
        print("==============================| trasladoDR finished |=============================")
        print("*************************************************************************************")
        print(f"Time taken in (hh:mm:ss.ms) to process and save data is {end - start}")
        print("*************************************************************************************")
    else:
        print("==> ==> ==> No data to post on TrasladoDR <== <== <==")

        
