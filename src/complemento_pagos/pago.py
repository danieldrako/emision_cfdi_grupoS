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
from tools.complemento_pagos.pago import transform_jsonrow as tjr
from config.constant.complemento_pagos import pago_vars as pdr
from config.constant.complemento_pagos import gvars as gv
from utils.search_key import function_existe
from utils.mytime import get_my_time
from data.post_data import post_result
#!##########################################Llamado a la base de datos############################################
#!########################################## Fuente de los datos  ################################################
#Variables
query = pdr.query
cols_= pdr.cols_
output_file = pdr.output_file

regular_exp = pdr.regular_exp
table_name = gv.tables_dest["pago"]
schema = gv.schema
#!#################################################################################################################

# #*##################################GuardarData frame procesado en local##########################################
def pago(next_step, pagos_df, carpeta):
    print("_"*80)
    print("\/"*60)
    print("="*80)
    print("")
    print("==============================| Pago |=============================")
    print("===========================| Processing |===================================")
    print("_"*80)
    print("/\\"*60)
    print("="*80)
    start = datetime.now()
    if next_step:
        start_proces =datetime.now() 
        cpagos_df=pagos_df.copy()
        pago = tjr.functionTwo_PagoCF(cpagos_df)
        pago_df = pd.DataFrame(pago)
        print(pago_df.shape)

        pago_reb_df = re_build_df(pago_df, pdr.ordered_cols, pdr.float_cols,
                                    pdr.str_cols, pdr.int_cols, new_name_tfduuid=pdr.new_name_tfduuid,
                                    to_delete=pdr.to_delete)
        print(pago_reb_df.head(3))
        print( pago_reb_df.shape)

        name_path = pdr.csv_file_path_to_post
        
        name_path = carpeta+"/"+name_path
        
        my_date = get_my_time()
        exten = ".csv"
        file_to_post = name_path+"_"+my_date+exten
        pago_reb_df .to_csv(file_to_post, index=False, sep='|')
        end_proces = datetime.now()
        print("*************************************************************************************")
        print(f"Time taken in (hh:mm:ss.ms) to process and save as {file_to_post} data is {end_proces - start_proces}")
        print("*************************************************************************************")
        
    else:
        print("No data to process on pago")

    #*###################################*###################################*###################################*####

    if next_step:
        print(f"==============================| Uploading on  {schema}.{table_name} |=============================")
        post_result(schema, table_name, file_to_post)#Subir datos a vertica 
        end = datetime.now()
        print("==============================| pago finished |=============================")
        print("*************************************************************************************")
        print(f"Time taken in (hh:mm:ss.ms) to process and save data is {end - start}")
        print("*************************************************************************************")
    else:
        print("==> ==> ==> No data to post on Pago <== <== <==")
        
        
