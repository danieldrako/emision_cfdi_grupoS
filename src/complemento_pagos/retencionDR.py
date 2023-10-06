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
from tools.complemento_pagos.retencionDR import transform_jsonrowV3 as tjr
from config.constant.complemento_pagos import retencionDR_vars as rdrv
from utils.search_key import function_existe
from utils.mytime import get_my_time
from data.post_data import post_result
#!##########################################Llamado a la base de datos############################################
#!########################################## Fuente de los datos  ################################################
#Variables
query = rdrv.query
cols_= rdrv.cols_
output_file = rdrv.output_file
regular_exp = rdrv.regular_exp
start = datetime.now()

# print("*************************************************************************************")
# print(">>>>>>>>>>>>>>>>>>>>>>>>>>>| GETTING DATA |<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")
# print("*************************************************************************************")
# print(start)    
# pagos = fetch_and_save_data(query=query)
# end_fetch = datetime.now()
# print("*************************************************************************************")
# print(f"Time taken in (hh:mm:ss.ms) to get data is {end_fetch - start}")
# print("*************************************************************************************")
    
# #pagos = pd.read_csv('E:\\Desarrollos\\extract_json_vertica_python\\app\\temp\\retencionDR.csv')
# if pagos is not None:
#     pagos_df = to_dataframe_parallel(pagos, cols_)
#     end_parallel = datetime.now()
#     print("*************************************************************************************")
#     print(f"Time taken in (hh:mm:ss.ms) to parallel data is {end_parallel - end_fetch}")
#     print("*************************************************************************************")
#     pagos_df.to_csv(r"temp/retencionDR.csv", index=False)
#     pagos_df = function_existe(pagos_df, regular_exp)
#     pagos_df.to_csv(r"temp/retencionDRFILTRADO.csv", index=False)
#     print(pagos_df.head())
#     print("Data retrieved and saved successfully.")
    
#     next_step = (pagos_df.shape[0] != 0)
# else:
#     next_step = False

#!#################################################################################################################

# #*##################################GuardarData frame procesado en local##########################################
def retencionDR(next_step, pagos_df):
    print("_"*80)
    print("\/"*60)
    print("="*80)
    print("")
    print("==============================| RetencionDR |=============================")
    print("_"*80)
    print("/\\"*60)
    print("="*80)
    if next_step:
        start_proces =datetime.now() 
        cpagos_df=pagos_df.copy()
        cpagos_df = function_existe(cpagos_df, regular_exp) 
        retencions_df = tjr.functionThree_ImpuestosDRCF(cpagos_df)
        retencion = tjr.functionTwo_RetencionDRCF(retencions_df)
        retencion_df = pd.DataFrame(retencion)
        print(retencion_df.shape)
        #print(retencion_df.head(4))
        

        retencion_reb_df = re_build_df(retencion_df, rdrv.ordered_cols, rdrv.float_cols,
                                    rdrv.str_cols, rdrv.int_cols, new_name_tfduuid=rdrv.new_name_tfduuid,
                                    to_delete=rdrv.to_delete)

        print(retencion_reb_df.shape)
        #date_to_save = get_my_time()
        
        name_path = rdrv.csv_file_path_to_post
        my_date = get_my_time()
        exten = ".csv"
        file_to_post = name_path+"_"+my_date+exten
        retencion_reb_df.to_csv(file_to_post, index=False, sep='|')
        end_proces = datetime.now()
        print("*************************************************************************************")
        print(f"Time taken in (hh:mm:ss.ms) to process and save data is {end_proces - start_proces}")
        print("*************************************************************************************")
        
    else:
        print("==> ==> ==> No data to process on RetencionDR <== <== <==")

    # #*###################################*###################################*###################################*####

    # ###########################?

    if next_step:
        post_result(rdrv.schema, rdrv.table_name,file_to_post)
    else:
        print("==> ==> ==> No data to post on RetencionDR <== <== <==")
        
