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


from utils.re_build_df import re_build_df
from tools.cfdi import transform_jsonrow as tjr
from config.constant.cfdi import gvars as gv
from config.constant.cfdi import cfdiRel_vars as cfdiv
from config.constant.cfdi import uuidRel_vars as uidv

from utils.rebuild_uuid_relacionado_df import re_build_uuid_relacionado_df
from utils.mytime import get_my_time
from data.post_data import post_result
#!##########################################Llamado a la base de datos############################################
#!########################################## Fuente de los datos  ################################################
#Variables

cols_= cfdiv.cols_
output_file = cfdiv.output_file
regular_exp = cfdiv.regular_exp
table_name_cfdi = gv.tables_dest["cfdiRelacionados"]
table_name_uuid = gv.tables_dest["uuidRelacionado"]
schema = gv.schema


#!#################################################################################################################

# #*##################################GuardarData frame procesado en local##########################################



def cfdiRel(next_step, cfdi_df, carpeta):
    print("_"*80)
    print("\/"*60)
    print("="*80)
    print("")
    print("==============================| cfdi Relacionados |=============================")
    print("====================================| Processing |===================================")
    print("_"*80)
    print("/\\"*60)
    print("="*80)
    start = datetime.now()
    ccfdi_df=cfdi_df.copy()
    if next_step:
        
        
        lists_dicts = tjr.functionTwo_CfdiRelacionados(ccfdi_df)
        cfdi_relacionados_df  = tjr.functionThree_CfdiRelacionados(lists_dicts, expl=cfdiv.exp_cols)
        
        #cfdi_relacionados_df = cfdi_relacionados_df.dropna()
        cfdi_relacionados_reb_df = re_build_df(cfdi_relacionados_df, cfdiv.ordered_cols, cfdiv.float_cols,
                                    cfdiv.str_cols, cfdiv.int_cols, new_name_tfduuid=cfdiv.new_name_tfduuid,
                                    to_delete=cfdiv.to_delete)


        #cfdi_relacionados_reb_df = cfdi_relacionados_reb_df.dropna()
        print(cfdi_relacionados_reb_df.head())


        cfdi_relacionados_to_up_df = cfdi_relacionados_reb_df.drop(columns=['uuidrel'])
        name_path_cfdi = cfdiv.csv_file_path_to_post
        name_path_cfdi = carpeta+"/"+name_path_cfdi
        my_date = get_my_time()
        exten = ".csv"
        file_to_post_cfdi = name_path_cfdi+"_"+my_date+exten
        cfdi_relacionados_to_up_df.to_csv(file_to_post_cfdi, index=False, sep='|')
        end_proces = datetime.now()
        print("*************************************************************************************")
        print(f"Time taken in (hh:mm:ss.ms) to process and save dataas {file_to_post_cfdi} is {end_proces - start}")
        print("====================|data to cfdi saved on csv file|========================")
        print("*************************************************************************************")
        print("\n"*3)
    else:
        print("No data to process on cfdiRelacionados")
######################################################################################################*####
###################################################################################################?
    if cfdi_relacionados_reb_df is not None and not cfdi_relacionados_reb_df.empty:
        start_uuid = datetime.now()
        print("_"*80)
        print("\/"*60)
        print("="*80)
        print("")
        print("==============================| uuid Relacionado |=============================")
        print("================================| Processing |===============================")
        print("_"*80)
        print("/\\"*60)
        print("="*80)
        uuid_relacionado_df = re_build_uuid_relacionado_df(dataframe= cfdi_relacionados_reb_df, float_cols=uidv.float_cols,
                                        str_cols = uidv.str_cols, int_cols = uidv.int_cols, ordered_cols  = uidv.ordered_cols )
        print(uuid_relacionado_df.head(5))
        
        
        name_path_uuid = uidv.csv_file_path_to_post
        name_path_uuid = carpeta+"/"+name_path_uuid
        my_date = get_my_time()
        exten = ".csv"
        file_to_post_uuid = name_path_uuid+"_"+my_date+exten
        uuid_relacionado_df.to_csv(file_to_post_uuid, index=False, sep='|')
        end_uuid = datetime.now()
        print("*************************************************************************************")
        print(f"Time taken in (hh:mm:ss.ms) to process and save data as {file_to_post_uuid} is {end_uuid - start_uuid}")
        print("====================|data to uuid saved on csv file|========================")
        print("*************************************************************************************")
        print("\n"*3)
        
        
    if cfdi_relacionados_to_up_df is not None and not cfdi_relacionados_to_up_df.empty:
        print(f"==============================| Uploading on  {schema}.{table_name_cfdi} |=============================")
        try:
            start_up_cfdi = datetime.now()
            post_result(schema, table_name_cfdi, file_to_post_cfdi)
            end_up_cfdi  = datetime.now()
            print("==============================| cfdiRelacionados finished |=============================")
            print("*************************************************************************************")
            print(f"Time taken in (hh:mm:ss.ms) to upload cfdiRelacionados is {end_up_cfdi - start_up_cfdi}")
            print("*************************************************************************************")
            print("\n"*3)
            ready_to_up_uuidRel = True
        except Exception as e:
            print("Error: ", e)
            ready_to_up_uuidRel = False
    else:
        print("==> ==> ==> No data to post on cfdiRelacionados <== <== <==")
        
    if ready_to_up_uuidRel == True:
        print(f"==============================| Uploading on  {schema}.{table_name_uuid} |=============================")
        try:
            start_up_uuid = datetime.now()
            post_result(schema, table_name_uuid, file_to_post_uuid)
            end_up_uuid = datetime.now()
            print("==============================| uuidRelacionado finished |=============================")
            print("*************************************************************************************")
            print(f"Time taken in (hh:mm:ss.ms) to upload uuidRelacionado is {end_up_uuid - start_up_uuid}")
            print("*************************************************************************************")
            print("\n"*3)
            cfdi_updated = True
        except Exception as e:
            print("Error: ", e)
            cfdi_updated = False
    else:
        print("==> ==> ==> No data to post on uuidRel <== <== <==")
        cfdi_updated = False
    return cfdi_updated 
        
