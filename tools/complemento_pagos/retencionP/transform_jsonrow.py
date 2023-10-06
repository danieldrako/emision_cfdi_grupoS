import sys
import os
# Obtener la ruta absoluta del directorio actual donde se encuentra retencionDR.py
directorio_actual = os.path.dirname(os.path.abspath(__file__))
# Obtener la ruta absoluta del directorio principal (un nivel arriba)
directorio_principal = os.path.abspath(os.path.join(directorio_actual, '../../..'))
# Agregar la ruta al directorio principal al sys.path
sys.path.append(directorio_principal)


import pandas as pd
from tqdm import tqdm
import json
from pandas import json_normalize
tqdm.pandas()

from config.constant.complemento_pagos import retencionP_vars as rp_vars
from utils.add_tfduuid import add_tfduuid
from utils.extract_keys_from_dicts import extract_keys_from_dicts
from utils.retencion_traslado_P.find_dicts_with_keys import find_dicts_with_keys


#!##################################Función que procesa el diccionario para retenciones#?####################################
def functionOne_RetencionesCF(row):
    keys_to_find = ["pago20:RetencionesP", "pago10:RetencionesP"]
    keys_to_extract = keys_to_find.copy() 
    tfduuid = "123456789"
    try:
        tfduuid = row['tfdUUID']
        complementoPagos_Text = row['ComplementoPagos']
        complementoPagos_Json = json.loads(complementoPagos_Text)
        keys_to_find = ["pago20:RetencionesP", "pago10:RetencionesP"]
        keys_to_extract = keys_to_find.copy()
        result_dicts = find_dicts_with_keys(complementoPagos_Json, keys_to_find, rp_vars.test_key_one)
        extracted_dicts = extract_keys_from_dicts(result_dicts, keys_to_extract)
        result = add_tfduuid(tfduuid, extracted_dicts)
    except Exception as e:
        default_values = {
            "pago10:RetencionesP":{
                "pago10:RetencionP":{
            '_ImpuestoP': 'NA',
            '_ImporteP': 'NA'}
}}
        result = [{'uuid': tfduuid, **default_values}]#'id': 00, 
    return result
#!###########################################################################?####################################

#?##################################Función sobre los renglones del dataframe#?####################################
def functionTwo_RetencionesCF(df):
    results = df.progress_apply(functionOne_RetencionesCF, axis=1).tolist()
    flat_list = [item for sublist in results for item in sublist]
    return flat_list

#?#####################################################################?##########################################

#*##################################Función que regresa un dataframe con el tfduuid y el json de retenciones#?####################################
def functionThree_RetencionesCF(df):
    df_data = []
    data = functionTwo_RetencionesCF(df)
    for dictionary in data:
        uuid = dictionary['tfduuid']
        retenciones_keys = [key for key in dictionary.keys() if ':RetencionesP' in key]
        for retenciones_key in retenciones_keys:
            retencionesP = dictionary[retenciones_key]
            df_data.append({'uuid': uuid, 'retencionesP': retencionesP})        
    retencionesP_df = pd.DataFrame(df_data)
    return retencionesP_df
#*###################################*###################################*###################################*#*##################################

#!##################################Función que procesa el diccionario extraido anteriormente#?####################################
def functionOne_RetencionCF(row):
    keys_to_find = ['_ImpuestoP', '_ImporteP']
    keys_to_extract = keys_to_find.copy() 
    uuid = "123456789"
    try:
        uuid = row['uuid']
        retenciones = row['retencionesP']
        retenciones_Text = str(retenciones)
        cadena_json = retenciones_Text.replace("'", '"')  # Reemplazar comillas simples por comillas dobles
        retenciones_Json = json.loads(cadena_json)
        keys_to_find = ['_ImpuestoP', '_ImporteP']
        keys_to_extract = keys_to_find.copy()
        result_dicts = find_dicts_with_keys(retenciones_Json, keys_to_find, rp_vars.test_key_two)
        extracted_dicts = extract_keys_from_dicts(result_dicts, keys_to_extract)
        result = add_tfduuid(uuid, extracted_dicts)
    except Exception as e:
        print(e)
        default_values = {
   '_ImpuestoP': '00',
   '_ImporteP': '00'
}
        result = [{'uuid': uuid, **default_values}]#'id': 00, 
    return result
#!###################################!###################################!###################################!##################################

#?##################################Función sobre los renglones del dataframe#?####################################
def functionTwo_RetencionCF(df):
    results = df.progress_apply(functionOne_RetencionCF, axis=1)
    flat_list = [item for sublist in results for item in sublist]
    return flat_list
#?###################################?###################################?##################################