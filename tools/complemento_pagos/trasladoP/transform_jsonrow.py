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

from config.constant.complemento_pagos import trasladoP_vars as tp_vars
from utils.add_tfduuid import add_tfduuid
from utils.extract_keys_from_dicts import extract_keys_from_dicts
from utils.retencion_traslado_P.find_dicts_with_keys import find_dicts_with_keys


#!##################################Función que procesa el diccionario para retenciones#?####################################
def functionOne_TrasladosPCF(row):
    keys_to_find = ["pago20:TrasladosP", "pago10:TrasladosP"]
    keys_to_extract = keys_to_find.copy() 
    tfduuid = "123456789"
    try:
        tfduuid = row['tfdUUID']
        complementoPagos_Text = row['ComplementoPagos']
        complementoPagos_Json = json.loads(complementoPagos_Text)
        keys_to_find = ["pago20:TrasladosP", "pago10:TrasladosP"]
        keys_to_extract = keys_to_find.copy()
        result_dicts = find_dicts_with_keys(complementoPagos_Json, keys_to_find, tp_vars.test_key_one)
        extracted_dicts = extract_keys_from_dicts(result_dicts, keys_to_extract)
        result = add_tfduuid(tfduuid, extracted_dicts)
    except Exception as e:
        default_values = {
            "pago10:TrasladosP":{
            "pago10:TrasladoP":{
            '_BaseP': '00',
            '_ImpuestoP': '---',
            '_TipoFactorP': '---',
            '_TasaOCuotaP': '00',
            '_ImporteP': '00'}
}}
        result = [{'uuid': tfduuid, **default_values}]#'id': 00, 
    return result
#!###########################################################################?####################################

#?##################################Función sobre los renglones del dataframe#?####################################
def functionTwo_TrasladosPCF(df):
    results = df.progress_apply(functionOne_TrasladosPCF, axis=1).tolist()
    flat_list = [item for sublist in results for item in sublist]
    return flat_list
#?#####################################################################?##########################################

#*##################################Función que regresa un dataframe con el tfduuid y el json de retenciones#?####################################
def functionThree_TrasladosPCF(df):
    df_data = []
    data = functionTwo_TrasladosPCF(df)
    for dictionary in data:
        uuid = dictionary['tfduuid']
        traslados_keys = [key for key in dictionary.keys() if ':TrasladosP' in key]
        for traslados_key in traslados_keys:
            trasladosP = dictionary[traslados_key]
            df_data.append({'uuid': uuid, 'trasladosP': trasladosP})
    trasladosP_df = pd.DataFrame(df_data)
    return  trasladosP_df
#*###################################*###################################*###################################*#*##################################

#!##################################Función que procesa el diccionario extraido anteriormente#?####################################
def functionOne_TrasladoPCF(row):
    keys_to_find = ['_BaseP','_ImpuestoP','_TipoFactorP','_TasaOCuotaP', '_ImporteP' ]
    keys_to_extract = keys_to_find.copy() 
    uuid = "123456789"
    try:
        uuid = row['uuid']
        traslados = row['trasladosP']
        traslados_Text = str(traslados)
        cadena_json = traslados_Text.replace("'", '"')  # Reemplazar comillas simples por comillas dobles
        traslados_Json = json.loads(cadena_json)
        keys_to_find = ['_BaseP','_ImpuestoP','_TipoFactorP','_TasaOCuotaP', '_ImporteP' ]
        keys_to_extract = keys_to_find.copy()
        result_dicts = find_dicts_with_keys(traslados_Json, keys_to_find, tp_vars.test_key_two)
        extracted_dicts = extract_keys_from_dicts(result_dicts, keys_to_extract)
        result = add_tfduuid(uuid, extracted_dicts)
    except Exception as e:
        print(e)
        default_values = {
   '_BaseP': '00',
   '_ImpuestoP': '00',
   '_TipoFactorP': '00',
   '_TasaOCuotaP': '00',
   '_ImporteP': '00'
}
        result = [{'uuid': uuid, **default_values}]#'id': 00, 
    return result
#!###################################!###################################!###################################!##################################

#?##################################Función sobre los renglones del dataframe#?####################################
def functionTwo_TrasladoPCF(df):
    results = df.progress_apply(functionOne_TrasladoPCF, axis=1)
    flat_list = [item for sublist in results for item in sublist]
    return flat_list
#?###################################?###################################?##################################