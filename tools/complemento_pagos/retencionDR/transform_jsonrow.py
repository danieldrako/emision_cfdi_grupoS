import os
import sys

from utils.retencion_traslado_dr import add_tfduuid_id
current_dir = os.path.dirname(os.path.abspath(__file__))
# Agrega el directorio 'prueba' al sys.path
prueba_dir = os.path.join(current_dir, '..')
sys.path.append(prueba_dir)

import pandas as pd
from tqdm import tqdm
import json
from utils import extract_keys_from_dicts, find_dicts_with_keys
tqdm.pandas()

#!##################################Función que procesa el diccionario para retenciones#?####################################
def functionOne_ImpuestosDRCF(row):
    keys_to_find = ['_IdDocumento',"pago20:ImpuestosDR","pago10:ImpuestosDR" ]
    keys_to_extract = keys_to_find.copy() 
    uuid = "123456789"
    	
    try:
        uuid = row['tfdUUID']
        traslados = row['ComplementoPagos']
        traslados_Text = str(traslados)
        cadena_json = traslados_Text.replace("'", '"')  # Reemplazar comillas simples por comillas dobles
        traslados_Json = json.loads(cadena_json)
        keys_to_find = ['_IdDocumento',"pago20:ImpuestosDR","pago10:ImpuestosDR" ]
        keys_to_extract = keys_to_find.copy()
        result_dicts = find_dicts_with_keys(traslados_Json, keys_to_find)
        extracted_dicts = extract_keys_from_dicts(result_dicts, keys_to_extract)
        result = add_tfduuid_id(uuid, extracted_dicts)
    except Exception as e:
        print(e)
        default_values = {'pago20:ImpuestosDR': {'pago20:RetencionesDR': {'pago20:RetencionDR': {'_BaseDR': '00',
     '_ImporteDR': '000',
     '_ImpuestoDR': '000',
     '_TasaOCuotaDR': '0.0000',
     '_TipoFactorDR': '0'}}}}
        result = [{'_IdDocumento': uuid, **default_values}]#'id': 00, 
    return result
#!###########################################################################?####################################

#?##################################Función sobre los renglones del dataframe#?####################################
def functionTwo_ImpuestosDRCF(df):
    results = df.progress_apply(functionOne_ImpuestosDRCF, axis=1).tolist()
    flat_list = [item for sublist in results for item in sublist]
    return flat_list
#?#####################################################################?##########################################

#*##################################Función que regresa un dataframe con el tfduuid y el json de retenciones#?####################################
def functionThree_ImpuestosDRCF(df):
    df_data = []
    data = functionTwo_ImpuestosDRCF(df)
    for dictionary in data:
        uuid = dictionary['_IdDocumento']
        ImpuestosDRkeys = [key for key in dictionary.keys() if ':ImpuestosDR' in key]
        
        for ImpuestosDRkey in ImpuestosDRkeys:
            ImpuestosDR = dictionary[ImpuestosDRkey]
            df_data.append({'_IdDocumento': uuid, 'ImpuestosDR': ImpuestosDR})
            new_list = df_data.copy()

    new_list = []   
    for item in df_data:
        _IdDocumento = item['_IdDocumento']
        impuestos = item.get('ImpuestosDR', {})

        for key, value in impuestos.items():
                if key.endswith(':RetencionesDR'):
                    new_item = {'_IdDocumento': _IdDocumento, 'ImpuestosDR': value}
                    new_list.append(new_item)

    ImpuestosDR_df = pd.DataFrame(new_list)

    return  ImpuestosDR_df
#*###################################*###################################*###################################*#*##################################

#!##################################Función que procesa el diccionario extraido anteriormente#?####################################
def functionOne_RetencionDRCF(row):
    keys_to_find = ['_BaseDR','_ImpuestoDR','_TipoFactorDR','_TasaOCuotaDR' '_ImporteDR' ]
    keys_to_extract = keys_to_find.copy() 
    uuid = "123456789"
    try:
        uuid = row['_IdDocumento']
        traslados = row['ImpuestosDR']
        traslados_Text = str(traslados)
        cadena_json = traslados_Text.replace("'", '"')  # Reemplazar comillas simples por comillas dobles
        traslados_Json = json.loads(cadena_json)
        keys_to_find = ['_BaseDR','_ImpuestoDR','_TipoFactorDR','_TasaOCuotaDR' '_ImporteDR' ]
        keys_to_extract = keys_to_find.copy()
        result_dicts = find_dicts_with_keys(traslados_Json, keys_to_find)
        extracted_dicts = extract_keys_from_dicts(result_dicts, keys_to_extract)
        result = add_tfduuid_id(uuid, extracted_dicts)
    except Exception as e:
        print(e)
        default_values = {
   '_BaseDR': '00',
   '_ImpuestoDR': '00',
   '_TipoFactorDR': '00',
   '_TasaOCuotaDR': '00',
   '_ImporteDR': '00'
}
        result = [{'_IdDocumento': uuid, **default_values}]#'id': 00, 
    return result
#!###################################!###################################!###################################!##################################

#?##################################Función sobre los renglones del dataframe#?####################################
def functionTwo_RetencionDRCF(df):
    results = df.progress_apply(functionOne_RetencionDRCF, axis=1)
    flat_list = [item for sublist in results for item in sublist]
    return flat_list
#?###################################?###################################?##################################