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

from config.constant.complemento_pagos import  docRelacionado_vars as drvars
from utils.docRelacionado.add_tfduuid_id import add_tfduuid_id
from utils.extract_keys_from_dicts import extract_keys_from_dicts
from utils.find_dicts_with_keys import find_dicts_with_keys

#!##################################Función que procesa el diccionario para retenciones#?####################################
def functionOne_DocuemntoCF(row):
    keys_to_find = [ "_IdDocumento", "_Serie", "_Folio", "_MonedaDR", "_EquivalenciaDR", "_NumParcialidad",
    "_ImpSaldoAnt", "_ImpPagado", "_ImpSaldoInsoluto", "_ObjetoImpDR" ]
    keys_to_extract = keys_to_find.copy() 
    tfduuid = "123456789"
    	
    try:
        uuid = row['tfdUUID']
        complementoPagos_Text = row['ComplementoPagos']
        complementoPagos_Json = json.loads(complementoPagos_Text)
        keys_to_find = ["_IdDocumento", "_Serie", "_Folio", "_MonedaDR", "_EquivalenciaDR", "_NumParcialidad",
                        "_ImpSaldoAnt", "_ImpPagado", "_ImpSaldoInsoluto", "_ObjetoImpDR"]
        keys_to_extract = keys_to_find.copy()
        result_dicts = find_dicts_with_keys(complementoPagos_Json, keys_to_find, drvars.test_key)
        extracted_dicts = extract_keys_from_dicts(result_dicts, keys_to_extract)
        result = add_tfduuid_id(uuid, extracted_dicts)
    except Exception as e:
        print(e)
        default_values = {
            '_IdDocumento': '00',
            '_Serie': '00',
            '_Folio': '00',
            '_MonedaDR': '00',
            '_EquivalenciaDR': '00',
            '_NumParcialidad': '00',
            '_ImpSaldoAnt': '00',
            '_ImpPagado': '00',
            '_ImpSaldoInsoluto': '00'
        }
        result = [{'tfduuid': tfduuid, 'id': 00, **default_values}]
    return result
#!###########################################################################?####################################

#?##################################Función sobre los renglones del dataframe#?####################################
def functionTwo_PagoCF(df):
    results = df.progress_apply(functionOne_DocuemntoCF, axis=1)
    flat_list = [item for sublist in results for item in sublist]
    return flat_list

#?#####################################################################?##########################################
