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

from config.constant.complemento_pagos import  totales_vars as tvars
from utils.add_tfduuid import add_tfduuid
from utils.extract_keys_from_dicts import extract_keys_from_dicts
from utils.find_dicts_with_keys import find_dicts_with_keys

#!##################################Función que procesa el diccionario para retenciones#?####################################
def functionOne_TotalesCF(row):
    keys_to_find = ["_TotalRetencionesIVA","_TotalRetencionesISR","_TotalRetencionesIEPS","_TotalTrasladosBaseIVA16",
   "_TotalTrasladosImpuestoIVA16","_TotalTrasladosBaseIVA8","_TotalTrasladosImpuestoIVA8",
   "_TotalTrasladosBaseIVA0","_TotalTrasladosImpuestoIVA0", "_TotalTrasladosBaseIVAExento","_MontoTotalPagos" ]
    keys_to_extract = keys_to_find.copy() 
    tfduuid = "123456789"
    try:
        uuid = row['tfdUUID']
        complementoPagos_Text = row['ComplementoPagos']
        complementoPagos_Json = json.loads(complementoPagos_Text)
        keys_to_find = ["_TotalRetencionesIVA","_TotalRetencionesISR","_TotalRetencionesIEPS","_TotalTrasladosBaseIVA16",
   "_TotalTrasladosImpuestoIVA16","_TotalTrasladosBaseIVA8","_TotalTrasladosImpuestoIVA8",
   "_TotalTrasladosBaseIVA0","_TotalTrasladosImpuestoIVA0", "_TotalTrasladosBaseIVAExento","_MontoTotalPagos" ]
        keys_to_extract = keys_to_find.copy()
        result_dicts = find_dicts_with_keys(complementoPagos_Json, keys_to_find, tvars.test_key)
        extracted_dicts = extract_keys_from_dicts(result_dicts, keys_to_extract)
        result = add_tfduuid(uuid, extracted_dicts)
    except Exception as e:
        default_values = {
            "_TotalRetencionesIVA":"00",
            "_TotalRetencionesISR":"00",
            "_TotalRetencionesIEPS":"00",
            "_TotalTrasladosBaseIVA16":"00",
            "_TotalTrasladosImpuestoIVA16":"00",
            "_TotalTrasladosBaseIVA8":"00",
            "_TotalTrasladosImpuestoIVA8":"00",
            "_TotalTrasladosBaseIVA0":"00",
            "_TotalTrasladosImpuestoIVA0":"00",
            "_TotalTrasladosBaseIVAExento":"00",
            "_MontoTotalPagos":"00"
        }
        result = [{'tfduuid': tfduuid, **default_values}]#'id': 00, 
    return result
#!###########################################################################?####################################

#?##################################Función sobre los renglones del dataframe#?####################################
def functionTwo_TotalesCF(df):
    results = df.progress_apply(functionOne_TotalesCF, axis=1)
    flat_list = [item for sublist in results for item in sublist]
    return flat_list
#?#####################################################################?##########################################
