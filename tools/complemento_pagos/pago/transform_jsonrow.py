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

from config.constant.complemento_pagos import  pago_vars as pvars
from utils.add_tfduuid import add_tfduuid
from utils.extract_keys_from_dicts import extract_keys_from_dicts
from utils.find_dicts_with_keys import find_dicts_with_keys

#!##################################Función que procesa el diccionario para retenciones#?####################################
def functionOne_PagoCF(row):
    keys_to_find = ["_FechaPago","_FormaDePagoP","_MonedaP","_TipoCambioP","_Monto","_NumOperacion",
   "_RfcEmisorCtaOrd","_NomBancoOrdExt","_CtaOrdenante","_RfcEmisorCtaBen","_CtaBeneficiario",
   "_TipoCadPago","_CertPago","_CadPago","_SelloPago"]
    keys_to_extract = keys_to_find.copy() 
    tfduuid = "123456789"
    	
    try:
        uuid = row['tfdUUID']
        complementoPagos_Text = row['ComplementoPagos']
        complementoPagos_Json = json.loads(complementoPagos_Text)
        keys_to_find = ["_FechaPago","_FormaDePagoP","_MonedaP","_TipoCambioP","_Monto","_NumOperacion",
   "_RfcEmisorCtaOrd","_NomBancoOrdExt","_CtaOrdenante","_RfcEmisorCtaBen","_CtaBeneficiario",
   "_TipoCadPago","_CertPago","_CadPago","_SelloPago"]
        keys_to_extract = keys_to_find.copy()
        result_dicts = find_dicts_with_keys(complementoPagos_Json, keys_to_find, pvars.test_key)
        extracted_dicts = extract_keys_from_dicts(result_dicts, keys_to_extract)
        result = add_tfduuid(uuid, extracted_dicts)
    except Exception as e:
        default_values = {
   "_FechaPago": "00",
   "_FormaDePagoP": "00",
   "_MonedaP": "00",
   "_TipoCambioP": "00", 
   "_Monto": "00",
   "_NumOperacion": "00",
   "_RfcEmisorCtaOrd": "00",
   "_NomBancoOrdExt": "00",
   "_CtaOrdenante": "00",
   "_RfcEmisorCtaBen": "00",
   "_CtaBeneficiario": "00",
   "_TipoCadPago": "00",
   "_CertPago": "00",
   "_CadPago": "00",
   "_SelloPago": "00"
}
        result = [{'tfduuid': tfduuid, **default_values}]#'id': 00, 
    return result
#!###########################################################################?####################################

#?##################################Función sobre los renglones del dataframe#?####################################
def functionTwo_PagoCF(df):
    results = df.progress_apply(functionOne_PagoCF, axis=1)
    flat_list = [item for sublist in results for item in sublist]
    return flat_list

#?#####################################################################?##########################################
