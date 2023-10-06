import sys
import os
# Obtener la ruta absoluta del directorio actual donde se encuentra retencionDR.py
directorio_actual = os.path.dirname(os.path.abspath(__file__))
# Obtener la ruta absoluta del directorio principal (un nivel arriba)
directorio_principal = os.path.abspath(os.path.join(directorio_actual, '../../'))
# Agregar la ruta al directorio principal al sys.path
sys.path.append(directorio_principal)

import pandas as pd
from tqdm import tqdm
import json
from pandas import json_normalize
tqdm.pandas()

from utils.find_dicts_with_keys import find_dicts_with_keys
from utils.extract_keys_from_dicts import extract_keys_from_dicts 

def add_dicts_on_list(cfdi_Json): 
    new = []
    if "0" in cfdi_Json:
        for i in list(cfdi_Json.keys()):
            new_dict = cfdi_Json[i]
            new.append(add_dicts_on_list(new_dict))
        new = [dic for sub_l in new for dic in sub_l ]
    else:
        k_rel=['_TipoRelacion']
        f_rel = find_dicts_with_keys(cfdi_Json, k_rel)
        ext_rel = extract_keys_from_dicts(f_rel, k_rel)
        for dic in ext_rel:
            k_uuid=["_UUID"]
            f_id = find_dicts_with_keys(cfdi_Json, k_uuid)
            new.append({**dic, "uuids":f_id})
    return new
