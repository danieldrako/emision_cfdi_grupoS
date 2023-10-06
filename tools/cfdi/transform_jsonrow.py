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

from tools.cfdi.add_dicts_on_list import add_dicts_on_list
from utils.create_dictionaries_do_df import create_dictionaries_to_df



def functionOne_CfdiRelacionados(row):
    tfduuid = row['tfdUUID']
    try:
        cfdi_Text = row['CfdiRelacionados']
        cfdi_Json = json.loads(cfdi_Text)
        add_dicts_to_procces_list = add_dicts_on_list(cfdi_Json)
        return create_dictionaries_to_df(add_dicts_to_procces_list,tfduuid)
    except Exception as e:
        print("Check functionOne on tfdUUID: ", tfduuid) 
   
#!###########################################################################?####################################

#?##################################Función sobre los renglones del dataframe#?####################################
def functionTwo_CfdiRelacionados(df):
    lists_dicts = df.progress_apply(functionOne_CfdiRelacionados, axis=1).tolist()
    return lists_dicts

#?#####################################################################?##########################################

#*##################################Función que regresa un dataframe con el tfduuid y el json de retenciones#?####################################

def functionThree_CfdiRelacionados(lists_dicts, expl=[]):
    df = pd.DataFrame(lists_dicts)
    df = df.dropna()
    df = df.explode(expl, ignore_index=True)
    return df

#*###################################*###################################*###################################*#*##################################

#!##################################Función que procesa el diccionario extraido anteriormente#?####################################
