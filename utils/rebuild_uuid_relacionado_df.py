import sys
import os
import pandas as pd
# Obtiene la ruta del directorio actual donde se encuentra el script
ruta_script = os.path.abspath(__file__)

# Obtener la ruta absoluta del directorio actual donde se encuentra retencionDR.py
directorio_actual = os.path.dirname(os.path.abspath(__file__))
# Obtener la ruta absoluta del directorio principal (un nivel arriba)
directorio_principal = os.path.abspath(os.path.join(directorio_actual, '../'))
# Agregar la ruta al directorio principal al sys.path
sys.path.append(directorio_principal)


from utils.get_values_keys import get_values, get_keys

def re_build_uuid_relacionado_df(dataframe, float_cols, str_cols, int_cols, ordered_cols):
    
    
    try:    
        # Eliminar filas con valores nulos

        uidRelacionado_df = dataframe.dropna()
        uidRelacionado_df = uidRelacionado_df.explode(['uuidrel'], ignore_index=True)
        # Crear una nueva columna "_uuid" con llos valores de los diccionarios
        #uidRelacionado_df['_uuid'] = uidRelacionado_df['uuidrel'].progress_apply(lambda x: list(x.values())[0])
        uidRelacionado_df['_uuid'] = uidRelacionado_df['uuidrel'].progress_apply(get_values)
        # Crear una nueva columna "id" con las claves de los diccionarios
        #uidRelacionado_df ['id'] = uidRelacionado_df ['uuidrel'].progress_apply(lambda x: list(x.keys())[0])
        uidRelacionado_df ['id'] = uidRelacionado_df ['uuidrel'].progress_apply(get_keys)

       

        # Agregar columnas faltantes con valores vacíos
        for new_col in ordered_cols:
            if new_col not in uidRelacionado_df.columns:
                uidRelacionado_df[new_col] = ""

        for colu in uidRelacionado_df.columns:
            try:
                if colu in float_cols:
                    uidRelacionado_df[colu] = uidRelacionado_df[colu].astype(float)
                elif colu in str_cols:
                    uidRelacionado_df[colu] = uidRelacionado_df[colu].astype(str)
                elif colu in int_cols:
                    uidRelacionado_df[colu] = uidRelacionado_df[colu].astype(float).astype(int)
            except ValueError:
                print(f"Error de conversión en la columna {colu}  | Se mantuvo como tipo de dato original.")
  
        uidRelacionado_df = uidRelacionado_df.replace('nan', '').fillna('')

        # Filtrar columnas según ordered_cols        
        uidRelacionado_df = uidRelacionado_df[ordered_cols]
        return uidRelacionado_df
    except Exception as e:
        print("ERROR")
        print("Check: ", ruta_script)
        print("Error: ", e)