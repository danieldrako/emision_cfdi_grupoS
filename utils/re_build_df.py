import os

# Obtiene la ruta del directorio actual donde se encuentra el script
ruta_script = os.path.abspath(__file__)
        
import pandas as pd



def re_build_df(dataframe, ordered_cols, float_cols, 
                str_cols, int_cols, new_name_tfduuid="", to_delete=""):
 
        df = dataframe.copy()  # Crear una copia del DataFrame original

        for colu in dataframe.columns:
            if colu.startswith('_'):
                new_col = colu.replace('_', '')
                df.rename(columns={colu: new_col}, inplace=True)
        
        # Renombrar la columna "tfduuid" o "tfdUUID" según el valor de new_name_tfduuid
        if "tfduuid" in df.columns:
            df.rename(columns={"tfduuid": new_name_tfduuid}, inplace=True)
        elif "tfdUUID" in df.columns:
            df.rename(columns={"tfdUUID": new_name_tfduuid}, inplace=True)
        elif "UUID" in df.columns:
            df.rename(columns={"UUID": new_name_tfduuid}, inplace=True)
        
        # Agregar columnas faltantes con valores vacíos
        for new_col in ordered_cols:
            if new_col not in df.columns:
                df[new_col] = ""

        # Cambiar tipos de datos con manejo de excepciones
        for colu in df.columns:
            try:
                if colu in float_cols:
                    df[colu] = df[colu].astype(float)
                elif colu in str_cols:
                    df[colu] = df[colu].astype(str)
                elif colu in int_cols:
                    df[colu] = df[colu].astype(float).astype(int)
            except ValueError:
                print(f"Error de conversión en la columna {colu}  | Se mantuvo como tipo de dato original.")

        # Reemplazar 'nan' con cadenas vacías y eliminar filas según to_delete
        df = df.replace('nan', '').fillna('')
        df = df[df['uuid'] != '']

        # Filtrar columnas según ordered_cols
        df = df[ordered_cols]
        return df