import os
# Obtiene la ruta del directorio actual donde se encuentra el script
ruta_script = os.path.abspath(__file__)

import concurrent.futures
import pandas as pd

def to_dataframe(row, cols_):
    # Convierte una fila de datos en un DataFrame con las columnas especificadas
    df = pd.DataFrame([row], columns=cols_)
    return df

def to_dataframe_parallel(result, cols_):
    try: 
        with concurrent.futures.ThreadPoolExecutor() as executor:
            dataframes = list(executor.map(lambda row: to_dataframe(row, cols_), result))
        # Combina los DataFrames en uno solo
        combined_df = pd.concat(dataframes, ignore_index=True)

        return combined_df

    except Exception as e:
            print("ERROR")
            print("Check: ", ruta_script)
            print("Error: ", e)