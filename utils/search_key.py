import re
import sys
import os
# Obtener la ruta absoluta del directorio actual donde se encuentra retencionDR.py
directorio_actual = os.path.dirname(os.path.abspath(__file__))
# Obtener la ruta absoluta del directorio principal (un nivel arriba)
directorio_principal = os.path.abspath(os.path.join(directorio_actual, '../'))
# Agregar la ruta al directorio principal al sys.path
sys.path.append(directorio_principal)

from tqdm import tqdm
tqdm.pandas()


def functionOne_Existe (row, regexp):
                            
    complementoPagos_Text = row['ComplementoPagos']
    searchRetencion = bool(re.search(regexp, complementoPagos_Text))
    return searchRetencion

def functionTwo_Existe(df,regexp):
    return df.apply(lambda row: functionOne_Existe(row,regexp), axis=1)

# Definir la función principal que filtra el DataFrame
def function_existe(df, regexp):
    # Aplicar la funciónTwo_Existe al DataFrame
    df["existe"] = functionTwo_Existe(df, regexp)
    dataframe_filtered = df[df["existe"] == True]
    return dataframe_filtered

#copia.to_csv("existeTotales.csv", index= False)