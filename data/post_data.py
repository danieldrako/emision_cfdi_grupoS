import os
import sys
current_dir = os.path.dirname(os.path.abspath(__file__))
# Agrega el directorio 'prueba' al sys.path
prueba_dir = os.path.join(current_dir, '..')
sys.path.append(prueba_dir)

import pandas as pd

from datetime import datetime
import vertica_python
from decouple import config



def post_result(schema,table_name, csv_file_path):

    try:
        start_post =datetime.now() 

        # Configuración de la conexión a la base de datos Vertica
        conn_info = {
        'host': config('VERTICA_HOST'),
        'port': config('VERTICA_PORT'),
        'password': config('VERTICA_PASSWORD'),
        'user': config('VERTICA_USER')
        } 
        conn = vertica_python.connect(**conn_info)
        cursor = conn.cursor()

        # Construir la consulta COPY y ejecutarla
        copy_query = f"COPY {schema}.{table_name} FROM LOCAL'{csv_file_path}'  DELIMITER '|'"
        cursor.execute(copy_query)

        # Realizar un COMMIT
        conn.commit()
        end_post = datetime.now()
        print("*************************************************************************************")
        print(f"<<<<<<<<<<|Time taken in (hh:mm:ss.ms) to post data is {end_post - start_post}|>>>>>>>>>>")
        print("*************************************************************************************")
        cursor.close()
        conn.close()  

    except vertica_python.errors.ConnectionError as conn_err:
        print("Error on: ", current_dir,"/post_data.py")
        print("Error de conexión a Vertica:", conn_err)
    except vertica_python.errors.QueryError as query_err:
        print("Error on: ", current_dir,"/post_data.py")
        print("Error de consulta a Vertica:", query_err)
    except Exception as e:
        print("Error on: ", current_dir,"/post_data.py")
        print("Error desconocido:", e) 
    finally:
        if 'cursor' in locals() and cursor is not None:
            cursor.close()
        if 'conn' in locals() and conn is not None:
            conn.close() 
        
if __name__ == "__main__":
    table_name = "tes_tretencionDR"  # Nombre de la tabla en Vertica
    schema = "DEV_FACTURACION"
    csv_file_path = 'temp/retencionDR_2023-09-18_12-01-24.csv'# Esquema en Vertica

        #? Llamada a la función de post a la base de datos

    print("¿Deseas volver a suir los datos?\n [1] Si\n [2]No ")
    re = input()
    if re == "1":
        try:
            inicio_post = datetime.now()
            post_result(schema,table_name, csv_file_path)
            fin_post = datetime.now()
            print("**************************************************************************************************************************")
            print(f"***************************----Time taken in (hh:mm:ss.ms) to post data is {fin_post - inicio_post}----***************************")
            print("**************************************************************************************************************************")
        except Exception as e:
            print("Error: ", e)
    else:
        print("Sin acciones")
    
        

