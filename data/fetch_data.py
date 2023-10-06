import os
import sys
current_dir = os.path.dirname(os.path.abspath(__file__))
# Agrega el directorio 'prueba' al sys.path
prueba_dir = os.path.join(current_dir, '..')
sys.path.append(prueba_dir)

import vertica_python
import pandas as pd
from config.database_config import vertica_host, vertica_port, vertica_user, vertica_password

def fetch_and_save_data(query,  host = vertica_host, port=vertica_port, user=vertica_user, password=vertica_password):

    cursor = None
    conn = None
    try:
        # Parámetros de conexión
        conn_info = {
            'host': host,
            'port': port,
            'password': password,
            'user': user
        }
        # Establecer la conexión
        conn = vertica_python.connect(**conn_info)
        # Crear un cursor
        cursor = conn.cursor()
        
        cursor.execute(query)
        
        # Recuperar los resultados
        results = cursor.fetchall()
              
        return results
        
    except Exception as e: 
        print("Error on: ", current_dir, "/fetch_data.py")
        print(" Error on get data from vertica with vertica_python. Error: ", e)
        return None
    
    finally:
        # Cerrar el cursor y la conexión si se crearon
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# Si deseas llamar a la función desde otro archivo
if __name__ == "__main__":
    host = vertica_host
    port = vertica_port
    password = vertica_password
    user = vertica_user
    output_file = r"temp/from_vertica_TestV1.csv"
    query = """
    SELECT
    tfdUUID
    , maptostring(Conceptos) as 'Conceptos'
    FROM DocumentDB.FacturaPersistida fp 
    WHERE 
    tfdUUID NOT IN (SELECT uuid FROM DEV_FACTURACION.testconcepto t)
    LIMIT 100;
    """
    cols_ = ['tfduuid', 'Conceptos']
    
    data_frame = fetch_and_save_data(host, port, user, password, output_file, query, cols_)
    if data_frame is not None:
        print("Data retrieved and saved successfully.")
        print(data_frame)
    else: 
        print("Query has no data")
