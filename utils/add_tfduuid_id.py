import os

# Obtiene la ruta del directorio actual donde se encuentra el script
ruta_script = os.path.abspath(__file__)

def add_tfduuid_id(tfduuid, extracted_dicts):
    try:
        dicts_with_id = []
        for ind, dict_ in enumerate(extracted_dicts):
            if isinstance(dict_, dict):
                dicts_with_id.append({"uuid": tfduuid,  **dict_})  #"id": ind+1,
        return dicts_with_id
    except Exception as e:
        print("ERROR")
        print("Check: ", ruta_script)
        print("Error: ", e)