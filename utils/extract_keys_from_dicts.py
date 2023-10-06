import os

# Obtiene la ruta del directorio actual donde se encuentra el script
ruta_script = os.path.abspath(__file__)


def extract_keys_from_dicts(dicts, keys_to_extract):
    try: 
        extracted_dicts = []
        for d in dicts:
            extracted_dict = {}
            for key in keys_to_extract:
                if key in d:
                    extracted_dict[key] = d[key]
            extracted_dicts.append(extracted_dict)
        return extracted_dicts
    except Exception as e:
        print("ERROR")
        print("Check: ", ruta_script)
        print("Error: ", e)