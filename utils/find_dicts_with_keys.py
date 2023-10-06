import os

# Obtiene la ruta del directorio actual donde se encuentra el script
ruta_script = os.path.abspath(__file__)



def find_dicts_with_keys(d, keys):
    try:
        found_dicts = []
        if isinstance(d, dict):
            if any(key in d for key in keys):
                found_dicts.append(d)
            for value in d.values():
                if isinstance(value, (dict, list)):
                    found_dicts.extend(find_dicts_with_keys(value, keys))
        elif isinstance(d, list):
            for item in d:
                found_dicts.extend(find_dicts_with_keys(item, keys))
        return found_dicts
    except Exception as e:
        print("ERROR")
        print("Check: ", ruta_script)
        print("Error: ", e)