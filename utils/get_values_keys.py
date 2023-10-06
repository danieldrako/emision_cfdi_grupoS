


def get_values(x):
    try:
        if isinstance(x, dict):
            return list(x.values())[0]
        else:
            print(x)
            return "REVISAR"
    except (TypeError, IndexError):
        print(x)
        return "REVISAR"
    
def get_keys(x):
    try:
        if isinstance(x, dict):
            return list(x.keys())[0]
        else:
            print(x)
            return "0"
    except (TypeError, IndexError):
        print(x)
        return "0"  