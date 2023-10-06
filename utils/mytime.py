from datetime import datetime

def get_my_time():
    # Obtén la fecha y hora actual
    fecha_hora_actual = datetime.now()

    # Formatea la fecha y hora en el formato deseado
    formato = "%d-%m-%Y_%H-%M-%S"
    fecha_hora_formateada = fecha_hora_actual.strftime(formato)

    # Retorna la fecha y hora formateada
    return fecha_hora_formateada