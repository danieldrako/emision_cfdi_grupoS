import os
from datetime import datetime

def get_my_time():
    # Obtén la fecha y hora actual
    fecha_hora_actual = datetime.now()

    # Formatea la fecha y hora en el formato deseado
    formato = "%d-%m-%Y_%H-%M-%S"
    fecha_hora_formateada = fecha_hora_actual.strftime(formato)

    # Retorna la fecha y hora formateada
    return fecha_hora_formateada

nombre = "cangos"
my_fecha = get_my_time()
extension = ".myextension"
archivo = nombre+"_"+my_fecha+extension
print(archivo)
print("<<<<<<<<<<<<<<<<<<<<<<<<<| ", archivo," |>>>>>>>>>>>>>>>>>>>>>>>>>" )
lista = (
"0168DE15-C626-414A-914E-07D3B978E6E2",
"11C4BCCE-CC3D-43E0-9E49-B8836401AB45",
"1B22133B-8812-4215-85A6-B644584CC0E9",
"216D3F20-9CCE-4A1D-9B75-874382C8752D",
"302337BF-5461-4778-9EC8-BB29FDEFAED5",
"302337BF-5461-4778-9EC8-BB29FDEFAED5",
"4DB5C34D-8188-4D1F-87D2-8E5836B4F6E3",
"512C3CCB-565A-4F2B-A682-A79AC59526B3",
"64AE6435-33C8-477F-8D7D-7DCA1075CA50",
"64AE6435-33C8-477F-8D7D-7DCA1075CA50",
"6AD8A824-5027-41E5-B637-6280D74D1410",
"79D7C29F-A2D4-4EE0-BEAB-9536DC47136E",
"9E1B1CAF-4E68-4F39-B242-B1A7CE221EBF",
"B41D9116-64FC-494B-9B4B-48955ABFC8EE",
"B41D9116-64FC-494B-9B4B-48955ABFC8EE",
"B41D9116-64FC-494B-9B4B-48955ABFC8EE",
"B41D9116-64FC-494B-9B4B-48955ABFC8EE",
"B55C4D4A-3D49-4FE2-83E3-B1E889A933A6",
"C32DF128-01DC-4AF5-A97C-03BCB5BE2586",
"C32DF128-01DC-4AF5-A97C-03BCB5BE2586",
"C69C942D-8002-44FE-81F7-2761C27F9181",
"CAA55A90-2DBA-49FB-AA03-E62DDE5FD641",
"F53E6C65-CF66-4A2F-B21D-BFB81D4BC295",
"F7181D04-B8EF-4C20-9FEE-FA0567531FD9",
"FD0343E7-23B8-4C60-9B64-FB28B2BA0718"
)

nueva = set(lista)

print(nueva)

g= ["a","b","c","d"]
print(len(g))
for i,f in enumerate(g):  
    if i < len(g)-1:
        print(f, end=", ")
    else:
        print(f)

        
    
for i,f in enumerate(g):
    print(i,f)
    


start = datetime.now()

nueva_carpeta = start.strftime("%d-%m-%Y_%H-%M-%S" )
nueva_carpeta = "temp/"+nueva_carpeta
print("*************************************************************************************")

print("<<<<<<<<<<<<<<<<<<<<<<<<<| Getting Data |>>>>>>>>>>>>>>>>>>>>>>>>>")
print("<<<<<<<<<<<<<<<<<<<<<<<<<| ", start," |>>>>>>>>>>>>>>>>>>>>>>>>>" )
print("*************************************************************************************")
if not os.path.exists(nueva_carpeta):
    os.makedirs(nueva_carpeta)
    print(f"Carpeta '{nueva_carpeta}' creada con éxito.")
else:
    print(f"La carpeta '{nueva_carpeta}' ya existe.")