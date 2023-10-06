import re

cols_=['tfdUUID', 'cfdiRelacionados']

output_file = r"cfdiRelacionados.csv"

test_key = "_TipoRelacion"

exp_cols = ['uuidrel','UUID', 'idRelacionados','tipoRelacion']

str_cols = ['uuid', 'tipoRelacion']

float_cols = []

int_cols = ["idRelacionados"]

ordered_cols = ['uuid', 'idRelacionados' ,'tipoRelacion', 'uuidrel']

new_name_tfduuid = "uuid"

to_delete = "uuid"

regular_exp = r'CfdiRelacionado' #sirve para saber que este elemento esté dentro del json principal

csv_file_path_to_post = "CfdiRelacionado"
