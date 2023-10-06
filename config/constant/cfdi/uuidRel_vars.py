import re

cols_=['tfdUUID', 'cfdiRelacionados']

output_file = r"temp/uuidRelacionado.csv"

test_key = "_UUID"


str_cols = ['uuid', '_uuid']

float_cols = []

int_cols = ["idRelacionados", "id"]

ordered_cols = ['uuid', 'idRelacionados' ,'id', '_uuid']

new_name_tfduuid = "uuid"

to_delete = "uuid"

regular_exp = r"_UUID" #sirve para saber que este elemento esté dentro del json principal

csv_file_path_to_post = "uuidRelacionado"
