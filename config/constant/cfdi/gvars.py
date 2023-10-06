import re

query = """
SELECT 
 fp.tfdUUID,  
 maptostring(fp.CfdiRelacionados) as 'CfdiRelacionados'  
FROM DocumentDB.FacturaPersistida fp  
WHERE  fp.CfdiRelacionados IS NOT NULL
AND fp.tfdUUID NOT IN( SELECT 
DISTINCT tcr.'uuid' 
FROM DEV_FACTURACION.test_cfdiRelacionados tcr)
LIMIT 3000000;
"""
cols_=['tfdUUID', 'CfdiRelacionados']

output_file = r"FROM_Vertica_Cfdi.csv"

tables_dest = {"cfdiRelacionados":"test_cfdiRelacionados", "uuidRelacionado":"test_uuidRelacionado"}

schema = "DEV_FACTURACION"
