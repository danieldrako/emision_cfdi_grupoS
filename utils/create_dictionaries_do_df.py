


def create_dictionaries_to_df(to_procces,tfduuid):
    uuid = []
    id_relacionados = []
    tipoRelacion = []
    _uids = []
    try:
        if len(to_procces) == 1:
            dic = to_procces[0]
            relacion = dic['_TipoRelacion']
            for j,dic_uid in enumerate(dic["uuids"]):
                uuid.append(tfduuid)
                id_relacionados.append(j+1)
                tipoRelacion.append(relacion)
                #key_uid_numeric =f"{j+1}"
                key_uid_numeric = 1   
                _uids.append([{key_uid_numeric:dic_uid['_UUID']}])
            new_extracted_dic = {"UUID":uuid, "idRelacionados":id_relacionados, "tipoRelacion":tipoRelacion, "uuidrel":_uids}
            
        else:
            for j,dicts in enumerate(to_procces):
                relacion = dicts['_TipoRelacion']
                uuid.append(tfduuid)
                id_relacionados.append(j+1)
                tipoRelacion.append(relacion)
                uuid_dicts = dicts['uuids']
                sub_uids = []
                for k,dic_uid in enumerate(uuid_dicts):
                    #key_uid_numeric =f"{k+1}"
                    key_uid_numeric =k+1
                    sub_uids.append({key_uid_numeric:dic_uid['_UUID']})
                _uids.append(sub_uids)
                new_extracted_dic = {"UUID":uuid, "idRelacionados":id_relacionados, "tipoRelacion":tipoRelacion, "uuidrel":_uids}
                
    except Exception as e:
        new_extracted_dic = {"UUID":[tfduuid], "idRelacionados":[0],"tipoRelacion":["0"], "uuidrel":[[{1:'--Nan--'}]] }
    return new_extracted_dic  





