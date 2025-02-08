from arquivos.access_db import AccessDB
import numpy as np

class InterationDb:

    def __init__(self):
        self.abreconn = AccessDB()

    def get_gold_id_interation(self, input_data):
        sql = f"SELECT id_gold_iteration FROM tb_gold_iteration WHERE iteration = '{input_data}'"
        retorna = self.abreconn.fetch_data_one(sql)
        if retorna is not None:
            return retorna[0]
        else:
            return None
        
    def get_gold_id_interation_duedate(self, firstdate, lastdate):
        sql = f"SELECT id_gold_iteration FROM tb_gold_iteration WHERE data_ini = #{firstdate}# and data_fim = #{lastdate}#"
        retorna = self.abreconn.fetch_data_one(sql)
        if retorna is not None:
            return retorna[0]
        else:
            return None
        
    
    def save_gold_iteration(self, data_ini, data_fim, iteration):
        sql = f"INSERT INTO tb_gold_iteration (data_ini, data_fim, iteration) VALUES ('{data_ini}', '{data_fim}', '{iteration}')"
        #gravagoldit = self.abreconn.grava_dados(sql)
        return sql
    
    def get_gold_id_biinteration(self, input_data):
        sql = f"SELECT id_gold_iteration FROM tb_gold_bi_iteration WHERE iteration = '{input_data}'"
        
        retorna = self.abreconn.fetch_data_one(sql)
        if retorna is not None:
            return retorna[0]
        else:
            return None 
    
    def save_gold_biiteration(self, data_ini, data_fim, iteration):
        sql = f"INSERT INTO tb_gold_bi_iteration (data_ini, data_fim, iteration) VALUES ('{data_ini}', '{data_fim}', '{iteration}')"
        #gravagoldit = self.abreconn.grava_dados(sql)
        return sql