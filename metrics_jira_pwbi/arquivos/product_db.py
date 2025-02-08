from arquivos.access_db import AccessDB
import numpy as np

class ProductDb:

    def __init__(self):
        self.abreconn = AccessDB()

    def get_id_product(self, input_data):
        sql = f"SELECT id_produto FROM tb_produto WHERE produto = '{input_data}'"
        consulta_produto = self.abreconn.fetch_data_one(sql)
        
