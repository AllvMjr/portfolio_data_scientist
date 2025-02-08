from arquivos.access_db import AccessDB
from arquivos.strutils import strutils
from datetime import datetime, timedelta
import numpy as np
import os

class Loadgold():

    def create_iteration_gold(self):
        abreconn = AccessDB()
        #Cria consulta ordenando por data de criação para correta ordenação
        sql = f"SELECT id_iteration, data_ini, data_fim, iteration FROM tb_iteration ORDER BY data_ini"
        consulta_iteracao = abreconn.fetch_data(sql)

        # Query para limpar a gold
        sql = f"DELETE FROM tb_gold_iteration"
        abreconn.grava_dados(sql)

        for iteration in consulta_iteracao:
            # Query para inserir os dados da
            data_ini  = iteration[1].strftime("%Y-%m-%d")
            data_fim =  iteration[2].strftime("%Y-%m-%d")
            sql = f"INSERT INTO tb_gold_iteration (data_ini, data_fim, iteration) VALUES ('{data_ini}', '{data_fim}', '{iteration[3]}')"
            abreconn.grava_dados(sql)

    def create_biiteration_gold(self):
        abreconn = AccessDB()
        # Cria consulta para criação correta ordenação
        sql = f"SELECT id_bi_iteration, data_ini, data_fim, iteration FROM tb_bi_iteration ORDER by data_ini"
        consulta_biiteration = abreconn.fetch_data(sql)

        #Query para limpar a bi gold
        sql = 'DELETE FROM tb_gold_bi_iteration'
        abreconn.grava_dados(sql)

        for biiteration in consulta_biiteration:
            # Query para inserir os dados da
            data_ini  = biiteration[1].strftime("%Y-%m-%d")
            data_fim =  biiteration[2].strftime("%Y-%m-%d")
            sql = f"INSERT INTO tb_gold_bi_iteration (data_ini, data_fim, iteration) VALUES ('{data_ini}', '{data_fim}', '{biiteration[3]}')"
            abreconn.grava_dados(sql)
    
    def add_iteration_gold(self):
        abreconn = AccessDB()

        # Localiza o último registro da tabela gold iteration
        sql = f"SELECT LAST(data_fim) as ultima_data FROM tb_gold_iteration"
        lastdatafim = abreconn.fetch_data_one(sql)

        datahoje = datetime.today().strftime('%Y-%m-%d')
        ultimadata = lastdatafim[0].strftime('%Y-%m-%d')
        if datahoje > ultimadata:
            dataserie = strutils.get_next_interation_weekly(datetime.today())
            sql = f"INSERT INTO tb_gold_iteration (data_ini, data_fim, iteration) VALUES ('{dataserie.get('inicio_iteracao')}', '{dataserie.get('fim_iteracao')}', '{dataserie.get('cut_inicio')} a {dataserie.get('cut_fim')}')"
            abreconn.grava_dados(sql)

    def add_biinteration_gold(self):
        abreconn = AccessDB()

        # Localiza o último registro da tabela gold iteration
        sql = f"SELECT LAST(data_fim) as ultima_data FROM tb_gold_bi_iteration"
        lastdatafim = abreconn.fetch_data_one(sql)
        
        datahoje = datahoje = datetime.today().strftime('%Y-%m-%d')
        ultimadata = lastdatafim.strftime('%Y-%m-%d')

        if datahoje > ultimadata:
            dataserie = strutils.get_next_binteration_weekly(datetime.today())
            sql = f"INSERT INTO tb_gold_iteration (data_ini, data_fim, iteration) VALUES ('{dataserie.get('inicio_iteracao')}', '{dataserie.get('fim_iteracao')}', '{dataserie.get('cut_inicio')} a {dataserie.get('cut_fim')}')"
            abreconn.grava_dados(sql)

            