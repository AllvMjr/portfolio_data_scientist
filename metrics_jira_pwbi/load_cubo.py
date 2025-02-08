from arquivos.access_db import AccessDB
from arquivos.api_client import APIClient
from arquivos.strutils import strutils
from arquivos.interation_db import InterationDb
from datetime import datetime, timedelta
import numpy as np
from dotenv import load_dotenv
import os
import json

class LoadCubo:

    # Carrega os primeiros dados do cubo para geraçãod as métricas
    def load_cubo(self):
        load_dotenv()
        abreconn = AccessDB()

        #limpa tabela do cubo
        #sqlde = 'DELETE FROM tb_cubo'
        #abreconn.grava_dados(sql)

        # Carrega o id_issue das historias
        sql = f"SELECT id_issue, id_status, issuetype, key, created, squad, updated, id_produto, produto, tipo_demanda, emailassignee, nameassignee, status, id_projeto, projeto, resolutiondate FROM tb_issue where key = 'DECS-17312'"
        consulta_issues = abreconn.fetch_data(sql)
        #datat = open('dados.txt', 'w')
        # Varre todo o DB de hitorias para buscar os andamentos de cada um

        #dadost = open('dados.txt', 'w')
        for dados_issues in consulta_issues:
            campo = ''
            value = ''
            id_issue = dados_issues[0]
            campo += 'id_issue, '
            value += f"{id_issue}, "
            id_status = dados_issues[1]
            campo += 'id_status, '
            value += f"{id_status}, "
            issuetype = dados_issues[2]
            campo += 'issuetype, '
            value += f"'{issuetype}', "
            key = dados_issues[3]
            campo += 'key, '
            value += f"'{key}', "
            created = dados_issues[4]
            campo += 'created, '
            datecreated = created.strftime("%Y-%m-%d")
            value += f"'{datecreated}', "            
            squad = dados_issues[5]
            if squad is not None:
                campo += 'squad, '
                value += f"'{squad}', "
            updated = dados_issues[6]
            dateupdated = updated.strftime("%Y-%m-%d")
            campo += 'updated, '
            value += f"'{dateupdated}', "
            id_produto = dados_issues[7]
            if id_produto is not None:
                campo += 'id_produto, '
                value += f"{id_produto}, "
            produto = dados_issues[8]
            if produto is not None:
                campo += 'produto, '
                value += f"'{produto}', "
            tipo_demanda = dados_issues[9]
            if tipo_demanda is not None:
                campo += 'tipo_demanda, '
                value += f"'{tipo_demanda}', "
            retorna_interacao = strutils.get_interation_weekly(created)
            weekwork = retorna_interacao.get('semana')
            campo += 'weekwork, '
            value += f"'{weekwork}', "
            dayfist_iteration = retorna_interacao.get('inicio_iteracao')
            campo += 'dayfisrt_iteration, '
            value += f"'{dayfist_iteration}', "
            lastlast_iteration = retorna_interacao.get('fim_iteracao')
            campo += 'daylast_iteration, '
            value += f"'{lastlast_iteration}', "

            iteracao = f"{retorna_interacao.get('cut_inicio')} a {retorna_interacao.get('cut_fim')}"
            campo += 'iteracao, '
            value += f"'{iteracao}', "

            email_author = dados_issues[10]
            campo += 'email_author, '
            value += f"'{email_author}', "            
            name_author = dados_issues[11]
            campo += 'name_author, '
            value += f"'{name_author}', "
            status = dados_issues[12]
            campo += 'status, '
            value += f"'{status}', "

            id_projeto = dados_issues[13]
            campo += 'id_projeto '
            value += f"'{id_projeto}' "
            

            # Seleciona o primeiro in progress ou andamento
            sql = f"SELECT id_changelog, data_change FROM tb_changelog WHERE id_issue = {id_issue} AND id_from_status = 12940"
            get_andamento = abreconn.fetch_data_one(sql)            
            if get_andamento is not None:
                id_changelog_in = get_andamento[0]                
                dateprogress = get_andamento[1].strftime("%Y-%m-%d")
                campo += ', id_changelog_in, dateprogress '
                value += f", {id_changelog_in}, '{dateprogress}'"
            else:
                id_changelog_in = ''
                dateprogress = ''
            
            # Seleciona o ultimo andamento (done)
            sql = f"SELECT id_changelog, data_change FROM tb_changelog WHERE id_issue = {id_issue} AND id_to_status = 13345"
            get_done = abreconn.fetch_data_one(sql)
            if get_done is not None:
                id_changelog_done = get_done[0]
                datedone = get_done[1].strftime("%Y-%m-%d")
                campo += ', id_changelog_done, datedone '
                value += f", {id_changelog_done}, '{datedone}'"
            else:
                id_changelog_done = ''
                datedone = ''

            checadataprogress = np.nonzero(dateprogress)
            checadatedone = np.nonzero(datedone)

            if id_status == 13345:
                if (checadatedone[0] == 0):
                    leadtime = strutils.diferenca_entre_datas(datecreated, datedone)
                    campo += ', leadtime '
                    value += f", {leadtime}"
            
            
            if (checadataprogress[0] == 0 and checadatedone[0] == 0):
                if (dateprogress is not None and datedone is not None):
                    cycletime = strutils.diferenca_entre_datas(dateprogress, datedone)
                    campo += ', cycletime '
                    value += f", {cycletime}"
            
            checaprojeto = np.nonzero(dados_issues[14])
            if checaprojeto[0] == 0:
                campo += ', projeto '

                value += f", '{dados_issues[14]}'"

            checaresolution = np.nonzero(dados_issues[15])
            if checaresolution[0] == 0:
                resolution = dados_issues[15].strftime("%Y-%m-%d")
                #resolution = resolution(strutils.cut_data(dados_issues[15]))
                campo += ', resolutiondate '
                value += f", '{resolution}'"

                #grava biweekly
                convert_data = dados_issues[15].strftime('%d-%m-%Y')
                sql = f"SELECT data_ini, data_fim FROM tb_bi_iteration WHERE #{convert_data}# BETWEEN data_ini AND data_fim"
                consulta = abreconn.fetch_data_one(sql)
                if consulta is not None:
                    data_ini = consulta[0].strftime("%Y-%m-%d")
                    data_fim = consulta[1].strftime("%Y-%m-%d")
                    iteracao_ini = consulta[0].strftime("%d-%m")
                    iteracao_fim = consulta[1].strftime("%d-%m")

                    campo += ', bidayfisrt_iteration, bidaylast_iteration, biiteracao'
                    value += f", '{data_ini}', '{data_fim}', '{iteracao_ini} a {iteracao_fim}'"

            
            sql = f"INSERT INTO tb_cubo ({campo}) VALUES ({value}) "
            #dadost.write(f"{sql}\n")
            
            abreconn.grava_dados(sql)
            #print(f"Registro da issuetype: {issuetype} - {key}, cadastrado com sucesso!")

    def get_biweekly_all(self):
        load_dotenv()
        abreconn = AccessDB()
        data_alvo = ''        
        conta = 0

        sql = 'SELECT MIN(resolutiondate) AS firstrecord from tb_issue'
        primeira_data = abreconn.fetch_data_one(sql)
        
        if conta == 0:
            data_alvo = '2024-11-01'
            data_alvo = datetime.strptime(data_alvo, '%Y-%m-%d')


        while primeira_data[0] <= data_alvo:
            data_retro_ini = ''
            data_retro_fim = ''
            if conta >= 1:
                data_alvo = data_alvo - timedelta(days=3)
                data_retro_ini = strutils.get_iteration_biweekly(data_alvo)
                data_ini = data_retro_ini
                data_retro_fim = data_alvo
                data_fim = data_retro_fim
                data_alvo = data_retro_ini
                conta + 1
            else:
                data_retro_ini = strutils.get_iteration_biweekly(data_alvo)
                data_ini = data_retro_ini
                data_retro_fim = primeira_data[0]
                data_fim = data_alvo
                data_alvo = data_retro_ini
                conta += 1
            
            data_ini_grava = data_ini.strftime("%Y-%m-%d")
            data_fim_grava = data_fim.strftime("%Y-%m-%d")
            #iteration_bi = "{'Data Inicio': '" + data_ini.strftime("%Y-%m-%d") +"': 'Data Fim': '"+ data_fim.strftime("%Y-%m-%d") +"'}"
            sql = f"INSERT INTO tb_bi_iteration (data_ini, data_fim) VALUES ('{data_ini_grava}', '{data_fim_grava}')"
            abreconn.grava_dados(sql)
            
    def update_bi_cubo(self):
        load_dotenv()
        abreconn = AccessDB()

        #Consulta data resolução Cubo
        sql = f"SELECT resolutiondate, id_issue FROM tb_cubo"
        consulta_resolution = abreconn.fetch_data(sql)
        
        for datas in consulta_resolution:
            data_resolution = datas[0]
            id_issue = datas[1]
            checa = np.nonzero(data_resolution)
            if checa[0] == 0:
                convert_data = datas[0].strftime('%d-%m-%Y')
                sql = f"SELECT data_ini, data_fim FROM tb_bi_iteration WHERE #{convert_data}# BETWEEN data_ini AND data_fim"
                consulta = abreconn.fetch_data_one(sql)
                if consulta is not None:
                    data_ini = consulta[0].strftime("%Y-%m-%d")
                    data_fim = consulta[1].strftime("%Y-%m-%d")
                    iteracao_ini = consulta[0].strftime("%d-%m")
                    iteracao_fim = consulta[1].strftime("%d-%m")
                    sql = f"UPDATE tb_cubo SET bidayfisrt_iteration = '{data_ini}', bidaylast_iteration =  '{data_fim}', biiteracao = '{iteracao_ini} a {iteracao_fim}' WHERE id_issue = {id_issue}"
                    abreconn.grava_dados(sql)

    # Atualiza os dados do cubo para geraçãod as métricas
    def update_cubo(self):
        load_dotenv()
        abreconn = AccessDB()

        # Carrega o id_issue das historias
        #sql = f"SELECT DISTINCT tb_issue.id_issue, tb_issue.id_status, tb_issue.issuetype, tb_issue.key, tb_issue.created, tb_issue.squad, tb_issue.updated, tb_issue.id_produto, tb_issue.produto, tb_issue.tipo_demanda, tb_issue.emailassignee, tb_issue.nameassignee, tb_issue.status, tb_issue.id_projeto, tb_issue.projeto, tb_issue.resolutiondate FROM tb_issue INNER JOIN tb_changelog ON tb_issue.id_issue = tb_changelog.id_issue WHERE tb_changelog.id_to_status Not In ({os.getenv('JIRA_IGNORE_STATUS')})"
        sql = f"SELECT DISTINCT tb_issue.id_issue, tb_issue.id_status, tb_issue.issuetype, tb_issue.key, tb_issue.created, tb_issue.squad, tb_issue.updated, tb_issue.id_produto, tb_issue.produto, tb_issue.tipo_demanda, tb_issue.emailassignee, tb_issue.nameassignee, tb_issue.status, tb_issue.id_projeto, tb_issue.projeto, tb_issue.resolutiondate FROM tb_issue INNER JOIN tb_changelog ON tb_issue.id_issue = tb_changelog.id_issue"
        consulta_issues = abreconn.fetch_data(sql)
        #datat = open('dados.txt', 'w')
        # Varre todo o DB de hitorias para buscar os andamentos de cada um

        #dadostc = open('dadosc.txt', 'w')
        for cubo_issues in consulta_issues:
            value = ''
            campo = ''
            #Consulta para ver se tem o cubo na base
            sql = f"SELECT id_issue FROM tb_cubo where id_issue = {cubo_issues[0]}"
            conncubo = abreconn.fetch_data(sql)
            checacubo = np.nonzero(conncubo)

            # Atualiza o que existe na base
            if checacubo[0] == 0:               
                
                id_issue = cubo_issues[0]   

                id_status = cubo_issues[1]
                value += f"id_status = {id_status}, "

                issuetype = cubo_issues[2]
                value += f"issuetype = '{issuetype}', "

                key = cubo_issues[3]
                value += f"key = '{key}', "

                created = cubo_issues[4]
                datecreated = created.strftime("%Y-%m-%d")
                value += f"created = '{datecreated}', "   

                squad = cubo_issues[5]
                if squad is not None:
                    value += f"squad = '{squad}', "

                updated = cubo_issues[6]
                dateupdated = updated.strftime("%Y-%m-%d")
                value += f"updated = '{dateupdated}', "

                id_produto = cubo_issues[7]
                if id_produto is not None:
                    value += f"id_produto = {id_produto}, "

                produto = cubo_issues[8]
                if produto is not None:
                    value += f"produto = '{produto}', "

                tipo_demanda = cubo_issues[9]
                if tipo_demanda is not None:
                    value += f"tipo_demanda = '{tipo_demanda}', "                

                email_author = cubo_issues[10]
                value += f"email_author = '{email_author}', "  

                name_author = cubo_issues[11]
                value += f"name_author = '{name_author}', "

                status = cubo_issues[12]
                value += f"status = '{status}', "

                id_projeto = cubo_issues[13]
                value += f"id_projeto = {id_projeto} "
                

                # Seleciona o primeiro in progress ou andamento
                sql = f"SELECT id_changelog, data_change FROM tb_changelog WHERE id_issue = {id_issue} AND (id_from_status = 12940 OR id_to_status = 12940)"
                get_andamento = abreconn.fetch_data_one(sql)            
                if get_andamento is not None:
                    id_changelog_in = get_andamento[0]
                    iterationprogress = get_andamento[1]            
                    dateprogress = get_andamento[1].strftime("%Y-%m-%d")
                    value += f", id_changelog_in = {id_changelog_in}, dateprogress = '{dateprogress}' "
                else:
                    id_changelog_in = ''
                    dateprogress = ''
                    iterationprogress = None
                
                # Seleciona o ultimo andamento (done)
                sql = f"SELECT id_changelog, data_change FROM tb_changelog WHERE id_issue = {id_issue} AND id_to_status = 13345"
                get_done = abreconn.fetch_data_one(sql)
                if get_done is not None:
                    id_changelog_done = get_done[0]
                    iterationdone = get_done[1]
                    datedone = get_done[1].strftime("%Y-%m-%d")
                    value += f",id_changelog_done =  {id_changelog_done}, datedone = '{datedone}'"
                else:
                    id_changelog_done = ''
                    datedone = ''
                    iterationdone = None
                
                if (iterationprogress is None and iterationdone is None):
                    retorna_interacao = strutils.get_interation_weekly(created)
                elif (iterationprogress is not None and iterationdone is None):
                    retorna_interacao = strutils.get_interation_weekly(iterationprogress)
                elif (iterationprogress is None and iterationdone is not None):
                    retorna_interacao = strutils.get_interation_weekly(iterationdone)
                elif (iterationprogress is not None and iterationdone is not None):
                    retorna_interacao = strutils.get_interation_weekly(iterationdone)
                else:
                    retorna_interacao = strutils.get_interation_weekly(created)

                weekwork = retorna_interacao.get('semana')
                value += f", weekwork = '{weekwork}', "

                dayfist_iteration = retorna_interacao.get('inicio_iteracao')
                value += f"dayfisrt_iteration = '{dayfist_iteration}', "

                lastlast_iteration = retorna_interacao.get('fim_iteracao')
                value += f"daylast_iteration = '{lastlast_iteration}', "

                iteracao = f"{retorna_interacao.get('cut_inicio')} a {retorna_interacao.get('cut_fim')}"
                value += f"iteracao = '{iteracao}' "

                ## Area de Tratamento das Iterações
                # Conexão com a iteração Gold
                conn_iteracao = InterationDb()
                verifica_iteracao = conn_iteracao.get_gold_id_interation_duedate(strutils.get_data_search_access(dayfist_iteration), strutils.get_data_search_access(lastlast_iteration))
                
                # Se voltar Vazio, insere um novo registro na tabel iteração para criar o indice
                if verifica_iteracao is None:
                    #grava = conn_iteracao.save_gold_iteration(dayfist_iteration, lastlast_iteration, iteracao)                    
                    #print(f"Nova Iteração {iteracao} gravada com sucesso\n")

                    verifica_iteracao = conn_iteracao.get_gold_id_interation_duedate(strutils.get_data_search_access(dayfist_iteration), strutils.get_data_search_access(lastlast_iteration))
                
                
                value += f", id_iteration = '{verifica_iteracao}' "
                
                                    
                checadataprogress = np.nonzero(dateprogress)
                checadatedone = np.nonzero(datedone)

                if id_status == 13345:
                    if (checadatedone[0] == 0):
                        leadtime = strutils.diferenca_entre_datas(datecreated, datedone)
                        value += f", leadtime = {leadtime}"
                
                if (checadataprogress[0] == 0 and checadatedone[0] == 0):
                    if (dateprogress is not None and datedone is not None):
                        cycletime = strutils.diferenca_entre_datas(dateprogress, datedone)
                        value += f", cycletime = {cycletime}"
                
                checaprojeto = np.nonzero(cubo_issues[14])
                if checaprojeto[0] == 0:
                    value += f", projeto = '{cubo_issues[14]}'"

                checaresolution = np.nonzero(cubo_issues[15])
                if checaresolution[0] == 0:
                    resolution = cubo_issues[15].strftime("%Y-%m-%d")
                    #resolution = resolution(strutils.cut_data(cubo_issues[15]))
                    value += f", resolutiondate = '{resolution}'"

                    #grava biweekly
                    convert_data = cubo_issues[15].strftime('%d-%m-%Y')
                    sql = f"SELECT data_ini, data_fim, id_gold_bi_iteration FROM tb_gold_bi_iteration WHERE #{convert_data}# BETWEEN data_ini AND data_fim"
                    
                    consulta = abreconn.fetch_data_one(sql)
                    if consulta is not None:
                        data_ini = consulta[0].strftime("%Y-%m-%d")
                        data_fim = consulta[1].strftime("%Y-%m-%d")
                        iteracao_ini = consulta[0].strftime("%d-%m")
                        iteracao_fim = consulta[1].strftime("%d-%m")
                        
                        
                        value += f", bidayfisrt_iteration = '{data_ini}', bidaylast_iteration = '{data_fim}', biiteracao = '{iteracao_ini} a {iteracao_fim}', id_bi_iteration = {consulta[2]}"

                
                sql = f"UPDATE tb_cubo SET {value} WHERE id_issue = {id_issue} "
                #datat.write(f"{sql}\n")
                
                abreconn.grava_dados(sql)
                print(f"Cubo da issuetype: {issuetype} - {key}, atualizado com sucesso!")
            else:
                #Insere um novo na base
                id_issue = cubo_issues[0]
                campo += 'id_issue, '
                value += f"{id_issue}, "
                id_status = cubo_issues[1]
                campo += 'id_status, '
                value += f"{id_status}, "
                issuetype = cubo_issues[2]
                campo += 'issuetype, '
                value += f"'{issuetype}', "
                key = cubo_issues[3]
                campo += 'key, '
                value += f"'{key}', "
                created = cubo_issues[4]
                campo += 'created, '
                datecreated = created.strftime("%Y-%m-%d")
                value += f"'{datecreated}', "            
                squad = cubo_issues[5]
                if squad is not None:
                    campo += 'squad, '
                    value += f"'{squad}', "
                updated = cubo_issues[6]
                dateupdated = updated.strftime("%Y-%m-%d")
                campo += 'updated, '
                value += f"'{dateupdated}', "
                id_produto = cubo_issues[7]
                if id_produto is not None:
                    campo += 'id_produto, '
                    value += f"{id_produto}, "
                produto = cubo_issues[8]
                if produto is not None:
                    campo += 'produto, '
                    value += f"'{produto}', "
                tipo_demanda = cubo_issues[9]
                if tipo_demanda is not None:
                    campo += 'tipo_demanda, '
                    value += f"'{tipo_demanda}', "                

                email_author = cubo_issues[10]
                campo += 'email_author, '
                value += f"'{email_author}', "            
                name_author = cubo_issues[11]
                campo += 'name_author, '
                value += f"'{name_author}', "
                status = cubo_issues[12]
                campo += 'status, '
                value += f"'{status}', "

                id_projeto = cubo_issues[13]
                campo += 'id_projeto '
                value += f"'{id_projeto}' "
                

                # Seleciona o primeiro in progress ou andamento
                sql = f"SELECT id_changelog, data_change FROM tb_changelog WHERE id_issue = {id_issue} AND id_from_status = 12940"
                get_andamento = abreconn.fetch_data_one(sql)            
                if get_andamento is not None:
                    id_changelog_in = get_andamento[0] 
                    iterationprogress = get_andamento[1]               
                    dateprogress = get_andamento[1].strftime("%Y-%m-%d")
                    campo += ', id_changelog_in, dateprogress '
                    value += f", {id_changelog_in}, '{dateprogress}' "
                else:
                    id_changelog_in = ''
                    dateprogress = ''
                    iterationprogress = None
                
                # Seleciona o ultimo andamento (done)
                sql = f"SELECT id_changelog, data_change FROM tb_changelog WHERE id_issue = {id_issue} AND id_to_status = 13345"
                get_done = abreconn.fetch_data_one(sql)
                if get_done is not None:
                    id_changelog_done = get_done[0]
                    iterationdone = get_done[1]
                    datedone = get_done[1].strftime("%Y-%m-%d")
                    campo += ', id_changelog_done, datedone '
                    value += f", {id_changelog_done}, '{datedone}' "
                else:
                    id_changelog_done = ''
                    datedone = ''
                    iterationdone = None

                if (iterationprogress is None and iterationdone is None):
                    retorna_interacao = strutils.get_interation_weekly(created)
                elif (iterationprogress is not None and iterationdone is None):
                    retorna_interacao = strutils.get_interation_weekly(iterationprogress)
                elif (iterationprogress is not None and iterationdone is not None):
                    retorna_interacao = strutils.get_interation_weekly(iterationdone)
                else:
                    retorna_interacao = strutils.get_interation_weekly(created)

                weekwork = retorna_interacao.get('semana')
                campo += ', weekwork, '
                value += f", {weekwork}, "
                dayfist_iteration = retorna_interacao.get('inicio_iteracao')
                campo += 'dayfisrt_iteration, '
                value += f"'{dayfist_iteration}', "
                lastlast_iteration = retorna_interacao.get('fim_iteracao')
                campo += 'daylast_iteration, '
                value += f"'{lastlast_iteration}', "

                iteracao = f"{retorna_interacao.get('cut_inicio')} a {retorna_interacao.get('cut_fim')}"
                campo += ' iteracao '
                value += f" '{iteracao}' "
                
                ## Area de Tratamento das Iterações
                # Conexão com a iteração Gold
                conn_iteracao = InterationDb()
                verifica_iteracao = conn_iteracao.get_gold_id_interation_duedate(strutils.get_data_search_access(dayfist_iteration), strutils.get_data_search_access(lastlast_iteration))
                
                # Se voltar Vazio, insere um novo registro na tabel iteração para criar o indice
                if verifica_iteracao is None:
                    #grava = conn_iteracao.save_gold_iteration(dayfist_iteration, lastlast_iteration, iteracao)                    
                    #print(f"Nova Iteração {iteracao} gravada com sucesso\n")

                    verifica_iteracao = conn_iteracao.get_gold_id_interation_duedate(strutils.get_data_search_access(dayfist_iteration), strutils.get_data_search_access(lastlast_iteration))
                try:
                    campo += ', id_iteration'
                    value += f", '{verifica_iteracao}' "
                except TypeError as k:
                    itev = ''
                
                checadataprogress = np.nonzero(dateprogress)
                checadatedone = np.nonzero(datedone)

                if id_status == 13345:
                    if (checadatedone[0] == 0):
                        leadtime = strutils.diferenca_entre_datas(datecreated, datedone)
                        campo += ', leadtime '
                        value += f", {leadtime}"
                
                
                if (checadataprogress[0] == 0 and checadatedone[0] == 0):
                    if (dateprogress is not None and datedone is not None):
                        cycletime = strutils.diferenca_entre_datas(dateprogress, datedone)
                        campo += ', cycletime '
                        value += f", {cycletime}"
                
                checaprojeto = np.nonzero(cubo_issues[14])
                if checaprojeto[0] == 0:
                    campo += ', projeto '

                    value += f", '{cubo_issues[14]}'"

                checaresolution = np.nonzero(cubo_issues[15])
                if checaresolution[0] == 0:
                    resolution = cubo_issues[15].strftime("%Y-%m-%d")
                    #resolution = resolution(strutils.cut_data(cubo_issues[15]))
                    campo += ', resolutiondate '
                    value += f", '{resolution}'"

                    #grava biweekly
                    convert_data = cubo_issues[15].strftime('%d-%m-%Y')
                    sql = f"SELECT data_ini, data_fim, id_gold_bi_iteration FROM tb_gold_bi_iteration WHERE #{convert_data}# BETWEEN data_ini AND data_fim"
                    consulta = abreconn.fetch_data_one(sql)
                    if consulta is not None:
                        data_ini = consulta[0].strftime("%Y-%m-%d")
                        data_fim = consulta[1].strftime("%Y-%m-%d")
                        iteracao_ini = consulta[0].strftime("%d-%m")
                        iteracao_fim = consulta[1].strftime("%d-%m")

                        campo += ', bidayfisrt_iteration, bidaylast_iteration, biiteracao, id_bi_iteration '
                        value += f", '{data_ini}', '{data_fim}', '{iteracao_ini} a {iteracao_fim}', {consulta[2]} "

                
                sql = f"INSERT INTO tb_cubo ({campo}) VALUES ({value}) "
                #datat.write(f"{sql}\n")
                
                abreconn.grava_dados(sql)
                print(f"Cubo da issuetype: {issuetype} - {key}, inserido com sucesso!")