from arquivos.access_db import AccessDB
from arquivos.api_client import APIClient
from arquivos.strutils import strutils
from datetime import datetime, timedelta
import numpy as np
from dotenv import load_dotenv
import os
import json

class LoadProject:

    def load_all_projects(self):       

        load_dotenv()

        client = APIClient()
        projetos = client.get(os.getenv('JIRA_PROJECTS'))

        abreconn = AccessDB()
        for dados in projetos:
            id = int(dados['id'])
            sigla = dados['key']
            name = dados['name']

            sql = f"INSERT INTO tb_projetos (id_projeto, sigla, nome) VALUES ({id}, '{sigla}', '{name}')"
            
            
            #abreconn.grava_dados(sql)

        abreconn.close()

    def load_status():

        load_dotenv()
        abreconn = AccessDB()      

        #Busca Status
        busca_status = APIClient()
        listaStatus = busca_status.get(os.getenv('JIRA_STATUS'))

        for status in listaStatus:
            idstatus = status['id']
            namestatus = status['name']
            try:
                untranslatedname = status['untranslatedName']
            except KeyError as k:
                untranslatedname = ''
            
            sql = f"INSERT INTO tb_status (id_Status, name, untranslatedname) VALUES ({idstatus}, '{namestatus}', '{untranslatedname}')"

            abreconn.grava_dados(sql)
        
        abreconn.close()

    def load_all_issues_project(self):
        conta = 0
        startat = 0
        qtd_registros = 200
        total_registros = 0
        qtd_restante = 0
        

        load_dotenv()
        abreconn = AccessDB()
        abre_projetos = ''
        limpa_projetos = ''
        sql = f"SELECT sigla FROM tb_projetos WHERE sigla in ({os.getenv('JIRA_METRICS')})"
        #sql = f"SELECT sigla FROM tb_projetos WHERE sigla in ('TEX')"
        
        lista_projetos = abreconn.fetch_data(sql)
        #dadost =  open('dados.txt', 'w')
        for abreprojetos in lista_projetos:           

            projeto = abreprojetos[0]
            
            #Busca as historias dos projetos
            busca_historias = APIClient()
            link_projetos = f"{os.getenv('JIRA_SEARCH_PROJECT')}{projeto}&startAt={startat}&maxResults={qtd_registros}"
            historias = busca_historias.get(link_projetos)
            
            total_registros = historias['total']
            
            while qtd_restante < total_registros:
                if startat > 0:
                    link_projetos = f"{os.getenv('JIRA_SEARCH_PROJECT')}{projeto}&startAt={startat}&maxResults={qtd_registros}"
                    historias = busca_historias.get(link_projetos)

                dados_issue = historias['issues']
                
                for dados in dados_issue:
                    campos =  ''
                    values = ''

                    # Inserindo o Id do jira
                    id_issue = dados['id']
                    campos += 'id_issue, '
                    values += id_issue + ', '

                    # Inserindo o id do projeto
                    id_projeto = dados['fields']['project']['id']
                    campos += 'id_projeto, '
                    values += id_projeto + ', '

                    #Inserindo a sigla do projeto
                    campos += 'projeto, '
                    values += f"'{projeto}', "

                    # Inserindo a chade da historia
                    key_hit = dados['key']
                    campos += 'key, '
                    values += f"'{key_hit}', "   
                                 

                    #Insere os dados da chave da atividade Ex: DECS-4234
                    try:
                        if (dados['fields']['parent'] != 'null' or dados['fields']['parent'] is not None ):
                            try:
                                id_parent = dados['fields']['parent']['id']
                                key_parent = dados['fields']['parent']['key']
                                campos += 'id_parent, key_parent, '
                                values += f"{id_parent}, '{key_parent}',"
                                
                            except KeyError as ke:
                                id_p= ''
                    except KeyError as ke:
                        id_p= ''

                                    
                    # Inserindo os Status
                    if (dados['fields']['status'] is not None or dados['fields']['status'] != 'null'):
                        id_status = dados['fields']['status']['id']
                        status = dados['fields']['status']['name']
                        campos += 'id_status, '
                        values += id_status + ', '
                        campos += 'status, '
                        values += f"'{status}', "
                    else:
                        id_status = ''
                        status = '' 
                    
                    #Insere dados da sprint se existir
                    try:
                        if(dados['fields']['customfield_10007'] is not None or dados['fields']['customfield_10007'] != 'null'):
                            try:
                                for sprints in dados['fields']['customfield_10007']:
                                    status = sprints['state']
                                    if status == 'active':
                                        id_sprint = sprints['id']
                                        sprint = sprints['name']
                                        sprint_dataini = strutils.cut_data(sprints['startDate'])
                                        sprint_datater = strutils.cut_data(sprints['endDate'])

                                        campos += 'id_sprint, sprint, sprint_dataini, sprint_datater, '
                                        values += f"{id_sprint}, '{sprint}', '{sprint_dataini}', '{sprint_datater}', "
                                        
                            except TypeError as ty:
                                retsprint= ''                       
                    except KeyError as ke:
                        retsprint= ''

                    # Insere o tipo da atividade
                    issuetype = dados['fields']['issuetype']['name']
                    campos += 'issuetype, '
                    values += f"'{issuetype}', "

                    # Insere a criação da atividade
                    created = strutils.cut_data(dados['fields']['created'])
                    campos += 'created, '
                    values += f"'{created}', "

                    # Insere dados da Squad
                    if dados['fields']['customfield_11779'] is not None:
                        try:
                            squad = dados['fields']['customfield_11779'][0]
                            campos += 'squad, '
                            values += f"'{squad}', "
                        except KeyError as ke:
                            squad = ''
                    else:
                        dsquad = ''  

                    #Insere dados do Criador da atividade              
                    if (dados['fields']['creator'] is not None or dados['fields']['creator'] != 'null'):                    
                        try:
                            #link_creator = busca_historias.get(os.getenv('JIRA_SEARCH_USER') + dados['fields']['creator']['accountId'])
                            emailcreator = dados['fields']['creator']['emailAddress']
                            namecreator = dados['fields']['creator']['displayName']
                            campos += 'emailcreator, namecreator, '
                            values += f"'{emailcreator}', '{namecreator}', "
                            
                        except KeyError:
                            criacao = ''

                    #Insere se tem um responsável
                    if (dados['fields']['assignee'] is None or dados['fields']['assignee'] == 'null'):
                        assin = ''
                    else:
                        try:
                            emailassignee = dados['fields']['assignee']['emailAddress']
                            nameassignee = dados['fields']['assignee']['displayName']
                            campos += 'emailassignee, nameassignee, '
                            values += f"'{emailassignee}', '{nameassignee}', "                          
                        except KeyError as ke:
                            assin = ''

                    #Insere as categorias
                    try:
                        if (dados['fields']['labels'] is not None or dados['fields']['labels'] != 'null'):
                            categorias = ', '.join(dados['fields']['labels'])
                            if categorias is not None:
                                campos += 'categorias, '
                                values += f"'{categorias}', "
                    except KeyError as k:
                        categ = ''

                    #insere o produto vinculado
                    try:
                        if(dados['fields']['customfield_11843'] is not None or dados['fields']['customfield_11843'] != 'null'):
                            try:
                                for produto in dados['fields']['customfield_11843']:
                                    if produto is not None:
                                        id_produto = produto['id']
                                        produto = produto['value']
                                        campos += 'id_produto, produto, '
                                        values += f"'{id_produto}', '{produto}', "                                    
                            except TypeError as t:
                                prod = ''
                    except KeyError as k:
                        prod = ''

                    #insere o time de demanda                    
                    demanda = dados['fields']['customfield_12053']
                    checademanda = np.nonzero(demanda)                    
                    if checademanda[0] == 0:
                        campos += 'tipo_demanda, '
                        values += f"'{demanda.get('value')}', "
                    
                    resolutiondate = dados['fields']['resolutiondate']
                    checaresolution = np.nonzero(resolutiondate)
                    if checaresolution[0] == 0:
                        campos += 'resolutiondate, '
                        values += f"'{strutils.cut_data(resolutiondate)}', "
                    
                    
                    # Insere Atualização
                    updated = strutils.cut_data(dados['fields']['updated'])
                    campos += 'updated '
                    values += f"'{updated}'"

                    # Cria o SQL áa Gravação
                    sql = f"INSERT INTO tb_issue ({campos}) VALUES ({values})"
                    abreconn.grava_dados(sql)
                    print(f"Registro gravado: {key_hit} com sucesso!\n")
                    #dadost.write(sql +"\r\t")
                    #dadost.write(f"Contatem: {conta} - id : {id_issue}, id_projeto: {id_projeto}, id_parent: {id_parent}, id_status: {id_status}, id_print: {id_sprint}, id_produto: {id_produto}, Key: {key}, keyparent: {key_parent}, issuetype: {issuetype}, created: {created}, status: {status}, updated: {updated}, squad: {squad}, emailcreator: {emailcreator}, namecreator: {namecreator}, nameassignee: {emailassignee}, emailassignee: {emailassignee}, sprint: {sprint}, sprint_dataini: {sprint_dataini}, sprint_dater: {sprint_datater}, categorias: {categorias}, produto: {produto}\r\t")
                    
                    conta += 1
                
                qtd_restante += 100
                qtd_registros += 100
                startat += 100
            startat = 0
            qtd_registros = 100
            total_registros = 0
            qtd_restante = 0
        

        abreconn.close()

    def update_all_issues_project(self):
            conta = 0
            startat = 0
            qtd_registros = 5000
            total_registros = 0
            qtd_restante = 0
            

            load_dotenv()
            abreconn = AccessDB()
            abre_projetos = ''
            limpa_projetos = ''
            sql = f"SELECT sigla FROM tb_projetos WHERE sigla in ({os.getenv('JIRA_METRICS')})"        
            
            lista_projetos = abreconn.fetch_data(sql)
            
            for abreprojetos in lista_projetos:           

                projeto = abreprojetos[0]
                
                #Busca as historias dos projetos
                busca_historias = APIClient()
                link_projetos = f"{os.getenv('JIRA_SEARCH_PROJECT')}{projeto}&startAt={startat}&maxResults={qtd_registros}"
                historias = busca_historias.get(link_projetos)
                
                total_registros = historias['total']
                
                while qtd_restante < total_registros:
                    if startat > 0:
                        link_projetos = f"{os.getenv('JIRA_SEARCH_PROJECT')}{projeto}&startAt={startat}&maxResults={qtd_registros}"
                        historias = busca_historias.get(link_projetos)

                    dados_issue = historias['issues']
                    
                    for dados in dados_issue:
                        campos =  ''
                        values = ''

                        # Inserindo o Id do jira
                        id_issue = dados['id']
                        campos += 'id_issue, '
                        values += id_issue + ', '

                        # Inserindo o id do projeto
                        id_projeto = dados['fields']['project']['id']
                        campos += 'id_projeto, '
                        values += id_projeto + ', '

                        # Inserindo a chade da historia
                        key_hit = dados['key']
                        campos += 'key, '
                        values += f"'{key_hit}', "                

                        #Insere os dados da chave da atividade Ex: DECS-4234
                        try:
                            if (dados['fields']['parent'] != 'null' or dados['fields']['parent'] is not None ):
                                try:
                                    id_parent = dados['fields']['parent']['id']
                                    key_parent = dados['fields']['parent']['key']
                                    campos += 'id_parent, key_parent, '
                                    values += f"{id_parent}, '{key_parent}',"
                                    
                                except KeyError as ke:
                                    id_p= ''
                        except KeyError as ke:
                            id_p= ''

                                        
                        # Inserindo os Status
                        if (dados['fields']['status'] is not None or dados['fields']['status'] != 'null'):
                            id_status = dados['fields']['status']['id']
                            status = dados['fields']['status']['name']
                            campos += 'id_status, '
                            values += id_status + ', '
                            campos += 'status, '
                            values += f"'{status}', "
                        else:
                            id_status = ''
                            status = '' 
                        
                        #Insere dados da sprint se existir
                        try:
                            if(dados['fields']['customfield_10007'] is not None or dados['fields']['customfield_10007'] != 'null'):
                                try:
                                    for sprints in dados['fields']['customfield_10007']:
                                        status = sprints['state']
                                        if status == 'active':
                                            id_sprint = sprints['id']
                                            sprint = sprints['name']
                                            sprint_dataini = strutils.cut_data(sprints['startDate'])
                                            sprint_datater = strutils.cut_data(sprints['endDate'])

                                            campos += 'id_sprint, sprint, sprint_dataini, sprint_datater, '
                                            values += f"{id_sprint}, '{sprint}', '{sprint_dataini}', '{sprint_datater}', "
                                            
                                except TypeError as ty:
                                    retsprint= ''                       
                        except KeyError as ke:
                            retsprint= ''

                        # Insere o tipo da atividade
                        issuetype = dados['fields']['issuetype']['name']
                        campos += 'issuetype, '
                        values += f"'{issuetype}', "

                        # Insere a criação da atividade
                        created = strutils.cut_data(dados['fields']['created'])
                        campos += 'created, '
                        values += f"'{created}', "

                        # Insere dados da Squad
                        if dados['fields']['customfield_11779'] is not None:
                            try:
                                squad = dados['fields']['customfield_11779'][0]
                                campos += 'squad, '
                                values += f"'{squad}', "
                            except KeyError as ke:
                                squad = ''
                        else:
                            dsquad = ''  

                        #Insere dados do Criador da atividade              
                        if (dados['fields']['creator'] is not None or dados['fields']['creator'] != 'null'):                    
                            try:
                                #link_creator = busca_historias.get(os.getenv('JIRA_SEARCH_USER') + dados['fields']['creator']['accountId'])
                                emailcreator = dados['fields']['creator']['emailAddress']
                                namecreator = dados['fields']['creator']['displayName']
                                campos += 'emailcreator, namecreator, '
                                values += f"'{emailcreator}', '{namecreator}', "
                                
                            except KeyError:
                                criacao = ''

                        #Insere se tem um responsável
                        if (dados['fields']['assignee'] is None or dados['fields']['assignee'] == 'null'):
                            assin = ''
                        else:
                            try:
                                emailassignee = dados['fields']['assignee']['emailAddress']
                                nameassignee = dados['fields']['assignee']['displayName']
                                campos += 'emailassignee, nameassignee, '
                                values += f"'{emailassignee}', '{nameassignee}', "                          
                            except KeyError as ke:
                                assin = ''

                        #Insere as categorias
                        try:
                            if (dados['fields']['labels'] is not None or dados['fields']['labels'] != 'null'):
                                categorias = ', '.join(dados['fields']['labels'])
                                if categorias is not None:
                                    campos += 'categorias, '
                                    values += f"'{categorias}', "
                        except KeyError as k:
                            categ = ''

                        #insere o produto vinculado
                        try:
                            if(dados['fields']['customfield_11843'] is not None or dados['fields']['customfield_11843'] != 'null'):
                                try:
                                    for produto in dados['fields']['customfield_11843']:
                                        if produto is not None:
                                            id_produto = produto['id']
                                            produto = produto['value']
                                            campos += 'id_produto, produto, '
                                            values += f"'{id_produto}', '{produto}', "                                    
                                except TypeError as t:
                                    prod = ''
                        except KeyError as k:
                            prod = ''

                        #insere o time de demanda                    
                        demanda = dados['fields']['customfield_12053']
                        checademanda = np.nonzero(demanda)                    
                        if checademanda[0] == 0:
                            campos += 'tipo_demanda, '
                            values += f"'{demanda.get('value')}', "
                    
                        
                        # Insere Atualização
                        updated = strutils.cut_data(dados['fields']['updated'])
                        campos += 'updated '
                        values += f"'{updated}'"

                        # Cria o SQL áa Gravação
                        sql = f"INSERT INTO tb_issue ({campos}) VALUES ({values})"
                        abreconn.grava_dados(sql)
                        #print(f"Registro gravado: {key_hit} com sucesso!\n")
                        #dadost.write(sql +"\r\t")
                        #dadost.write(f"Contatem: {conta} - id : {id_issue}, id_projeto: {id_projeto}, id_parent: {id_parent}, id_status: {id_status}, id_print: {id_sprint}, id_produto: {id_produto}, Key: {key}, keyparent: {key_parent}, issuetype: {issuetype}, created: {created}, status: {status}, updated: {updated}, squad: {squad}, emailcreator: {emailcreator}, namecreator: {namecreator}, nameassignee: {emailassignee}, emailassignee: {emailassignee}, sprint: {sprint}, sprint_dataini: {sprint_dataini}, sprint_dater: {sprint_datater}, categorias: {categorias}, produto: {produto}\r\t")
                        
                        conta += 1
                    
                    qtd_restante += 100
                    qtd_registros += 100
                    startat += 100
                startat = 0
                qtd_registros = 100
                total_registros = 0
                qtd_restante = 0
                

            abreconn.close()

    # Carrega os dados de historias diário
    def load_new_issues(self):
        conta = 0
        startat = 0
        limite = 10000
        qtd_registros = 100
        total_registros = 0
        qtd_restante = 0
        

        load_dotenv()
        abreconn = AccessDB()
        abre_projetos = ''
        limpa_projetos = ''
        sql = f"SELECT sigla FROM tb_projetos WHERE sigla in ({os.getenv('JIRA_METRICS')})"        
        
        lista_projetos = abreconn.fetch_data(sql)
        #dadost = open('dados.txt', 'w')
        for abreprojetos in lista_projetos:           

            projeto = abreprojetos[0]
            
            #Busca as historias dos projetos
            busca_historias = APIClient()
            link_projetos = f"{os.getenv('JIRA_SEARCH_PROJECT')}{projeto}&startAt={startat}&maxResults={qtd_registros}"
            historias = busca_historias.get(link_projetos)
            
            
            while qtd_registros <= limite:
                if startat > 0:
                    link_projetos = f"{os.getenv('JIRA_SEARCH_PROJECT')}{projeto}&startAt={startat}&maxResults={qtd_registros}"
                    historias = busca_historias.get(link_projetos)
                
                dados_issue = historias['issues']
                    
                for dados in dados_issue:
                    values = ''
                    id_issue = dados.get('id')
                    chave_historia = dados.get('key')
                    
                    #Verifica se a Historia ou ítem encontra-se concluido ou cancelado, saindo do fluxo de atualização ou inclusão
                    sql = f"SELECT id_issue FROM tb_issue WHERE id_issue = {id_issue} AND id_status NOT In ({os.getenv('JIRA_IGNORE_STATUS')})" #Ignora todos os Concluídos e Cancelados
                    consulta_status = abreconn.fetch_data_one(sql)
                    check_status = np.nonzero(consulta_status) 
                    
                    #Atualiza os status que estao diferentes de concluido, cancelado, 
                    try:
                        if (dados['fields']['parent'] != 'null' or dados['fields']['parent'] is not None ):
                            try:
                                id_parent = dados['fields']['parent']['id']
                                key_parent = dados['fields']['parent']['key']
                                values += f"id_parent = {id_parent}, key_parent = '{key_parent}', "                                    
                            except KeyError as ke:
                                id_p= ''
                    except KeyError as ke:
                        id_p= ''

                                    
                    # Inserindo os Status
                    if (dados['fields']['status'] is not None or dados['fields']['status'] != 'null'):
                        id_status = dados['fields']['status']['id']
                        status = dados['fields']['status']['name']
                        values += f"id_status = {id_status}, status = '{status}', "
                    else:
                        id_status = ''
                        status = '' 
                    
                    # Insere o tipo da atividade, pode acontecer!
                    issuetype = dados['fields']['issuetype']['name']
                    values += f"issuetype = '{issuetype}', "

                    #Insere se tem um responsável
                    if (dados['fields']['assignee'] is None or dados['fields']['assignee'] == 'null'):
                        assin = ''
                    else:
                        try:
                            emailassignee = dados['fields']['assignee']['emailAddress']
                            nameassignee = dados['fields']['assignee']['displayName']
                            values += f"emailassignee = '{emailassignee}', nameassignee = '{nameassignee}', "                          
                        except KeyError as ke:
                            assin = ''

                    #insere o produto vinculado
                    try:
                        if(dados['fields']['customfield_11843'] is not None or dados['fields']['customfield_11843'] != 'null'):
                            try:
                                for produto in dados['fields']['customfield_11843']:
                                    if produto is not None:
                                        id_produto = produto['id']
                                        produto = produto['value']
                                        values += f"id_produto = '{id_produto}', produto = '{produto}', "                                    
                            except TypeError as t:
                                prod = ''
                    except KeyError as k:
                        prod = ''

                    #insere o time de demanda                    
                    demanda = dados['fields']['customfield_12053']
                    checademanda = np.nonzero(demanda)                    
                    if checademanda[0] == 0:
                        values += f"tipo_demanda = '{demanda.get('value')}', "

                    #Insere da data de resolução
                    try:
                        if (dados['fields']['resolutiondate'] is not None):
                            dataresolucao = dados['fields']['resolutiondate']
                            values += f"resolutiondate = '{strutils.cut_data(dataresolucao)}', "
                    except KeyError as ke:
                        reso = ''                        
                    
                    # Insere Atualização
                    updated = strutils.cut_data(dados['fields']['updated'])
                    values += f"updated = '{updated}'"

                    sql = f"UPDATE tb_issue SET {values} WHERE id_issue = {id_issue}"
                    abreconn.grava_dados(sql)
                    print(f"Registro {id_issue} - key: {chave_historia}, atualizado com sucesso!")
                    #dadost.write(f"{sql}\n\t") 
                        
                                           
                        
                    
                        
                    
                qtd_restante += 100
                qtd_registros += 100
                startat += 100
            startat = 0
            qtd_registros = 100
            total_registros = 0
            qtd_restante = 0
            
        abreconn.close()

    # Carrega os andamentos um por um
    def load_one_changelog_key(self, id_issue, key):
        
        id_from = ''
        from_status = ''
        id_to = ''
        to_status = ''
        conta = 0
        contaj = 0
        verifica_status_jira = ''

        load_dotenv()
        abreconn = AccessDB()

        link_changelog = f"{os.getenv('JIRA_ISSUE')}/{key}/changelog"
        lista_changelog = APIClient()
        changelog = lista_changelog.get(link_changelog)
        
        dados_changelog = changelog['values']
        
        contadaddos = len(dados_changelog)
        #dadost = open('dados.txt', 'w')
        for i in range(0, contadaddos):
            data_status = strutils.cut_data(dados_changelog[i]['created'])
            id_changelog = dados_changelog[i]['id']
            # Verifica se existe o id_changelog gravado no DB
            sql = f"SELECT id_changelog FROM tb_changelog WHERE id_changelog = {id_changelog}"
            #dadost.write(f"{sql}\n\t")
            consulta_changelog = abreconn.fetch_data_one(sql)            
            verifica_changelog = np.nonzero(consulta_changelog)  
            #print(f"SQL -> {sql}\r\t -> consulta_changelog -> {consulta_changelog} - Valor verifica -> {verifica_changelog}")
            if verifica_changelog[0] == 0:
                jatem = True
            else:
                try:                
                    author = dados_changelog[i]['author']
                    email_author = author.get('emailAddress')
                    name_author = author.get('displayName')
                    
                except KeyError as k:
                    author = ''
                    email_author = ''
                    name_author = ''

                conta_itens = len(dados_changelog[i]['items'])
                for j in range(0, conta_itens):
                    campos_andamentos = dados_changelog[i]['items'][j]

                    verifica_field = campos_andamentos.get('field')
                    verifica_field_d = campos_andamentos.get('fieldId')                   
                    
                    if (verifica_field == 'status' and verifica_field_d == 'status'):
                        id_from = campos_andamentos.get('from')
                        from_status = campos_andamentos.get('fromString')
                        id_to = campos_andamentos.get('to')
                        to_status = campos_andamentos.get('toString')
                        contaj += 1                                           
                
                        sql = f"INSERT INTO tb_changelog (id_changelog, id_issue, key, id_from_status, from_status, id_to_status, to_status, data_change, email_autor, name_auhor) VALUES ({id_changelog}, {id_issue}, '{key}', {id_from}, '{from_status}', {id_to}, '{to_status}', '{data_status}', '{email_author}', '{name_author}')"
                        #print(f"{sql}\n")
                        abreconn.grava_dados(sql)
                        print(f"Andamento: {id_changelog} na atividade {key} gravado com sucesso!\n ")
                        #dadost.write(f"{sql}\n\t")
            


    # Carrega todos os andamentos
    def load_chancelog(self, *diario):
        id_from = ''
        from_status = ''
        id_to = ''
        to_status = ''
        conta = 0
        contaj = 0
        verifica_status_jira = ''

        load_dotenv()

        abreconn = AccessDB()
        check_diario = ','.join(diario)
        if check_diario is None or check_diario == '':
            sql = f"SELECT id_issue, key FROM tb_issue"
        else:
            limpa = ','.join(diario)
            sql = f"SELECT id_issue, key FROM tb_issue WHERE key = '{limpa}'"
        
        #sql = f"SELECT id_issue, key FROM tb_issue"
        lista_change = abreconn.fetch_data(sql)
        #dadost = open('dados.txt', 'w')
        for change_historias in lista_change:
            id_issue = change_historias[0]
            chave_historia = change_historias[1]
            
            link_changelog = f"{os.getenv('JIRA_ISSUE')}/{chave_historia}/changelog"
            lista_changelog = APIClient()
            changelog = lista_changelog.get(link_changelog)
            
            try:
                dados_changelog = changelog['values']
                contadaddos = len(dados_changelog)

                for i in range(0, contadaddos):
                    data_status = strutils.cut_data(dados_changelog[i]['created'])
                    id_changelog = dados_changelog[i]['id']

                    # Verifica se tem o changelog
                    sql = f"SELECT id_changelog from tb_changelog WHERE id_changelog = {id_changelog}"
                    checachange = abreconn.fetch_data_one(sql)
                    
                    try:
                        author = dados_changelog[i]['author']
                        email_author = author.get('emailAddress')
                        name_author = author.get('displayName')                        
                    except KeyError as k:
                        author = ''
                        email_author = ''
                        name_author = ''

                    conta_itens = len(dados_changelog[i]['items'])
                    for j in range(0, conta_itens):
                        campos_andamentos = dados_changelog[i]['items'][j]

                        verifica_field = campos_andamentos.get('field')
                        verifica_field_d = campos_andamentos.get('fieldId')                   
                        
                        if (verifica_field == 'status' and verifica_field_d == 'status'):
                            id_from = campos_andamentos.get('from')
                            from_status = campos_andamentos.get('fromString')
                            id_to = campos_andamentos.get('to')
                            to_status = campos_andamentos.get('toString')
                            contaj += 1                                           

                            if (checachange is None):
                                sql = f"INSERT INTO tb_changelog (id_changelog, id_issue, key, id_from_status, from_status, id_to_status, to_status, data_change, email_autor, name_auhor) VALUES ({id_changelog}, {id_issue}, '{chave_historia}', {id_from}, '{from_status}', {id_to}, '{to_status}', '{data_status}', '{email_author}', '{name_author}')"
                                print(f"Registro: {id_changelog} inserido na atividade {chave_historia} gravado com sucesso!\n ")
                            else:
                                sql = f"UPDATE tb_changelog SET id_from_status = {id_from}, from_status = '{from_status}', id_to_status = {id_to}, to_status = '{to_status}', data_change = '{data_status}', email_autor = '{email_author}', name_auhor = '{name_author}' WHERE id_changelog = {id_changelog}"
                                print(f"Registro: {id_changelog} atualizado na atividade {chave_historia} gravado com sucesso!\n ")
                            abreconn.grava_dados(sql)
                            
                            #dadost.write(f"{sql}\n\t")
            except TypeError as ty:
                type = 0
        
        abreconn.close()
                
                
    def load_last_day(self):
        conta = 0
        load_dotenv()
        abreconn = AccessDB()

        #Carregaas historias do dia anterior
        #verifica se o dia anterior domingo, se foi inclui as 3 datas
        #data_hoje = datetime.now()
        data_hoje = datetime.today()
        data_filtro = strutils.get_sat_sun(data_hoje)
        data_filtro = data_filtro.strftime("%Y-%m-%d")
        
        jql = f"project in ({os.getenv('JIRA_METRICS')}) AND created >= '2024-10-15' ORDER BY created DESC"
        dadost = open('dados.txt', 'w')
        lista_historias = APIClient()
        historias_anterior = lista_historias.get_search(jql)
        
        historias_anterior['total']

        dados_issue = historias_anterior['issues']
                
        for dados in dados_issue:
            campos =  ''
            values = ''

            # Inserindo o Id do jira
            id_issue = dados['id']
            campos += 'id_issue, '
            values += id_issue + ', '

            # Inserindo o id do projeto
            id_projeto = dados['fields']['project']['id']
            campos += 'id_projeto, '
            values += id_projeto + ', '

            # Inserindo a chade da historia
            key_hit = dados['key']
            campos += 'key, '
            values += f"'{key_hit}', "                

            #Insere os dados da chave da atividade Ex: DECS-4234
            try:
                if (dados['fields']['parent'] != 'null' or dados['fields']['parent'] is not None ):
                    try:
                        id_parent = dados['fields']['parent']['id']
                        key_parent = dados['fields']['parent']['key']
                        campos += 'id_parent, key_parent, '
                        values += f"{id_parent}, '{key_parent}',"
                        
                    except KeyError as ke:
                        id_p= ''
            except KeyError as ke:
                id_p= ''

                            
            # Inserindo os Status
            if (dados['fields']['status'] is not None or dados['fields']['status'] != 'null'):
                id_status = dados['fields']['status']['id']
                status = dados['fields']['status']['name']
                campos += 'id_status, '
                values += id_status + ', '
                campos += 'status, '
                values += f"'{status}', "
            else:
                id_status = ''
                status = '' 
            
            #Insere dados da sprint se existir
            try:
                if(dados['fields']['customfield_10007'] is not None or dados['fields']['customfield_10007'] != 'null'):
                    try:
                        for sprints in dados['fields']['customfield_10007']:
                            status = sprints['state']
                            if status == 'active':
                                id_sprint = sprints['id']
                                sprint = sprints['name']
                                sprint_dataini = strutils.cut_data(sprints['startDate'])
                                sprint_datater = strutils.cut_data(sprints['endDate'])

                                campos += 'id_sprint, sprint, sprint_dataini, sprint_datater, '
                                values += f"{id_sprint}, '{sprint}', '{sprint_dataini}', '{sprint_datater}', "
                                
                    except TypeError as ty:
                        retsprint= ''                       
            except KeyError as ke:
                retsprint= ''

            # Insere o tipo da atividade
            issuetype = dados['fields']['issuetype']['name']
            campos += 'issuetype, '
            values += f"'{issuetype}', "

            # Insere a criação da atividade
            created = strutils.cut_data(dados['fields']['created'])
            campos += 'created, '
            values += f"'{created}', "

            # Insere dados da Squad
            if dados['fields']['customfield_11779'] is not None:
                try:
                    squad = dados['fields']['customfield_11779'][0]
                    campos += 'squad, '
                    values += f"'{squad}', "
                except KeyError as ke:
                    squad = ''
            else:
                dsquad = ''  

            #Insere dados do Criador da atividade              
            if (dados['fields']['creator'] is not None or dados['fields']['creator'] != 'null'):                    
                try:
                    #link_creator = busca_historias.get(os.getenv('JIRA_SEARCH_USER') + dados['fields']['creator']['accountId'])
                    emailcreator = dados['fields']['creator']['emailAddress']
                    namecreator = dados['fields']['creator']['displayName']
                    campos += 'emailcreator, namecreator, '
                    values += f"'{emailcreator}', '{namecreator}', "
                    
                except KeyError:
                    criacao = ''

            #Insere se tem um responsável
            if (dados['fields']['assignee'] is None or dados['fields']['assignee'] == 'null'):
                assin = ''
            else:
                try:
                    emailassignee = dados['fields']['assignee']['emailAddress']
                    nameassignee = dados['fields']['assignee']['displayName']
                    campos += 'emailassignee, nameassignee, '
                    values += f"'{emailassignee}', '{nameassignee}', "                          
                except KeyError as ke:
                    assin = ''

            #Insere as categorias
            try:
                if (dados['fields']['labels'] is not None or dados['fields']['labels'] != 'null'):
                    categorias = ', '.join(dados['fields']['labels'])
                    if categorias is not None:
                        campos += 'categorias, '
                        values += f"'{categorias}', "
            except KeyError as k:
                categ = ''

            #insere o produto vinculado
            try:
                if(dados['fields']['customfield_11843'] is not None or dados['fields']['customfield_11843'] != 'null'):
                    try:
                        for produto in dados['fields']['customfield_11843']:
                            if produto is not None:
                                id_produto = produto['id']
                                produto = produto['value']
                                campos += 'id_produto, produto, '
                                values += f"'{id_produto}', '{produto}', "                                    
                    except TypeError as t:
                        prod = ''
            except KeyError as k:
                prod = ''
            
            # Insere Atualização
            updated = strutils.cut_data(dados['fields']['updated'])
            campos += 'updated '
            values += f"'{updated}'"

            # Cria o SQL áa Gravação
            sql = f"INSERT INTO tb_issue ({campos}) VALUES ({values})"
            abreconn.grava_dados(sql)
            print(f"Registro gravado: {key_hit} com sucesso!\n")
            self.load_chancelog(key_hit)
            #dadost.write(sql +"\r\t")
            #dadost.write(f"Contatem: {conta} - id : {id_issue}, id_projeto: {id_projeto}, id_parent: {id_parent}, id_status: {id_status}, id_print: {id_sprint}, id_produto: {id_produto}, Key: {key}, keyparent: {key_parent}, issuetype: {issuetype}, created: {created}, status: {status}, updated: {updated}, squad: {squad}, emailcreator: {emailcreator}, namecreator: {namecreator}, nameassignee: {emailassignee}, emailassignee: {emailassignee}, sprint: {sprint}, sprint_dataini: {sprint_dataini}, sprint_dater: {sprint_datater}, categorias: {categorias}, produto: {produto}\r\t")
            
            conta += 1

        abreconn.close()


    # Atualiza limit issues
    def update_all_limit_issues_project(self):
            conta = 0
            startat = 0
            qtd_registros = 100
            total_registros = 0
            qtd_restante = 0
            limite = 4000
            

            load_dotenv()
            abreconn = AccessDB()
            abre_projetos = ''
            limpa_projetos = ''
            sql = f"SELECT sigla FROM tb_projetos WHERE sigla in ({os.getenv('JIRA_METRICS')})"        
            
            lista_projetos = abreconn.fetch_data(sql)
            #dadost = open('dados.txt', 'w')
            for abreprojetos in lista_projetos:
                projeto = abreprojetos[0]
                
                #Busca as historias dos projetos
                busca_historias = APIClient()
                link_projetos = f"{os.getenv('JIRA_SEARCH_PROJECT')}{projeto}&startAt={startat}&maxResults={qtd_registros}"
                
                
                historias = busca_historias.get(link_projetos)
                
                total_registros = historias['total']
                
                while qtd_restante < limite:
                    if startat > 0:
                        link_projetos = f"{os.getenv('JIRA_SEARCH_PROJECT')}{projeto}&startAt={startat}&maxResults={qtd_registros}"
                        historias = busca_historias.get(link_projetos)
                    
                        dados_issue = historias['issues']
                        for dados in dados_issue:
                            campos =  ''
                            values = ''

                            # Inserindo o Id do jira
                            id_issue = dados['id']

                            sql = f"SELECT id_issue FROM tb_issue where id_issue = {id_issue}"
                            checanewissue = abreconn.fetch_data_one(sql)

                            # Senão retornou nenhum registro, insere um novo registro na tb_issou (nova key)
                            if (checanewissue is None or checanewissue == 'null'):
                                campos =  ''
                                values = ''

                                # Inserindo o Id do jira
                                id_issue = dados['id']
                                campos += 'id_issue, '
                                values += id_issue + ', '

                                # Inserindo o id do projeto
                                id_projeto = dados['fields']['project']['id']
                                campos += 'id_projeto, '
                                values += id_projeto + ', '

                                #Inserindo a sigla do projeto
                                campos += 'projeto, '
                                values += f"'{projeto}', "

                                # Inserindo a chade da historia
                                key_hit = dados['key']
                                campos += 'key, '
                                values += f"'{key_hit}', "                

                                #Insere os dados da chave da atividade Ex: DECS-4234
                                try:
                                    if (dados['fields']['parent'] != 'null' or dados['fields']['parent'] is not None ):
                                        try:
                                            id_parent = dados['fields']['parent']['id']
                                            key_parent = dados['fields']['parent']['key']
                                            campos += 'id_parent, key_parent, '
                                            values += f"{id_parent}, '{key_parent}',"
                                            
                                        except KeyError as ke:
                                            id_p= ''
                                except KeyError as ke:
                                    id_p= ''

                                                
                                # Inserindo os Status
                                if (dados['fields']['status'] is not None or dados['fields']['status'] != 'null'):
                                    id_status = dados['fields']['status']['id']
                                    status = dados['fields']['status']['name']
                                    campos += 'id_status, '
                                    values += id_status + ', '
                                    campos += 'status, '
                                    values += f"'{status}', "
                                else:
                                    id_status = ''
                                    status = '' 
                                
                                #Insere dados da sprint se existir
                                try:
                                    if(dados['fields']['customfield_10007'] is not None or dados['fields']['customfield_10007'] != 'null'):
                                        try:
                                            for sprints in dados['fields']['customfield_10007']:
                                                status = sprints['state']
                                                if status == 'active':
                                                    id_sprint = sprints['id']
                                                    sprint = sprints['name']
                                                    sprint_dataini = strutils.cut_data(sprints['startDate'])
                                                    sprint_datater = strutils.cut_data(sprints['endDate'])

                                                    campos += 'id_sprint, sprint, sprint_dataini, sprint_datater, '
                                                    values += f"{id_sprint}, '{sprint}', '{sprint_dataini}', '{sprint_datater}', "
                                                    
                                        except TypeError as ty:
                                            retsprint= ''                       
                                except KeyError as ke:
                                    retsprint= ''

                                # Insere o tipo da atividade
                                issuetype = dados['fields']['issuetype']['name']
                                campos += 'issuetype, '
                                values += f"'{issuetype}', "

                                # Insere a criação da atividade
                                created = strutils.cut_data(dados['fields']['created'])
                                campos += 'created, '
                                values += f"'{created}', "

                                # Insere dados da Squad
                                if dados['fields']['customfield_11779'] is not None:
                                    try:
                                        squad = dados['fields']['customfield_11779'][0]
                                        campos += 'squad, '
                                        values += f"'{squad}', "
                                    except KeyError as ke:
                                        squad = ''
                                else:
                                    dsquad = ''  

                                #Insere dados do Criador da atividade              
                                if (dados['fields']['creator'] is not None or dados['fields']['creator'] != 'null'):                    
                                    try:
                                        #link_creator = busca_historias.get(os.getenv('JIRA_SEARCH_USER') + dados['fields']['creator']['accountId'])
                                        emailcreator = dados['fields']['creator']['emailAddress']
                                        namecreator = dados['fields']['creator']['displayName']
                                        campos += 'emailcreator, namecreator, '
                                        values += f"'{emailcreator}', '{namecreator}', "
                                        
                                    except KeyError:
                                        criacao = ''

                                #Insere se tem um responsável
                                if (dados['fields']['assignee'] is None or dados['fields']['assignee'] == 'null'):
                                    assin = ''
                                else:
                                    try:
                                        emailassignee = dados['fields']['assignee']['emailAddress']
                                        nameassignee = dados['fields']['assignee']['displayName']
                                        campos += 'emailassignee, nameassignee, '
                                        values += f"'{emailassignee}', '{nameassignee}', "                          
                                    except KeyError as ke:
                                        assin = ''

                                #Insere as categorias
                                try:
                                    if (dados['fields']['labels'] is not None or dados['fields']['labels'] != 'null'):
                                        categorias = ', '.join(dados['fields']['labels'])
                                        if categorias is not None:
                                            campos += 'categorias, '
                                            values += f"'{categorias}', "
                                except KeyError as k:
                                    categ = ''

                                #insere o produto vinculado
                                try:
                                    if(dados['fields']['customfield_11843'] is not None or dados['fields']['customfield_11843'] != 'null'):
                                        try:
                                            produto = dados['fields']['customfield_11843'][0]
                                            if produto is not None:
                                                id_produto = produto['id']
                                                produto = produto['value']
                                                campos += 'id_produto, produto, '
                                                values += f"{id_produto}, '{produto}', "                              
                                        except TypeError as t:
                                            prod = ''
                                except KeyError as k:
                                    prod = ''

                                #insere o time de demanda                    
                                demanda = dados['fields']['customfield_12053']
                                checademanda = np.nonzero(demanda)                    
                                if checademanda[0] == 0:
                                    campos += 'tipo_demanda, '
                                    values += f"'{demanda.get('value')}', "

                                #Insere data de Resolução
                                resolutiondate = dados['fields']['resolutiondate']
                                checaresolution = np.nonzero(resolutiondate)
                                if checaresolution[0] == 0:
                                    campos += 'resolutiondate, '
                                    values += f"'{strutils.cut_data(resolutiondate)}', "                                
                                
                                # Insere Atualização
                                updated = strutils.cut_data(dados['fields']['updated'])
                                campos += 'updated '
                                values += f"'{updated}'"

                                # Cria o SQL áa Gravação
                                sql = f"INSERT INTO tb_issue ({campos}) VALUES ({values})"
                                abreconn.grava_dados(sql)
                                print(f"Novo Registro gravado: {key_hit} com sucesso!\n")
                                #dadost.write(sql +"\r\t")
                                #dadost.write(f"Contatem: {conta} - id : {id_issue}, id_projeto: {id_projeto}, id_parent: {id_parent}, id_status: {id_status}, id_print: {id_sprint}, id_produto: {id_produto}, Key: {key}, keyparent: {key_parent}, issuetype: {issuetype}, created: {created}, status: {status}, updated: {updated}, squad: {squad}, emailcreator: {emailcreator}, namecreator: {namecreator}, nameassignee: {emailassignee}, emailassignee: {emailassignee}, sprint: {sprint}, sprint_dataini: {sprint_dataini}, sprint_dater: {sprint_datater}, categorias: {categorias}, produto: {produto}\r\t")
                                self.load_one_changelog_key(id_issue, key_hit)                           

                            else:

                                # Atualiza os ultimos dados
                                key_hit = dados['key']
                                
                                #Insere os dados da chave da atividade Ex: DECS-4234
                                try:
                                    if (dados['fields']['parent'] != 'null' or dados['fields']['parent'] is not None ):
                                        try:
                                            id_parent = dados['fields']['parent']['id']
                                            key_parent = dados['fields']['parent']['key']
                                            values += f"id_parent = {id_parent}, key_parent = '{key_parent}',"
                                            
                                        except KeyError as ke:
                                            id_p= ''
                                except KeyError as ke:
                                    id_p= ''

                                                
                                # Inserindo os Status
                                if (dados['fields']['status'] is not None or dados['fields']['status'] != 'null'):
                                    id_status = dados['fields']['status']['id']
                                    status = dados['fields']['status']['name']
                                    values += f"id_status = {id_status}, status = '{status}', "
                                else:
                                    id_status = ''
                                    status = '' 
                                
                                #Insere dados da sprint se existir
                                try:
                                    if(dados['fields']['customfield_10007'] is not None or dados['fields']['customfield_10007'] != 'null'):
                                        try:
                                            for sprints in dados['fields']['customfield_10007']:
                                                status = sprints['state']
                                                if status == 'active':
                                                    id_sprint = sprints['id']
                                                    sprint = sprints['name']
                                                    sprint_dataini = strutils.cut_data(sprints['startDate'])
                                                    sprint_datater = strutils.cut_data(sprints['endDate'])

                                                    values += f"id_sprint = {id_sprint}, sprint = '{sprint}', sprint_dataini = '{sprint_dataini}', sprint_datater = '{sprint_datater}', "
                                                    
                                        except TypeError as ty:
                                            retsprint= ''                       
                                except KeyError as ke:
                                    retsprint= ''

                                # Insere o tipo da atividade
                                issuetype = dados['fields']['issuetype']['name']
                                values += f"issuetype = '{issuetype}', "

                                # Insere a criação da atividade
                                created = strutils.cut_data(dados['fields']['created'])
                                values += f"created = '{created}', "

                                # Insere dados da Squad
                                if dados['fields']['customfield_11779'] is not None:
                                    try:
                                        squad = dados['fields']['customfield_11779'][0]
                                        values += f"squad = '{squad}', "
                                    except KeyError as ke:
                                        squad = ''
                                else:
                                    dsquad = ''  

                                #Insere dados do Criador da atividade              
                                if (dados['fields']['creator'] is not None or dados['fields']['creator'] != 'null'):                    
                                    try:
                                        #link_creator = busca_historias.get(os.getenv('JIRA_SEARCH_USER') + dados['fields']['creator']['accountId'])
                                        emailcreator = dados['fields']['creator']['emailAddress']
                                        namecreator = dados['fields']['creator']['displayName']
                                        values += f"emailcreator = '{emailcreator}', namecreator = '{namecreator}', "
                                        
                                    except KeyError:
                                        criacao = ''

                                #Insere se tem um responsável
                                if (dados['fields']['assignee'] is None or dados['fields']['assignee'] == 'null'):
                                    assin = ''
                                else:
                                    try:
                                        emailassignee = dados['fields']['assignee']['emailAddress']
                                        nameassignee = dados['fields']['assignee']['displayName']
                                        values += f"emailassignee = '{emailassignee}', nameassignee = '{nameassignee}', "                          
                                    except KeyError as ke:
                                        assin = ''

                                #Insere as categorias
                                try:
                                    if (dados['fields']['labels'] is not None or dados['fields']['labels'] != 'null'):
                                        categorias = ', '.join(dados['fields']['labels'])
                                        if categorias is not None:
                                            values += f"categorias = '{categorias}', "
                                except KeyError as k:
                                    categ = ''

                                #insere o produto vinculado
                                try:
                                    if(dados['fields']['customfield_11843'] is not None or dados['fields']['customfield_11843'] != 'null'):                                    
                                        try:                                        
                                            produto = dados['fields']['customfield_11843'][0]
                                            if produto is not None:
                                                id_produto = produto['id']
                                                produto = produto['value']
                                                values += f"id_produto = {id_produto}, produto = '{produto}', "                                    
                                        except TypeError as t:
                                            prod = ''
                                except KeyError as k:
                                    prod = ''

                                #insere o time de demanda                    
                                demanda = dados['fields']['customfield_12053']
                                checademanda = np.nonzero(demanda)                    
                                if checademanda[0] == 0:
                                    values += f"tipo_demanda = '{demanda.get('value')}', "

                                resolutiondate = dados['fields']['resolutiondate']
                                checaresolution = np.nonzero(resolutiondate)
                                if checaresolution[0] == 0:
                                    values += f"resolutiondate = '{strutils.cut_data(resolutiondate)}', "
                            
                                
                                # Insere Atualização
                                updated = strutils.cut_data(dados['fields']['updated'])
                                values += f"updated = '{updated}'"

                                # Cria o SQL áa Gravação
                                sql = f"UPDATE tb_issue SET {values} WHERE id_issue = {id_issue}"
                                abreconn.grava_dados(sql)
                                print(f"Registro atualizado: {id_issue} - Key {key_hit} com sucesso!\n")
                                #dadost.write(sql +"\r\t")
                                #dadost.write(f"Contatem: {conta} - id : {id_issue}, id_projeto: {id_projeto}, id_parent: {id_parent}, id_status: {id_status}, id_print: {id_sprint}, id_produto: {id_produto}, Key: {key}, keyparent: {key_parent}, issuetype: {issuetype}, created: {created}, status: {status}, updated: {updated}, squad: {squad}, emailcreator: {emailcreator}, namecreator: {namecreator}, nameassignee: {emailassignee}, emailassignee: {emailassignee}, sprint: {sprint}, sprint_dataini: {sprint_dataini}, sprint_dater: {sprint_datater}, categorias: {categorias}, produto: {produto}\r\t")
                                self.load_one_changelog_key(id_issue, key_hit)
                    

                    ###### ------- Contador da paginação
                    startat += 100
                    qtd_restante += 100
                
                
                startat = 0
                qtd_registros = 100
                total_registros = 0
                qtd_restante = 0
            

            abreconn.close()
     
        

            
            


    