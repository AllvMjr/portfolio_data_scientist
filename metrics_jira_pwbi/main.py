from arquivos.access_db import AccessDB
from arquivos.api_client import APIClient
from load_projects import LoadProject
from load_progress import LoadProgress
from load_cubo import LoadCubo
from load_gold import Loadgold
from dotenv import load_dotenv
import os
import json

load_dotenv()

# ---- Cria a silver das atividades

carrega_issues = LoadProject()
carrega_issues.update_all_limit_issues_project()
#carrega_issues.load_one_changelog_key(378176, 'DECS-16998')

atualiza_cubo = LoadCubo()
atualiza_cubo.update_cubo()

#---------- Progresso
#carrega_progresso = LoadProgress()
#carrega_progresso.set_progress()


#-----------------------------
#carrege_atualiza = LoadProject()
#carrege_atualiza.update_all_limit_issues_project()

#carrega_issues = LoadProject()
#carrega_issues.load_chancelog()

#carrega_cubo = LoadCubo()
#carrega_cubo.load_cubo()

# PArte de criação das Golds

# 1 criar a gold da iteração. ordenando de forma correta
#create_iteration_gold = Loadgold()
#create_iteration_gold.create_iteration_gold() # Cria gold da iteração, ordenado
#create_iteration_gold.create_biiteration_gold() # Cria gold da Quinzena, ordenado
#create_iteration_gold.add_iteration_gold()
#create_iteration_gold.add_biinteration_gold()