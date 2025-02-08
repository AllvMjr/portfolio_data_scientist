from arquivos.access_db import AccessDB
from arquivos.api_client import APIClient
from arquivos.strutils import strutils
from datetime import datetime, timedelta
import numpy as np
from dotenv import load_dotenv

class LoadProgress:

    def set_progress(self):
        load_dotenv()
        abreconn = AccessDB()

        #Varre as issues da tabela issue
        sql = f"SELECT id_issue, key FROM tb_issue"
        consulta_issue = abreconn.fetch_data(sql)

        for dados_issue in consulta_issue:
            
            # Selecionando os changelogs
            sql = f"SELECT id_from_status, from_status, id_to_status, to_status, data_change"