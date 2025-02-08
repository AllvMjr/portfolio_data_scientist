import requests
from requests.auth import HTTPBasicAuth
from dotenv import load_dotenv
import os
import json

class APIClient:
    def __init__(self):
        load_dotenv()  # Carrega as variáveis do arquivo .env
        self.base_url = os.getenv('JIRA_URL')
        self.auth = HTTPBasicAuth(os.getenv('JIRA_USER'), os.getenv('JIRA_TOKEN'))

    def get(self, endpoint):
        url = f"{self.base_url}/{endpoint}"
        headers = {
            "Accept": "application/json"
        }
        try:
            response = requests.get(url, headers=headers, auth=self.auth)
            response.raise_for_status()  # Levanta um erro para códigos de status HTTP 4xx/5xx
            return response.json()  # Retorna a resposta em formato JSON
        except requests.exceptions.HTTPError as http_err:
            print(f"HTTP error occurred: {http_err}")
        except Exception as err:
            print(f"Other error occurred: {err}")

    def get_search(self, jql):
        load_dotenv()
        url = f"{self.base_url}/{os.getenv('JIRA_SEARCH')}"
        headers = {
            "Accept": "application/json"
        }
        params = {
            'jql' : jql,
            'startAt': 0,
            'maxResults': 100
        }
        try:
            response = requests.get(url, headers=headers, auth=self.auth, params=params)
            response.raise_for_status()  # Levanta um erro para códigos de status HTTP 4xx/5xx
            return response.json()  # Retorna a resposta em formato JSON
        except requests.exceptions.HTTPError as http_err:
            print(f"HTTP error occurred: {http_err}")
        except Exception as err:
            print(f"Other error occurred: {err}")
