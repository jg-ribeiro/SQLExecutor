import locale
import os
import sys
import json
import re

locale.setlocale(locale.LC_TIME, 'pt_br')

"""
##----------------------------------------
Path aux functions
##----------------------------------------
"""


def resource_path(relative_path):
    if hasattr(sys, '_MEIPASS'):
        return os.path.join(sys._MEIPASS, relative_path)
    return os.path.join(os.path.abspath("."), relative_path)


"""
##----------------------------------------
Json files aux function
##----------------------------------------
"""


def open_json() -> dict:
    base_content = """{
  "oracle_database": {
    "TSN": "",
    "INSTANT_CLIENT": "",
    "user_name": "",
    "user_pass": "",
  },
  "postgres":{
    "hostname": "localhost",
    "port": "5432",
    "database": "",
    "username": "",
    "password": ""
  }
}"""

    try:
        with open('datafile.json', 'r', encoding='utf-8') as jsonfile:
            datafile = jsonfile.read()
    except FileNotFoundError:
        print('Arquivo não encontrado!')
        with open('datafile.json', 'w', encoding='utf-8') as jsonfile:
            jsonfile.writelines(base_content)
        exit()

    return json.loads(datafile)


"""
##----------------------------------------
SQL check functions
##----------------------------------------
"""

def is_select_query(sql):
    """
    Verifica se a consulta SQL é apenas para leitura (DQL).
    """
    # Remove comentários e normaliza espaços em branco
    normalized_sql = re.sub(r'--.*?\n|/\*.*?\*/', '', sql, flags=re.DOTALL)
    normalized_sql = re.sub(r'\s+', ' ', normalized_sql).strip().upper()
    
    # Verifica se a consulta começa com palavras-chave de leitura
    allowed_patterns = [
        r'^SELECT\s+',
        r'^WITH\s+',
        r'^SHOW\s+',
        r'^DESCRIBE\s+',
        r'^EXPLAIN\s+'
    ]
    
    # Verifica se a consulta é apenas de leitura
    is_read_only = any(re.match(pattern, normalized_sql) for pattern in allowed_patterns)
    
    # Verifica se não contém palavras-chave de modificação de dados
    forbidden_patterns = [
        r'\s+INSERT\s+',
        r'\s+UPDATE\s+', 
        r'\s+DELETE\s+',
        r'\s+DROP\s+',
        r'\s+CREATE\s+',
        r'\s+ALTER\s+',
        r'\s+TRUNCATE\s+',
        r'\s+GRANT\s+',
        r'\s+REVOKE\s+',
        r'\s+MERGE\s+',
        r'^INSERT\s+', 
        r'^UPDATE\s+', 
        r'^DELETE\s+',
        r'^DROP\s+',
        r'^CREATE\s+',
        r'^ALTER\s+',
        r'^TRUNCATE\s+',
        r'^GRANT\s+',
        r'^REVOKE\s+',
        r'^MERGE\s+'
    ]
    
    contains_forbidden = any(re.search(pattern, normalized_sql) for pattern in forbidden_patterns)
    
    return is_read_only and not contains_forbidden


if __name__ == '__main__':
    open_json()
