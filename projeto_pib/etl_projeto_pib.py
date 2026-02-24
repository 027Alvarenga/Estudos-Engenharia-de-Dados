# importando bibliotecas necessárias

import pandas as pd
import numpy as np
import glob
import requests
from bs4 import BeautifulSoup
import sqlite3
from datetime import datetime


url = 'https://web.archive.org/web/20230824184940/https://pt.wikipedia.org/wiki/Lista_de_pa%C3%ADses_por_PIB_nominal'

atributos_tabela = ["Pais/Territorio", "PIB" ]

nome_db = 'Economias_Mundo.db'

nome_tabela = 'Paises_PIB'

csv_path = 'C:/Estudos Engenharia de Dados/projeto_pib/Paises_PIB.csv'

# Criando função para extrair os dados

def extrair(url, atributos_tabela):
    #Faz requisição na página web
    pagina = requests.get(url).text
   
    # Crio variável para armazenar a leitura da página
    dados = BeautifulSoup(pagina, 'html.parser')
    
    # Crio um dataframe utilizando as colunas criadas com a variável de atributos da tabela
    df = pd.DataFrame(columns={atributos_tabela })
    
    # Busca da tag de tbody
    tabela = dados.find_all('tbody')
    
    # Busca as linhas da tabela
    rows = tabela[2].find_all('tr')
    
    # Itera sobre as linhas
    for row in rows:
        col = row.find_all('td')
        if len(col) != 0:
            if col[0].find('a') is not None and '—' not in col[2]:
                dicionario = {
                    "Pais/Territorio": col[0].a.contents[0],
                    "PIB": col[2].contents[0]
                }
                df1 = pd.DataFrame(dicionario, index=[0])   
                df = pd.concat([df, df1], ignore_index=True)
                return df
        else:
            break
    

def transformar(df):
    lista_PIB = df["PIB"].tolist()
    lista_PIB = [float("".join(x.split(','))) for x in lista_PIB]
    lista_PIB = [np.round(x/1000, 2) for x in lista_PIB]
    df["PIB"] = lista_PIB
    df = df.rename(columns={"PIB": "PIB_Bilhoes"})
    return df

def carregar_csv(df, csv_path):
    df.to_csv(csv_path)
    
def carregar_db(df, sql_connection, nome_db):
    df.to_sql(nome_tabela, sql_connection, if_existas='replace', index=False)
    
def query(query_statement, sql_connection):
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)
    
def log_processos(mensagem):
    timestamp_format = '%Y-%g-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open("./projeto_pib/etl_projeto_log.txt") as f:
        f.write(timestamp + ': ' + mensagem + '\n')
        
# Chamadas de função
df = extrair(url, atributos_tabela)