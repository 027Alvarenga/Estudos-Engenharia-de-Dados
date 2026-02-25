# importando bibliotecas necessárias

import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
import sqlite3
from datetime import datetime


# Criando função para extrair os dados

def extrair(url, atributos_tabela):
    #Faz requisição na página web
    pagina = requests.get(url).text
   
    # Crio variável para armazenar a leitura da página
    dados = BeautifulSoup(pagina, 'html.parser')
    
    # Crio um dataframe utilizando as colunas criadas com a variável de atributos da tabela
    df = pd.DataFrame(columns=atributos_tabela)
    
    # Busca da tag de tbody
    tabela = dados.find_all('tbody')
    
    # Busca as linhas da tabela
    rows = tabela[2].find_all('tr')
    
    # Itera sobre as linhas
    for row in rows:
        col = row.find_all('td')
        if len(col) != 0:
            pib_text = col[2].get_text(strip=True)
            if col[0].find('a') is not None and pib_text not in ['—', '-', '']:
                dicionario = {
                    "Pais/Territorio": col[0].a.contents[0],
                    "PIB": pib_text
                }
                df1 = pd.DataFrame(dicionario, index=[0])   
                df = pd.concat([df, df1], ignore_index=True)
    
    return df

def transformar(df):
    # Substitui a vírgula por nada para tratar o formato americano (milhar)
    # Se o site usar vírgula como decimal, ajuste conforme necessário
    df["PIB"] = df["PIB"].str.replace(',', '', regex=True)
    
    # Converte para float, forçando erros a virarem NaN (Not a Number)
    df["PIB"] = pd.to_numeric(df["PIB"], errors='coerce')
    
    # Remove linhas que por ventura ficaram vazias (NaN)
    df = df.dropna(subset=["PIB"])
    
    # Realiza o cálculo e arredondamento
    df["PIB"] = np.round(df["PIB"] / 1000, 2)
    
    df = df.rename(columns={"PIB": "PIB_Bilhoes"})
    return df

def carregar_csv(df, csv_path):
    df.to_csv(csv_path)

def carregar_db(df, sql_connection, nome_tabela):
    df.to_sql(nome_tabela, sql_connection, if_exists='replace', index=False)
    
def query(query_statement, sql_connection):
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)
    
def log_processos(mensagem):
    timestamp_format = '%Y-%g-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open("etl_projeto_log.txt", "a") as f:
        f.write(timestamp + ': ' + mensagem + '\n')
        
        
url = 'https://web.archive.org/web/20230824184940/https://pt.wikipedia.org/wiki/Lista_de_pa%C3%ADses_por_PIB_nominal'

atributos_tabela = ["Pais/Territorio", "PIB" ]

nome_db = 'Economias_Mundo.db'

nome_tabela = 'Paises_PIB'

csv_path = 'Paises_PIB.csv'
# Chamadas de função


log_processos('Preliminares completas. Inicializando o ETL process')
df = extrair(url, atributos_tabela)
print(df.head())

log_processos("processo de extração completo, inicializando processo de transformacao")

df = transformar(df)

log_processos("Transformacao de dados completo. inicalizando o carregamento dos dados")

carregar_csv(df, csv_path)

log_processos("Dados salvos como CSV")

sql_connection = sqlite3.connect('Economias_Mundo.db')

log_processos('Conexao com o banco de daos iniciada')

carregar_db(df, sql_connection, nome_tabela)

log_processos('Dados carregados para banco de dados. Rode a query')

query_statement = f"SELECT * FROM {nome_tabela} WHERE PIB_Bilhoes >= 100"
query(query_statement, sql_connection)

log_processos("Processo concluido.")

sql_connection.close()

