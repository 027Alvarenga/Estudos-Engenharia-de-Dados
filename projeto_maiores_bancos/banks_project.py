import pandas as pd
import requests 
from bs4 import BeautifulSoup
from datetime import datetime
import numpy as np
import sqlite3


def extrair(url, atributos_tabela):
    # Adicionando um User-Agent para evitar bloqueios
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
    
    pagina = requests.get(url, headers=headers).text
    dados = BeautifulSoup(pagina, 'html.parser')
    df = pd.DataFrame(columns = atributos_tabela)
    tabela = dados.find_all('tbody')
    rows = tabela[2].find_all('tr')
    
    
    for row in rows:
        col = row.find_all('td')
        if len(col) != 0 :
            dicionario = {
                "nome": col[0].get_text(strip=True),
                "mc_usd_billion": col[2].get_text(strip=True)
            }
            df1 = pd.DataFrame(dicionario, index=[0])
            df = pd.concat([df, df1], ignore_index=True)
        
    return df
    
    

def transformar(df):
    exchange_rates = pd.read_csv('exchange_rate.csv')
    rates_dict = exchange_rates.set_index('Currency').to_dict()['Rate']
    gpb_rate = float(rates_dict['GBP'])
    eur_rate = float(rates_dict['EUR'])
    inr_rate = float(rates_dict['INR'])
    
    df['mc_usd_billion'] = df['mc_usd_billion'].astype(str).str.replace(',', '')
    df['mc_usd_billion'] = pd.to_numeric(df['mc_usd_billion'], errors='coerce')
    
    df['mc_gbp_billion'] = np.round(df['mc_usd_billion'] * gpb_rate, 2)
    df['mc_eur_billion'] = np.round(df['mc_usd_billion'] * eur_rate, 2)
    df['mc_inr_billion'] = np.round(df['mc_usd_billion'] * inr_rate, 2)
    return df
    
    
def carregar_csv(df, csv_path):
    df.to_csv(csv_path)
    
def carregar_sql(df, conexao_sql, nome_tabela):
    df.to_sql(nome_tabela, conexao_sql, if_exists='replace', index=False)
    
    
def query_statement(query, conexao_sql):
    print(query)
    query_output = pd.read_sql(query, conexao_sql)
    print(query_output)
    

def log(message):
    timestamp_format = '%Y-%g-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open('code_log.txt', 'a') as f:
        f.write(timestamp + ': ' + message + '\n')


log('Preliminares completas. Iniciando o processo ETL')


query1 = f'SELECT * FROM Largest_banks'
query2 = f'SELECT AVG(MC_GBP_Billion) FROM Largest_banks'
query3 = f'SELECT nome from Largest_banks LIMIT 5'


url = 'https://en.wikipedia.org/wiki/List_of_largest_banks'
atributos_tabela = ['nome', 'mc_usd_billion']


csv_path = 'Largest_banks_data.csv'

banco_bd = 'Banks.db'

tabela = 'Largest_Banks'


log('inicio de extracao')
df = extrair(url, atributos_tabela)
print(df.head())

log('fim de extracao')


log('inicio de transformacao')

transformar(df)
print(df.head())

log('Fim de transformacao')


log('Dados salvos no arquivo CSV')

carregar_csv(df, csv_path)

log('Conexao SQL iniciada')

sql_connection = sqlite3.connect('Banks.db')

log('Dados carregados no Banco de Dados como uma tabela, Executando consultas ')
carregar_sql(df, sql_connection, tabela)

log('Processo Completo')
query_statement(query1, sql_connection)
query_statement(query2, sql_connection)
query_statement(query3, sql_connection)

log('Conexao com o servidor fechada')
sql_connection.close()