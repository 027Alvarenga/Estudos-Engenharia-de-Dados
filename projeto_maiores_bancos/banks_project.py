import pandas as pd
import requests 
from bs4 import BeautifulSoup
from datetime import datetime


def extrair(url, atributos_tabela):
    # Adicionando um User-Agent para evitar bloqueios
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
    
    pagina = requests.get(url, headers=headers).text
    dados = BeautifulSoup(pagina, 'html.parser')
    df = pd.DataFrame(columns = atributos_tabela)
    tabela = dados.find_all('tbody')
    # if len(tabela) <= 4:
    #     raise ValueError(f"A página retornou apenas {len(tabela)} tbody(s); não é possível acessar o índice 3")
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
    
    #continuar função
    

def transformar(df):
    df['mc_gbp_billion'] = float(df['mc_usd_billion'] * 0.75)
    #Implementar codigo do transformar
    return df
    
    
def carregar_csv(df, csv_path):
    df.to_csv(csv_path)
    
def carregar_sql(df, conexao_sql, nome_tabela):
    df.to_sql( conexao_sql, nome_tabela, if_exists='replace', index=False)
    
    
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


query1 = f'SELECT Nome, MC_GBP_BILLION FROM LONDRES'
query2 = f'SELECT Nome, MC_GBP_BILLION FROM BERLIM'
query3 = f'SELECT Nome, MC_GBP_BILLION FROM NOVA_DÉLHI'


url = 'https://en.wikipedia.org/wiki/List_of_largest_banks'
atributos_tabela = ['nome', 'mc_usd_billion']



log('Inicio do Processo de ETL')

log('inicio de extracao')
df = extrair(url, atributos_tabela)
print(df.head())

log('fim de extracao')


log('inicio de transformacao')

transformar(df)
print(df.head())

log('Fim de transformacao')


log('inicio de carregamento')


log('fim de carregamento')
