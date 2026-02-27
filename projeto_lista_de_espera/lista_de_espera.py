import requests
from bs4 import BeautifulSoup
import pandas as pd
import datetime



def extrair(url, atributos_tabela):
    pagina = requests.get(url).text
    dados = BeautifulSoup(pagina, "html.parser")
    df = pd.DataFrame(columns=atributos_tabela)
    
    tabela = dados.find_all('tbody')
    linhas = tabela[0].find_all('tr')
    
    legenda_criterios ={
    "Mandado Judicial": "MJ",
    "Excepcionalidade": "EX",
    "Irmão na Mesma Escola": "IE",
    "Morar no Bairro da Escola": "MB",
    "Residir na Região Atendida": "RA",
    "Inscrição por Transferência": "TR",
    "Estudante da Rede Municipal": "RM"
    }
    
    for linha in linhas:
        col = linha.find_all('td')
        if len(col) != 0:
            icones = col[9].find_all(['span', 'i', 'img'])
            
            lista_criterios = [i['data-original-title'] for i in icones if i.has_attr('data-original-title')]
            
            
            criterios_texto = ", ".join(lista_criterios)
            
            dicionario = {
                "estudante": col[3].get_text(strip=True),
                "dt_inscricao": col[6].get_text(strip=True),
                "situacao": col[7].get_text(strip=True),
                "criterios_atendidos": criterios_texto
            }
            df1 = pd.DataFrame(dicionario, index=[1])
            df = pd.concat([df, df1], ignore_index=True)
    return df

def transformar(df):
    #remove espaços em branco extras no inicio e fim das strings
    df['estudante'] = df['estudante'].str.strip().str.upper()
    
    # Converte a coluna para o formato datetime do Python
    df['dt_inscricao'] = pd.to_datetime(df["dt_inscricao"], dayfirst=True)
    
    hoje = pd.Timestamp.now()
    df["dias_na_espera"] = (hoje - df["dt_inscricao"]).dt.days
    
    return df

def carregar_dados_csv(df, csv_path):
    df.to_csv(csv_path)
    
def carregar_db(df, sql_connection, nome_tabela):
    df.to_sql(nome_tabela, sql_connection, if_exists='replace', index=False)

url = 'https://linhares.sisp.com.br/lista-publica/buscar-inscricoes?unidade_id=128&ano_de_escolaridade_id=116&turno_obrigatorio='

atributos_tabela = ["estudante","dt_inscricao","situacao", "criterios_atendidos"]

csv_path = 'lista_de_espera.csv'


df = extrair(url, atributos_tabela)
print(df)

df = transformar(df)
print(df)

df = carregar_dados_csv(df, csv_path)
