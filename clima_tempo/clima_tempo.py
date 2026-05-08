import requests
import pandas as pd
import os
from dotenv import load_dotenv
load_dotenv()
from sqlalchemy import create_engine




# Função para extrair os dados da API
def extract(cidades, api_key):
    resultados = []
    
    for cidade in cidades:
        url = f'https://api.openweathermap.org/data/2.5/weather?q={cidade}&units=metric&lang=pt_br&APPID={api_key}'
        response = requests.get(url)
        print(f'Cidade: {cidade}, Status Code: {response.status_code}')

        if response.status_code == 200:
           resultados.append(response.json())
        else:
            print(f'Aviso: falha ao buscar {cidade} - Status {response.status_code}')
    return resultados


def transform(dados_brutos):
    registros = []
    
    for dado in dados_brutos:
        registro = {
            'cidade': dado['name'],
            'temperatura': dado['main']['temp'],
            'sensacao_termica': dado['main']['feels_like'],
            'umidade': dado['main']['humidity'],
            'pressao': dado['main']['pressure'],
            'velo_vento': dado['wind']['speed'],
            'desc_clima': dado['weather'][0]['description'],
            'coletado_em': pd.Timestamp.now()
        }
        registros.append(registro)
    return pd.DataFrame(registros)

def load(df):
    database_url = os.getenv('database_url')
    
    engine = create_engine(database_url)
    
    df.to_sql(
        'clima_tempo',
        con=engine,
        if_exists='append',
        index=False
    )
    
    print(f'{len(df)} registros inseridos com sucesso!')



# Lista de Cidades para consulta
cidades = ['Vitoria', 'São Paulo', 'Rio de Janeiro', 'Belo Horizonte']
api_key = os.getenv('OPENWEATHER_API_KEY')

#Inicia o processo de extração dos dados

dados_brutos = extract(cidades, api_key)
print (f'Total de cidades coletadas: {len(dados_brutos)}')

# Transformação dos dados

DF = transform(dados_brutos)
print(DF)
print(DF.dtypes)

# Carregamento no Supabase
load(DF)
