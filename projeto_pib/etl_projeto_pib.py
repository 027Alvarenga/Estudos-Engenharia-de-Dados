# importando bibliotecas necessárias

import pandas as pd
import glob
import requests
from bs4 import BeautifulSoup
import sqlite3


url = 'https://web.archive.org/web/20230824184940/https://pt.wikipedia.org/wiki/Lista_de_pa%C3%ADses_por_PIB_nominal'

atributos_tabela = ["Pais/Territorio", "PIB" ]

nome_db = 'Economias_Mundo.db'

nome_tabela = 'Paises_PIB'

csv_path = 'C:/Estudos Engenharia de Dados/projeto_pib/Paises_PIB.csv'

# Criando função para extrair os dados

def extrair(url, atributos_tabela):
    pagina = requests.get(url).text
    dados = BeautifulSoup(pagina, 'html.parser')
    df = pd.DataFrame(columns={atributos_tabela })
    tabela = dados.find_all('tbody')
    rows = tabela[2].find_all('tr')
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
    

            