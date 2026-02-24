import pandas as pd
import requests 
from bs4 import BeautifulSoup
import sqlite3

db_name = 'ps2_to_ps3.db'
table_name = 'ps2_to_ps3'
csv_path = 'C:/Python/ps2_to_ps3.csv'

url = 'https://web.archive.org/web/20230727024331/https://en.everybodywiki.com/List_of_PlayStation_2_games_compatible_with_PlayStation_3'
count = 0
df = pd.DataFrame(columns=["Title", "Publisher"])
html_page = requests.get(url).text
dados = BeautifulSoup(html_page, "html.parser")

tables = dados.find_all('tbody')
rows = tables[2].find_all('tr')


for row in rows:
    if count < len(rows):
        col = row.find_all('td')
        if len(col)!= 0:
            dict = {
                "Title": col[0].get_text(strip=True),
                "Publisher": col[1].get_text(strip=True)
            }
            df1 = pd.DataFrame(dict, index=[0])
            df = pd.concat([df, df1], ignore_index=True)
            count+=1
    else:
        break
    
    
query_statement = "SELECT * FROM ps2_to_ps3"
df = df.rename(columns={"Title": "Titulo", "Publisher": "Editora"})
print(df)

df.to_csv(csv_path, index=False)
conn = sqlite3.connect(db_name)
df.to_sql(table_name, conn, if_exists='replace', index=False)

df = pd.read_sql(query_statement, conn)
print(query_statement)
conn.close()

