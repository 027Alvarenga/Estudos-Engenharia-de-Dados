import pandas as pd
import requests 
from bs4 import BeautifulSoup
from datetime import datetime
import numpy as np
import sqlite3


def extract(url, table_attribs):
    # Adicionando um User-Agent para evitar bloqueios
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
    
    page = requests.get(url, headers=headers).text
    data = BeautifulSoup(page, 'html.parser')
    df = pd.DataFrame(columns = table_attribs)
    table = data.find_all('tbody')
    rows = table[2].find_all('tr')
    
    
    for row in rows:
        col = row.find_all('td')
        if len(col) != 0 :
            dicionario = {
                "Name": col[0].get_text(strip=True),
                "MC_USD_BILLION": col[2].get_text(strip=True)
            }
            df1 = pd.DataFrame(dicionario, index=[0])
            df = pd.concat([df, df1], ignore_index=True)
        
    return df
    
    

def transform(df):
    exchange_rates = pd.read_csv('exchange_rate.csv')
    rates_dict = exchange_rates.set_index('Currency').to_dict()['Rate']
    gbp_rate = float(rates_dict['GBP'])
    eur_rate = float(rates_dict['EUR'])
    inr_rate = float(rates_dict['INR'])
    
    df['MC_USD_BILLION'] = df['MC_USD_BILLION'].astype(str).str.replace(',', '')
    df['MC_USD_BILLION'] = pd.to_numeric(df['MC_USD_BILLION'], errors='coerce')
    
    df['MC_GBP_BILLION'] = [np.round(x * gbp_rate, 2) for x in df['MC_USD_BILLION']]
    df['MC_EUR_BILLION'] = [np.round(x * eur_rate, 2) for x in df['MC_USD_BILLION']]
    df['MC_INR_BILLION'] = [np.round(x * inr_rate, 2) for x in df['MC_USD_BILLION']]
    return df
    
    
def load_csv(df, csv_path):
    df.to_csv(csv_path)
    
def load_sql(df, conn, table_name):
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    
    
def query_statement(query, conn):
    print(query)
    query_output = pd.read_sql(query, conn)
    print(query_output)
    

def log_process(message):
    timestamp_format = '%Y-%g-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open('code_log.txt', 'a') as f:
        f.write(timestamp + ': ' + message + '\n')


log_process('Preliminaries complete. Initiating ETL process')


query1 = f'SELECT * FROM Largest_banks'
query2 = f'SELECT AVG(MC_GBP_Billion) FROM Largest_banks'
query3 = f'SELECT Name from Largest_banks LIMIT 5'


url = 'https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ['Name', 'MC_USD_BILLION']


csv_path = 'Largest_banks_data.csv'

banco_bd = 'Banks.db'

table = 'Largest_Banks'


df = extract(url, table_attribs)
print(df)

log_process('Data extraction complete. Initiating Transformation process')

transform(df)
print(df)

log_process('Data transformation complete. Initiating Loading process')

load_csv(df, csv_path)

log_process('Data saved to CSV file')

log_process('SQL Connection initiated')

sql_connection = sqlite3.connect('Banks.db')


load_sql(df, sql_connection, table)
log_process('	Data loaded to Database as a table, Executing queries')

query_statement(query1, sql_connection)
query_statement(query2, sql_connection)
query_statement(query3, sql_connection)
log_process('	Process Complete')


sql_connection.close()
log_process('Server Connection closed')