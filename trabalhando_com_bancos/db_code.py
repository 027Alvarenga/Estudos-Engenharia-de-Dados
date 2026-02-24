import sqlite3
import pandas as pd

conn = sqlite3.connect('staff.db')

# tabela 1
nome_tabela = 'INSTRUCTOR'
lista_atributos = ['ID', 'NOME', 'SOBRENOME', 'CIDADE', 'UF']

caminho_csv = 'INSTRUCTOR.csv'
df = pd.read_csv(caminho_csv, names=lista_atributos)

df.to_sql(nome_tabela, conn, if_exists='replace', index=False)
print('tabela pronta')

# tabela 2
departamentos = 'Departaments'
atributos_departamentos = ['ID_DEP', 'NOME_DEP', 'ID_GERENTE', 'LOC_ID']

csv_departamentos = 'Departments.csv'
df2 = pd.read_csv(csv_departamentos, names=atributos_departamentos)

df2.to_sql(departamentos, conn, if_exists='replace', index=False)
print('tabela pronta')

query_statement = f"SELECT * FROM {nome_tabela}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)


query_statement = f"SELECT NOME FROM {nome_tabela}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)


query_statement = f"SELECT count(*) FROM {nome_tabela}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)


data_dict = {'ID': [100],
             'NOME': ['John'],
             'SOBRENOME': ['Doe'],
             'CIDADE': ['Paris'],
             'UF': ['FR']}
data_append = pd.DataFrame(data_dict)

data_append.to_sql(nome_tabela, conn, if_exists='append', index=False)
print('Dados de um funcionário inseridos')


dicionario_departamentos = {
    'ID_DEP': [8],
    'NOME_DEP': ['Garantia de Qualidade'],
    'ID_GERENTE': [30010],
    'LOC_ID': ['l0010']
}

departamento_append = pd.DataFrame(dicionario_departamentos)
departamento_append.to_sql(
    departamentos, conn, if_exists='append', index=False)
print('Dados de um departamento inseridos')

query_statement = f"SELECT COUNT(*) FROM {departamentos}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

conn.close()
