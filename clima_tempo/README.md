# 🌦️ Pipeline ETL — Dados Meteorológicos do Sudeste Brasileiro
 
Pipeline de dados automatizado que coleta, transforma e armazena informações meteorológicas em tempo real de quatro capitais do Sudeste brasileiro, executando ciclos de coleta a cada 20 minutos.
 
---
 
## 🏗️ Arquitetura
 
```
OpenWeatherMap API
        │
        ▼
   [ EXTRACT ]
   Requisições HTTP para cada cidade
   Tratamento de falhas por cidade
        │
        ▼
  [ TRANSFORM ]
   JSON bruto → DataFrame pandas
   Normalização e tipagem dos campos
        │
        ▼
    [ LOAD ]
   DataFrame → PostgreSQL (Supabase)
   Inserção incremental com append
        │
        ▼
  [ SCHEDULER ]
   Execução automática a cada 20 minutos
```
 
**Cidades monitoradas:** Vitória, São Paulo, Rio de Janeiro, Belo Horizonte
 
---
 
## 🛠️ Tecnologias Utilizadas
 
| Tecnologia | Finalidade |
|---|---|
| Python 3.13 | Linguagem principal |
| Requests | Consumo da API REST |
| Pandas | Transformação dos dados |
| SQLAlchemy | Conexão com o banco de dados |
| PostgreSQL (Supabase) | Armazenamento dos dados |
| Schedule | Agendamento do pipeline |
| Python-dotenv | Gerenciamento de variáveis de ambiente |
 
---
 
## 📦 Estrutura do Projeto
 
```
clima_tempo/
├── clima_tempo.py   # Pipeline ETL completo
├── .env             # Variáveis de ambiente (não versionado)
├── .gitignore       # Arquivos ignorados pelo Git
└── README.md        # Documentação do projeto
```
 
---
 
## ⚙️ Como Executar
 
### Pré-requisitos
- Python 3.10+
- Conta na [OpenWeatherMap](https://openweathermap.org/) para obter a API Key
- Conta no [Supabase](https://supabase.com/) com banco PostgreSQL configurado
### 1. Clone o repositório
```bash
git clone https://github.com/seu-usuario/clima-tempo.git
cd clima-tempo
```
 
### 2. Instale as dependências
```bash
pip install requests pandas sqlalchemy psycopg2-binary schedule python-dotenv
```
 
### 3. Configure as variáveis de ambiente
Crie um arquivo `.env` na raiz do projeto:
```
OPENWEATHER_API_KEY=sua_chave_aqui
DATABASE_URL=postgresql://postgres:senha@host:5432/postgres
```
 
### 4. Crie a tabela no Supabase
Execute no SQL Editor do Supabase:
```sql
CREATE TABLE clima_tempo (
    id               SERIAL PRIMARY KEY,
    cidade           VARCHAR(100),
    temperatura      FLOAT,
    sensacao_termica FLOAT,
    umidade          INT,
    pressao          INT,
    velo_vento       FLOAT,
    desc_clima       VARCHAR(200),
    coletado_em      TIMESTAMP,
    CONSTRAINT uq_cidade_coletado UNIQUE (cidade, coletado_em)
);
```
 
### 5. Execute o pipeline
```bash
python clima_tempo.py
```
 
O pipeline irá:
- Executar imediatamente na primeira vez
- Repetir automaticamente a cada 20 minutos
- Registrar logs com timestamp em cada etapa
---
 
## 📋 Exemplo de Log
 
```
2026-05-08 21:52:10 - INFO - Cidade: Vitória, Status Code: 200
2026-05-08 21:52:11 - INFO - Cidade: São Paulo, Status Code: 200
2026-05-08 21:52:12 - INFO - Cidade: Rio de Janeiro, Status Code: 200
2026-05-08 21:52:13 - INFO - Cidade: Belo Horizonte, Status Code: 200
2026-05-08 21:52:15 - INFO - Total de cidades coletadas: 4
2026-05-08 21:52:15 - INFO - Dados carregados com sucesso no Supabase
2026-05-08 21:52:15 - INFO - Processo ETL concluído com sucesso
```
 
---
 
## 🚀 Próximos Passos
 
- [ ] Retry automático em caso de falha na API
- [ ] Alertas por e-mail em caso de erro crítico
- [ ] Persistência dos JSONs brutos para auditoria
- [ ] Containerização com Docker
- [ ] Orquestração com Apache Airflow
---
 
## 👨‍💻 Autor
 
Desenvolvido como projeto prático de Engenharia de Dados.