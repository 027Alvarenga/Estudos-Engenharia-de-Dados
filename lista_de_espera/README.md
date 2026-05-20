# Pipeline ETL - Lista de Espera Pública - Linhares

Pipeline automatizado que coleta e armazena as informações sobre a lista de espera pública do município de Linhares. Separando por unidade e ano de escolaridade quantos estudantes estão aguardando na lista.

## 🏗️ Arquitetura

```
Lista de Espera Pública
        │
        ▼
   [ EXTRACT ]
   Requisições HTTP para cada unidade e ano de escolaridade
   Tratamento de falhas por unidade e ano de escolaridade
        │
        ▼
  [ TRANSFORM ]
   JSON bruto → DataFrame pandas
   Limpeza, Normalização e tipagem dos campos
        │
        ▼
    [ LOAD ]
   DataFrame → Arquivo .CSV local
   Inserção incremental com append
```

**Unidades Consultadas:** Todas as unidades que ofertam lista de espera on-line

---
 
## 🛠️ Tecnologias Utilizadas
 
| Tecnologia | Finalidade |
|---|---|
| Python 3.13 | Linguagem principal |
| Requests | Consumo dos dados via requisição HTTP |
| Pandas | Transformação dos dados |
| Logging | Logs para execução de cada etapa no processo de ETL |
| CSV | Carregamento dos dados em um formato de planilhas |
 
---

