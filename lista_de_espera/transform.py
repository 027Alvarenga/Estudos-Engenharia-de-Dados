import pandas as pd
from datetime import datetime
from config import ORDEM_ANO
import logging

logger = logging.getLogger(__name__)

def transform(resultados):
    
    validados = [r for r in resultados if r is not None]
    
    com_espera = [r for r in validados if r['total_espera'] > 0]
    
    df = pd.DataFrame(com_espera)

    df['data_coleta'] = datetime.now()    
    
    df['ordem_ano'] = df['ano_de_escolaridade_id'].map(ORDEM_ANO)
    df = df.sort_values(['nome_unidade', 'ordem_ano'] )
    df = df.drop(columns=['ano_de_escolaridade_id','ordem_ano'])
    logger.info(f'{len(df)} combinações com lista de espera encontradas')
    
    return df