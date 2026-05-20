import os
from datetime import datetime
import logging
from config import OUTPUT_DIR

logger = logging.getLogger(__name__)

def load(df):
    
    try:
        if df.empty:
            logger.warning('Dataframe vazio, nada para salvar.')
            return None
        
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        nome_arquivo = f"lista_espera_{datetime.now().strftime('%Y-%m-%d')}.csv"
        caminho = os.path.join(OUTPUT_DIR, nome_arquivo)
        df.to_csv(caminho, index=False, encoding='utf-8-sig')
        logger.info(f'Arquivo salvo em: {caminho}')
    except Exception as e:
        logger.warning(f"Erro ao tentar salvar arquivo: {e}")
   
   