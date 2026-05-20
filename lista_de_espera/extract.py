import requests 
from config import BASE_URL, CONSULTAS
import logging

logger = logging.getLogger(__name__)

def extract(consulta):
    
    params = {
        'unidade_id': consulta['unidade_id'],
        'ano_de_escolaridade_id': consulta['ano_de_escolaridade_id']
    }
    
    headers = {
    'Accept': 'application/json',
    'Referer': 'https://linhares.sisp.com.br/lista-publica',
    'X-Requested-With': 'XMLHttpRequest',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    }
    
    try:
        response = requests.get(BASE_URL, params=params, headers=headers, timeout=10)
        
        if response.status_code == 200:
            dados = response.json()
            inscricoes = dados.get('inscricoes', [])
            
            resultado = {
               'unidade_id': consulta['unidade_id'],
                'nome_unidade': consulta['nome_unidade'],
                'ano_de_escolaridade_id': consulta['ano_de_escolaridade_id'],
                'nome_ano': consulta['nome_ano'],
                'total_espera': len(inscricoes)
            }
            
            logger.info(f"Consulta Realizada com sucesso: {consulta['nome_unidade']} /"
                f"{consulta['nome_ano']}- Status {response.status_code}" )
            return resultado
        else: 
            logger.warning(
                f"Falha na consulta: {consulta['nome_unidade']} /"
                f"{consulta['nome_ano']}- Status {response.status_code}" 
            )
            return None
        
        
    except Exception as e:
        logger.error(f"Erro ao consultar API: {e}")
        return None
        