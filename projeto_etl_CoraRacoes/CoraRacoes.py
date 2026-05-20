import requests
import logging
import os
from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_latest_folder(usuario, repo, token):
    url = f'https://api.github.com/repos/{usuario}/{repo}/contents/'
    
    headers = {
        'authorization': f'token {token}',
        'Accept': 'application/vnd.github.v3+json'
    }
    
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        itens = response.json()
        pastas = [item['name'] for item in itens if item['type'] == 'dir']
        return sorted(pastas, reverse=True)[0]
    else:
        logger.warning(f'Falha ao buscar dados para {usuario}/{repo} - Status {response.status_code}')

    return None

def extract(usuario, repo, token, pasta):
    dados = {}
    
    headers = {
        'authorization': f'token {token}',
        'Accept': 'application/vnd.github.v3+json'
    }
    
    url = f'https://api.github.com/repos/{usuario}/{repo}/contents/{pasta}'
    response = requests.get(url, headers=headers)  
     
    if response.status_code == 200:
        arquivos = response.json()
        for arquivo in arquivos:
            conteudo = requests.get(arquivo['download_url'], headers=headers)
            nome = arquivo['name'].replace('.json', '')
            dados[nome] = conteudo.json()
    else:
        logger.warning(f'Falha ao buscar dados para {usuario}/{repo}/{pasta} - Status {response.status_code}')
            
    return dados

def transform(dados):
    return dados

def run():
    try:
        token = os.getenv('GITHUB_TOKEN')
        usuario = os.getenv('GITHUB_USER')
        repo = os.getenv('GITHUB_REPO')
        
        pasta = get_latest_folder(usuario, repo, token)
        logger.info(f'Pasta mais recente: {pasta}')
        
        dados = extract(usuario, repo, token, pasta)
        logger.info(f'Dados extraídos: {dados.keys()}')
        
        dados_transformados = transform(dados)
        logger.info(f'Dados transformados: {dados_transformados.keys()}')
        
    except Exception as e:
        logger.error(f'Erro ao executar o processo: {e}')
        
   
        
        
if __name__ == '__main__':
    run()