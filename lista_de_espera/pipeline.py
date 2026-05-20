from extract import extract
from transform import transform
from load import load
from config import CONSULTAS
import logging


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


def run():
    resultados = []
    
    
    logging.info(f"Início do processo ETL - Extração dos Dados")
    for consulta in CONSULTAS:
        resultado = extract(consulta)
        resultados.append(resultado)
    logging.info(f"Fim da extração")
   
   
    logging.info(f"Início da Transformação dos Dados")
    df = transform(resultados)
    logging.info(f"Fim da Transformação")
    
    logging.info(f" Início do Carregamento dos Dados")
    load(df)
    logging.info(f"Fim do Carregamento")
   


if __name__ == '__main__':
    # O processo completo está demorando 18 minutos e 57 segundos.
    run()