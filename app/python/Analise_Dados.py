# Bibliotecas
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
from pathlib import Path

# Caminho base do projeto
base_dir = Path(__file__).resolve().parent.parent.parent

# Conectando ao servidor
load_dotenv(base_dir/'conexao.env')

usuario = os.getenv('usuario')
senha = os.getenv('senha')
host = os.getenv('host')
database = os.getenv('banco_dados')
porta = os.getenv('port')

## URL de conexão
database_url = f'postgresql+psycopg2://{usuario}:{senha}@{host}:{porta}/{database}'

## Engine
engine = create_engine(database_url)

## Conectando
engine.connect()

def df_sql(sql:str, database_url:str) -> pd.DataFrame:
    """
    Obj: Essa função tem o objetivo de conectar ao banco e ler uma query específica
    Args:
    sql: nome da query esperada para ser lida
    database_url: url de conexão ao banco de dados
    """

    # Pasta de localização
    base_dir = Path(__file__).resolve().parent.parent.parent

    # Engine de conexão
    engine = create_engine(database_url)

    # leitor de arquivo sql
    with open(base_dir/'app/sql/{}.sql'.format(sql)) as select:
        query = select.read()

    # trazendo as informações do banco
    with engine.connect() as con:
        df = pd.read_sql_query(query, con)

    return df

df_ordens = df_sql('ordens', database_url)
df_fornecedores = df_sql('fornecedores', database_url)
df_categorias = df_sql('categorias', database_url)
df_funcionarios = df_sql('funcionarios', database_url)
df_order_details = df_sql('order_details', database_url)
df_produtos = df_sql('produtos', database_url)
df_territorios = df_sql('territorio', database_url)
df_regiao = df_sql('região', database_url)
df_expedidor = df_sql('expedidor', database_url)

df_expedidor.head()