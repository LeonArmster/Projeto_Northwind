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

## URL de conexão
database_url = f'postgresql+psycopg2://{usuario}:{senha}@{host}:{porta}/{database}'


df_ordens = df_sql('ordens', database_url)
df_fornecedores = df_sql('fornecedores', database_url)
df_categorias = df_sql('categorias', database_url)
df_funcionarios = df_sql('funcionarios', database_url)
df_order_details = df_sql('order_details', database_url)
df_produtos = df_sql('produtos', database_url)
df_territorios = df_sql('territorio', database_url)
df_regiao = df_sql('região', database_url)
df_expedidor = df_sql('expedidor', database_url)
df_customer = df_sql('customers', database_url)


# Visualizando o dataframe
df_ordens.head()
df_fornecedores.head()
df_categorias.head()
df_funcionarios.head()
df_order_details.head()
df_produtos.head()
df_territorios.head()
df_regiao.head()
df_expedidor.head()
df_customer.head()


# Visualizando a quantidade de informações
df_ordens.shape
df_fornecedores.shape
df_categorias.shape
df_funcionarios.shape
df_order_details.shape
df_produtos.shape
df_territorios.shape
df_regiao.shape
df_expedidor.shape


# Primeira pergunta: Qual é o produto mais vendido
df_mais_vendido = pd.merge(df_produtos, df_order_details, left_on='product_id', right_on='product_id', how='inner')

df_mais_vendido['Total_Sales'] = df_mais_vendido.groupby(['product_name'],sort='product_name')[['product_name']].count()

df_vendas = df_mais_vendido.groupby(['product_name'],sort='product_name')[['product_id']].count()

df_vendas_total = pd.merge(df_mais_vendido, df_vendas, left_on='product_name', right_on='product_name', suffixes=['_old', '_new'], how='inner')

df_vendas_total = df_vendas_total.drop_duplicates('product_name')

df_vendas_total = df_vendas_total.sort_values('product_id_new', ascending=False)

df_vendas_total.head()

# Conclusão: O produto mais vendido foi Raclette Courdavault


# Segunda pergunta: Produto que trouxe mais lucro

## Criando a coluna valor_total no df order_details
df_order_details['Total_Value'] = df_order_details['unit_price'] * df_order_details['quantity']

df_total_vendas_produtos = pd.merge(df_produtos, df_order_details, left_on='product_id', right_on='product_id', suffixes=['_old', '_new'], how='inner')

df_total_vendas_produtos = df_total_vendas_produtos.groupby('product_name')[['Total_Value']].sum()

df_total_vendas_produtos.sort_values('Total_Value', ascending=False)


# Conclusão: O produto mais vendido foi Côte de Blaye


# Terceira pergunta: Qual o vendedor que mais vendeu?

## Selecionando as colunas necessárias
colunas = ['employee_id','Full_Name', 'title']

## Criando a coluna nome completo pois pode ter mais de um funcionário com o mesmo primeiro nome
df_funcionarios['Full_Name'] = df_funcionarios['first_name'] + ' ' + df_funcionarios['last_name']

df_funcionarios_new = df_funcionarios[colunas]

df_total_ordens = pd.merge(df_ordens, df_funcionarios_new, suffixes=['_old','_new'], how='inner')

df_total_ordens.groupby('Full_Name')[['order_id']].count()

df_total_vendas_funcionario = df_total_ordens.groupby('Full_Name')[['order_id']].count()

df_total_vendas_funcionario.sort_values('order_id', ascending=False)

# Conclusão: O funcionário que mais vendeu foi Margaret Peacock


# Quarta pergunta: Qual o vendedor que trouxe mais lucro?
df_valor_pedido = df_order_details.groupby('order_id')[['Total_Value']].sum()

df_valor_ordem = pd.merge(df_ordens, df_valor_pedido, left_on='order_id', right_on='order_id', suffixes=['_old','_new'], how='inner')

df_valor_ordem = pd.merge(df_valor_ordem, df_funcionarios_new, how='inner')

df_valor_ordem.groupby('Full_Name')[['Total_Value']].sum()

df_maior_valor = df_valor_ordem.groupby('Full_Name')[['Total_Value']].sum()

df_maior_valor.sort_values('Total_Value', ascending=False)

# Conclusão: O funcionário que trouxe mais lucro foi Margaret Peacock. 
# Ponto importante, podemos ver que a Laura Collahan que é a 4º maior vendedora em termos de quantidade é apenas a 6 º em termos de trazer lucros e mesmo o Robert King, 6º em quantidade e tendo 30% menos vendas que a Laura trouxe mais lucro que a mesma


# Quinta Pergunta: Qual o principal cliente? Ou seja, o que mais comprou? E o que trouxe mais lucro?
df_clientes = pd.merge(df_customer, df_ordens, how='inner')

df_clientes = df_clientes.groupby('company_name')[['customer_id']].count()

df_clientes.sort_values('customer_id', ascending=False)

df_valor_pedido = df_order_details.groupby('order_id')[['Total_Value']].sum()

df_valor_ordem = pd.merge(df_ordens, df_valor_pedido, left_on='order_id', right_on='order_id', suffixes=['_old','_new'], how='inner')

df_valor_ordem = pd.merge(df_valor_ordem, df_funcionarios_new, how='inner')

df_valor_ordem.head()

df_valor_cliente =pd.merge(df_customer, df_valor_ordem, how='inner')

df_valor_cliente = df_valor_cliente.groupby('company_name')[['Total_Value']].sum()

df_valor_cliente.sort_values('Total_Value', ascending=False)

df_analise_cliente = pd.merge(df_valor_cliente, df_clientes, left_on='company_name', right_on='company_name',how='inner')

df_analise_cliente.sort_values('Total_Value', ascending=False)

# Sexta Pergunta: Países mais vendidos
df_paises = pd.merge(df_customer, df_ordens, how='inner')

df_paises = df_paises.groupby('country')[['customer_id']].count()

df_paises.sort_values('customer_id', ascending=False)


# Sétima pergunta: Qual a categoria de produto mais vendida?
df_tipo_produto = pd.merge(df_produtos, df_categorias, how='inner')

df_produto_vendido = pd.merge(df_order_details, df_tipo_produto, how='inner')

df_qtd_produto = df_produto_vendido.groupby('category_name')[['quantity']].sum()

df_qtd_produto.sort_values('quantity', ascending=False)