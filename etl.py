import polars as pl
import requests
import mysql.connector
import psycopg2
from datetime import datetime

# Conexão com o PostgreSQL (OLTP)
conn_pg = psycopg2.connect(
    host="localhost",
    dbname="starcomex_oltp",
    user="usuario_pg",
    password="senha_pg"
)

# Consulta dados da tabela de transações no OLTP
df_oltp = pl.read_database(
    query="SELECT id, data_transacao, moeda_origem, moeda_destino, valor FROM transacoes",
    connection=conn_pg
)

# Função para pegar a taxa de câmbio do Frankfurter
def pegar_cambio(data, base, destino):
    url = f"https://api.frankfurter.dev/{data}?base={base}&symbols={destino}"
    resp = requests.get(url)
    if resp.status_code == 200:
        return resp.json()["rates"][destino]
    return None

# Tratamento: maiúsculas e formato de data YYYYMMDD
df_tratado = df_oltp.with_columns([
    pl.col("moeda_origem").str.to_uppercase(),
    pl.col("moeda_destino").str.to_uppercase(),
    pl.col("data_transacao").dt.strftime("%Y%m%d").alias("data_formatada")
])

# Adiciona coluna com valor convertido
taxas = []
for row in df_tratado.iter_rows(named=True):
    taxa = pegar_cambio(
        datetime.strptime(row['data_transacao'], "%Y-%m-%d").date().isoformat(),
        row['moeda_origem'],
        row['moeda_destino']
    )
    taxas.append(taxa if taxa else 0)

df_tratado = df_tratado.with_columns([
    pl.Series("taxa_cambio", taxas),
    (pl.col("valor") * pl.Series(taxas)).alias("valor_convertido")
])

# Conexão com o MySQL (Data Mart)
conn_mysql = mysql.connector.connect(
    host="localhost",
    database="starcomex_dm",
    user="usuario_mysql",
    password="senha_mysql"
)

cursor = conn_mysql.cursor()

# Insere os dados tratados no Data Mart
for row in df_tratado.iter_rows(named=True):
    insert_query = """
        INSERT INTO fato_transacoes (
            id_transacao, data_transacao, moeda_origem, moeda_destino,
            valor_original, taxa_cambio, valor_convertido
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    cursor.execute(insert_query, (
        row["id"],
        row["data_formatada"],
        row["moeda_origem"],
        row["moeda_destino"],
        row["valor"],
        row["taxa_cambio"],
        row["valor_convertido"]
    ))

conn_mysql.commit()
cursor.close()
conn_mysql.close()
conn_pg.close()
