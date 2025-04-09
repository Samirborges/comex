import polars as pl
import requests
import mysql.connector
import psycopg2
from datetime import datetime

# === 1. CONEXÃO COM O POSTGRESQL (OLTP) ===
conn_pg = psycopg2.connect(
    host="localhost",
    dbname="db_comex_oltp",
    user="postgres",
    password="root"
)

# === 2. CONSULTA SQL ADAPTADA PARA EXTRAIR AS INFORMAÇÕES ===
query = """
SELECT
    t.id,
    c.data AS data_transacao,
    mo_origem.pais AS moeda_origem,
    mo_destino.pais AS moeda_destino,
    t.valor_monetario AS valor
FROM
    transacoes t
JOIN cambios c ON t.cambio_id = c.id
JOIN moedas mo_origem ON c.moeda_origem = mo_origem.id
JOIN moedas mo_destino ON c.moeda_destino = mo_destino.id
"""

# === 3. LEITURA DOS DADOS NO FORMATO POLARS ===
df_oltp = pl.read_database(query=query, connection=conn_pg)

# === 4. FUNÇÃO PARA OBTER TAXA DE CÂMBIO COM A API ===
def pegar_cambio(data, base, destino):
    url = f"https://api.frankfurter.app/{data}?base={base}&symbols={destino}"
    resp = requests.get(url)
    if resp.status_code == 200:
        return resp.json()["rates"][destino]
    return None

# === 5. TRANSFORMAÇÕES (CAIXA ALTA E FORMATO DE DATA) ===
df_tratado = df_oltp.with_columns([
    pl.col("moeda_origem").str.to_uppercase(),
    pl.col("moeda_destino").str.to_uppercase(),
    pl.col("data_transacao").dt.strftime("%Y%m%d").alias("data_formatada")
])

# === 6. OBTENDO TAXAS E CALCULANDO VALOR CONVERTIDO ===
taxas = []
for row in df_tratado.iter_rows(named=True):
    taxa = pegar_cambio(
        row["data_transacao"].isoformat(),
        row["moeda_origem"],
        row["moeda_destino"]
    )
    taxas.append(taxa if taxa else 0)

df_tratado = df_tratado.with_columns([
    pl.Series("taxa_cambio", taxas),
    (pl.col("valor") * pl.Series(taxas)).alias("valor_convertido")
])

# === 7. CONEXÃO COM O MYSQL (DATA MART) ===
conn_mysql = mysql.connector.connect(
    host="localhost",
    database="starcomex_dm",
    user="root",
    password="@Sa111419"
)

cursor = conn_mysql.cursor()

# === 8. INSERÇÃO DOS DADOS TRATADOS NO DATA MART ===
for row in df_tratado.iter_rows(named=True):
    insert_query = """
        INSERT INTO teste_transacoes (
            data_transacao, moeda_origem, moeda_destino,
            valor_original, taxa_cambio, valor_convertido
        )
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    cursor.execute(insert_query, (
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
