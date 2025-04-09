import polars as pl
import psycopg2
import mysql.connector

# === 1. Conexão com os bancos ===
conn_pg = psycopg2.connect(
    host="localhost",
    dbname="db_comex_oltp",
    user="usuário_postgres",
    password="senha"
)

conn_mysql = mysql.connector.connect(
    host="localhost",
    database="starcomex_dm",
    user="usuário_mysql",
    password="senha"
)

cursor = conn_mysql.cursor()

# === 2. Extração do OLTP ===
query = "SELECT id, descricao, pais FROM moedas"
df_moedas = pl.read_database(query=query, connection=conn_pg)

# === 3. Transformação: caixa alta ===
df_tratado = df_moedas.with_columns([
    pl.col("descricao").str.to_uppercase(),
    pl.col("pais").str.to_uppercase()
])

# === 4. Inserção no Data Mart ===
insert_query = """
    INSERT INTO dim_moeda (id, descricao, pais)
    VALUES (%s, %s, %s)
    ON DUPLICATE KEY UPDATE
        descricao = VALUES(descricao),
        pais = VALUES(pais)
"""

for row in df_tratado.iter_rows(named=True):
    cursor.execute(insert_query, (
        row["id"],
        row["descricao"],
        row["pais"]
    ))

conn_mysql.commit()
cursor.close()
conn_mysql.close()
conn_pg.close()

print("✅ Dimensão moeda carregada com sucesso no Data Mart!")
