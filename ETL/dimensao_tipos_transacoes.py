import polars as pl
import psycopg2
import mysql.connector

# === 1. Conexão com o OLTP (PostgreSQL) ===
conn_pg = psycopg2.connect(
    host="localhost",
    dbname="db_comex_oltp",
    user="usuário_postegre",
    password="senha"
)

# === 2. Conexão com o Data Mart (MySQL) ===
conn_mysql = mysql.connector.connect(
    host="localhost",
    database="starcomex_dm",
    user="usuário",
    password="senha"
)

cursor = conn_mysql.cursor()

# === 3. Consulta ===
query = "SELECT id, descricao FROM tipos_transacoes"

df_tipo = pl.read_database(query=query, connection=conn_pg)

# === 4. Transformações (caixa alta) ===
df_tratado = df_tipo.with_columns([
    pl.col("descricao").str.to_uppercase()
])

# === 5. Inserção no Data Mart ===
insert_query = """
    INSERT INTO dim_tipo_transacao (id, descricao)
    VALUES (%s, %s)
    ON DUPLICATE KEY UPDATE
        descricao = VALUES(descricao)
"""

for row in df_tratado.iter_rows(named=True):
    cursor.execute(insert_query, (
        row["id"],
        row["descricao"]
    ))

conn_mysql.commit()
cursor.close()
conn_mysql.close()
conn_pg.close()

print("✅ Dimensão tipo_transacao carregada com sucesso no Data Mart!")
