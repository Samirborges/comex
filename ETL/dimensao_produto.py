import polars as pl
import psycopg2
import mysql.connector

# === 1. Conexão com o OLTP (PostgreSQL) ===
conn_pg = psycopg2.connect(
    host="localhost",
    dbname="db_comex_oltp",
    user="usuário_postgres",
    password="usuário_root"
)

# === 2. Conexão com o Data Mart (MySQL) ===
conn_mysql = mysql.connector.connect(
    host="localhost",
    database="starcomex_dm",
    user="root",
    password="@Sa111419"
)

cursor = conn_mysql.cursor()

# === 3. Consulta com JOIN ===
query = """
SELECT
    p.id,
    p.descricao,
    c.descricao AS categoria,
    p.codigo_ncm
FROM produtos p
JOIN categoria_produtos c ON p.categoria_id = c.id
"""

df_produto = pl.read_database(query=query, connection=conn_pg)

# === 4. Transformações: tudo em caixa alta ===
df_tratado = df_produto.with_columns([
    pl.col("descricao").str.to_uppercase(),
    pl.col("categoria").str.to_uppercase(),
    pl.col("codigo_ncm").str.to_uppercase()
])

# === 5. Inserção no Data Mart ===
insert_query = """
    INSERT INTO dim_produto (id, descricao, categoria, codigo_ncm)
    VALUES (%s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        descricao = VALUES(descricao),
        categoria = VALUES(categoria),
        codigo_ncm = VALUES(codigo_ncm)
"""

for row in df_tratado.iter_rows(named=True):
    cursor.execute(insert_query, (
        row["id"],
        row["descricao"],
        row["categoria"],
        row["codigo_ncm"]
    ))

conn_mysql.commit()
cursor.close()
conn_mysql.close()
conn_pg.close()

print("✅ Dimensão produto carregada com sucesso no Data Mart!")
