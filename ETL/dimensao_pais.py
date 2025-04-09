import polars as pl
import psycopg2
import mysql.connector

# === 1. Conexões com os bancos ===
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

# === 2. Consulta com JOIN para pegar o bloco econômico ===
query = """
SELECT
    p.id,
    p.nome,
    p.codigo_iso,
    b.nome AS bloco_economico
FROM paises p
JOIN blocos_economicos b ON p.bloco_id = b.id
"""

df_pais = pl.read_database(query=query, connection=conn_pg)

# === 3. Transformações: caixa alta ===
df_tratado = df_pais.with_columns([
    pl.col("nome").str.to_uppercase(),
    pl.col("codigo_iso").str.to_uppercase(),
    pl.col("bloco_economico").str.to_uppercase()
])

# === 4. Inserção no Data Mart ===
insert_query = """
    INSERT INTO dim_pais (id, nome, codigo_iso, bloco_economico)
    VALUES (%s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        nome = VALUES(nome),
        codigo_iso = VALUES(codigo_iso),
        bloco_economico = VALUES(bloco_economico)
"""

for row in df_tratado.iter_rows(named=True):
    cursor.execute(insert_query, (
        row["id"],
        row["nome"],
        row["codigo_iso"],
        row["bloco_economico"]
    ))

conn_mysql.commit()
cursor.close()
conn_mysql.close()
conn_pg.close()

print("✅ Dimensão país carregada com sucesso no Data Mart!")
