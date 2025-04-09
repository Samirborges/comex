import polars as pl
import mysql.connector
import psycopg2

# === 1. CONEXÃO COM O POSTGRESQL (OLTP) ===
conn_pg = psycopg2.connect(
    host="localhost",
    dbname="db_comex_oltp",
    user="usuário_postgres",
    password="senha"
)

# === 2. EXTRAIR TODAS AS DATAS ÚNICAS DA TABELA CAMBIOS ===
df_datas = pl.read_database(
    query="SELECT DISTINCT data FROM cambios",
    connection=conn_pg
)

# === 3. TRATAMENTO DOS CAMPOS DE DATA ===
df_dim_tempo = df_datas.with_columns([
    pl.col("data").alias("data"),
    pl.col("data").dt.year().alias("ano"),
    pl.col("data").dt.month().alias("mes"),
    pl.col("data").dt.day().alias("dia"),
    pl.col("data").dt.strftime("%Y-%m").alias("ano_mes"),
    pl.col("data").dt.strftime("%Y%m%d").cast(pl.Int32).alias("data_yyyymmdd")
]).sort("data")

# === 4. CONEXÃO COM O MYSQL (DATA MART) ===
conn_mysql = mysql.connector.connect(
    host="localhost",
    database="starcomex_dm",
    user="usuário",
    password="senha"
)
cursor = conn_mysql.cursor()

# === 5. INSERÇÃO NA TABELA dim_tempo ===
for row in df_dim_tempo.iter_rows(named=True):
    insert_query = """
        INSERT INTO dim_tempo (data, ano, mes, dia, ano_mes, data_yyyymmdd)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    cursor.execute(insert_query, (
        row["data"],
        row["ano"],
        row["mes"],
        row["dia"],
        row["ano_mes"],
        row["data_yyyymmdd"]
    ))

conn_mysql.commit()
cursor.close()
conn_mysql.close()
conn_pg.close()
