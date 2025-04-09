import polars as pl
import requests
import psycopg2
import mysql.connector

# === 1. CONEXÃO COM O POSTGRESQL (OLTP) ===
conn_pg = psycopg2.connect(
    host="localhost",
    dbname="db_comex_oltp",
    user="usuário_postegre",
    password="senha"
)

# === 2. CONSULTA PARES ÚNICOS DE MOEDAS + DATA ===
query = """
SELECT DISTINCT
    c.data AS data_transacao,
    mo_origem.pais AS moeda_origem,
    mo_destino.pais AS moeda_destino
FROM
    transacoes t
JOIN cambios c ON t.cambio_id = c.id
JOIN moedas mo_origem ON c.moeda_origem = mo_origem.id
JOIN moedas mo_destino ON c.moeda_destino = mo_destino.id
"""

df_pares = pl.read_database(query, connection=conn_pg)

# === 3. CONEXÃO COM O MYSQL (DATA MART) ===
conn_mysql = mysql.connector.connect(
    host="localhost",
    database="starcomex_dm",
    user="usuário_mysql",
    password="senha"
)
cursor = conn_mysql.cursor()

# === 4. FUNÇÃO PARA PEGAR TAXA DA API ===
def pegar_cambio(data, base, destino):
    url = f"https://api.frankfurter.app/{data}?base={base}&symbols={destino}"
    resp = requests.get(url)
    if resp.status_code == 200:
        return resp.json()["rates"].get(destino)
    return None

# === 5. INSERIR TAXAS NA DIM_CAMBIO (SE NÃO EXISTIR) ===
for row in df_pares.iter_rows(named=True):
    data = row['data_transacao']
    origem = row['moeda_origem'].upper()
    destino = row['moeda_destino'].upper()

    # Verifica se já existe na tabela
    cursor.execute("""
        SELECT taxa_cambio FROM dim_cambio
        WHERE data = %s AND moeda_origem = %s AND moeda_destino = %s
    """, (data, origem, destino))
    existe = cursor.fetchone()

    if not existe:
        taxa = pegar_cambio(data.isoformat(), origem, destino)
        if taxa:
            cursor.execute("""
                INSERT INTO dim_cambio (data, moeda_origem, moeda_destino, taxa_cambio)
                VALUES (%s, %s, %s, %s)
            """, (data, origem, destino, taxa))
            print(f"✔️ Inserido: {data} | {origem} -> {destino} | {taxa}")
        else:
            print(f"⚠️ API não retornou taxa para {data} | {origem} -> {destino}")
    else:
        print(f"⏩ Já existe: {data} | {origem} -> {destino}")

conn_mysql.commit()
cursor.close()
conn_mysql.close()
conn_pg.close()

