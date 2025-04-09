import polars as pl
import psycopg2
import os

# === 1. Conexão com o OLTP (PostgreSQL) ===
conn_pg = psycopg2.connect(
    host="localhost",
    dbname="db_comex_oltp",
    user="usuário",
    password="senha"
)

# === 2. Query SQL de extração para fato_transacoes (pré-transformada) ===
query = """
SELECT 
    t.id,
    c.data AS data_transacao,
    mo.pais AS moeda_origem,
    md.pais AS moeda_destino,
    t.valor_monetario,
    t.quantidade,
    p.descricao AS produto,
    tr.descricao AS transporte,
    tt.descricao AS tipo_transacao,
    po.nome AS pais_origem,
    pd.nome AS pais_destino
FROM transacoes t
JOIN cambios c ON t.cambio_id = c.id
JOIN tipos_transacoes tt ON t.tipo_id = tt.id
JOIN moedas mo ON c.moeda_origem = mo.id
JOIN moedas md ON c.moeda_destino = md.id
JOIN produtos p ON t.produto_id = p.id
JOIN transportes tr ON t.transporte_id = tr.id
JOIN paises po ON t.pais_origem = po.id
JOIN paises pd ON t.pais_destino = pd.id
"""

# === 3. Lê os dados com Polars ===
df = pl.read_database(query=query, connection=conn_pg)

# === 4. Caminho local para salvar CSV ===
caminho_csv = "C:/Users/"

# Cria diretório se não existir
if not os.path.exists(caminho_csv):
    os.makedirs(caminho_csv)

# === 5. Salva o CSV local para inspeção ===
df.write_csv(f"{caminho_csv}/fato_transacoes.csv")
print(f"✅ Arquivo salvo em: {caminho_csv}/fato_transacoes.csv")

# === 6. Fecha a conexão ===
conn_pg.close()
