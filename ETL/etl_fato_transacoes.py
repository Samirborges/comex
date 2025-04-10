import polars as pl
import psycopg2
import mysql.connector
from datetime import datetime

# === 1. Conexão com o OLTP ===
conn_pg = psycopg2.connect(
    host="localhost",
    dbname="db_comex_oltp",
    user="postegres",
    password="root"
)

# === 2. Query base (com descrições, não IDs) ===
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
JOIN moedas mo ON c.moeda_origem = mo.id
JOIN moedas md ON c.moeda_destino = md.id
JOIN produtos p ON t.produto_id = p.id
JOIN transportes tr ON t.transporte_id = tr.id
JOIN tipos_transacoes tt ON t.tipo_id = tt.id
JOIN paises po ON t.pais_origem = po.id
JOIN paises pd ON t.pais_destino = pd.id
"""

df = pl.read_database(query, connection=conn_pg)

# === 3. Conexão com o Data Mart ===
conn_mysql = mysql.connector.connect(
    host="localhost",
    user="root",
    password="@Sa111419",
    database="starcomex_dm"
)
cursor = conn_mysql.cursor()

# === 4. Inserção na tabela fato_transacoes ===
for row in df.iter_rows(named=True):
    # === 4.1 Buscar os IDs das dimensões ===
    cursor.execute("SELECT id FROM dim_tempo WHERE data = %s", (row["data_transacao"],))
    data_id = cursor.fetchone()

    cursor.execute("SELECT id FROM dim_pais WHERE nome = %s", (row["pais_origem"],))
    pais_origem_id = cursor.fetchone()

    cursor.execute("SELECT id FROM dim_pais WHERE nome = %s", (row["pais_destino"],))
    pais_destino_id = cursor.fetchone()

    cursor.execute("SELECT id FROM dim_produto WHERE descricao = %s", (row["produto"],))
    produto_id = cursor.fetchone()

    cursor.execute("SELECT id FROM dim_transporte WHERE descricao = %s", (row["transporte"],))
    transporte_id = cursor.fetchone()

    cursor.execute("SELECT id FROM dim_tipo_transacao WHERE descricao = %s", (row["tipo_transacao"],))
    tipo_transacao_id = cursor.fetchone()

    cursor.execute("SELECT id FROM dim_moeda WHERE codigo_iso = %s", (row["moeda_origem"],))
    moeda_origem_id = cursor.fetchone()

    cursor.execute("SELECT id FROM dim_moeda WHERE codigo_iso = %s", (row["moeda_destino"],))
    moeda_destino_id = cursor.fetchone()

    cursor.execute("""
        SELECT taxa_cambio FROM dim_cambio
        WHERE data = %s AND moeda_origem = %s AND moeda_destino = %s
    """, (row["data_transacao"], row["moeda_origem"], row["moeda_destino"]))
    taxa = cursor.fetchone()

    # Validação simples (pular se faltar algum ID)
    if data_id is None:
        print(f"⚠️ data_id não encontrado para {row['data_transacao']}")
    if pais_origem_id is None:
        print(f"⚠️ pais_origem_id não encontrado para {row['pais_origem']}")
    if pais_destino_id is None:
        print(f"⚠️ pais_destino_id não encontrado para {row['pais_destino']}")
    if produto_id is None:
        print(f"⚠️ produto_id não encontrado para {row['produto']}")
    if transporte_id is None:
        print(f"⚠️ transporte_id não encontrado para {row['transporte']}")
    if tipo_transacao_id is None:
        print(f"⚠️ tipo_transacao_id não encontrado para {row['tipo_transacao']}")
    if moeda_origem_id is None:
        print(f"⚠️ moeda_origem_id não encontrado para {row['moeda_origem']}")
    if moeda_destino_id is None:
        print(f"⚠️ moeda_destino_id não encontrado para {row['moeda_destino']}")
    if taxa is None:
        print(f"⚠️ Taxa de câmbio não encontrada para {row['data_transacao']} | {row['moeda_origem']} -> {row['moeda_destino']}")
    
    if None in [data_id, pais_origem_id, pais_destino_id, produto_id,
                transporte_id, tipo_transacao_id, moeda_origem_id, moeda_destino_id, taxa]:
        print(f"❌ Dados incompletos para transação ID {row['id']}. Ignorado.\n")
        continue

    taxa_cambio = taxa[0]
    valor_convertido = float(row["valor_monetario"]) * float(taxa_cambio)

    # === 4.2 Inserir na fato_transacoes ===
    insert_query = """
        INSERT INTO fato_transacoes (
            data_id, pais_origem_id, pais_destino_id, produto_id,
            tipo_transacao_id, moeda_origem_id, moeda_destino_id,
            transporte_id, valor_monetario, quantidade, taxa_cambio, valor_convertido
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    cursor.execute(insert_query, (
        data_id[0], pais_origem_id[0], pais_destino_id[0], produto_id[0],
        tipo_transacao_id[0], moeda_origem_id[0], moeda_destino_id[0],
        transporte_id[0], row["valor_monetario"], row["quantidade"],
        taxa_cambio, valor_convertido
    ))

conn_mysql.commit()
cursor.close()
conn_mysql.close()
conn_pg.close()

print("✅ Fato carregada com sucesso.")
