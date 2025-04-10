import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import mysql.connector

# Configuração do dashboard
st.set_page_config(page_title="Dashboard Lab. Banco de Dados", layout="wide")
st.title("📊 Starcomex")

# Barra lateral de navegação
menu = st.sidebar.radio("Navegação", ["🏠 Visão Geral", "📈 Gráficos", "🌐 Sankey & Câmbio", "🚚 Transporte"])

# Função para criar KPI card (indicadores)
def kpi_card(title, value, color="#3498db", icon="📊"):
    return f"""
    <div style='background-color: {color}; border-radius: 10px; padding: 20px; margin: 10px; width: 280px; color: white; box-shadow: 0px 2px 10px rgba(0,0,0,0.2);'>
        <h4 style='margin-bottom: 5px;'>{icon} {title}</h4>
        <p style='font-size: 22px; font-weight: bold; margin: 0;'>{value}</p>
    </div>
    """

# Conexão com MySQL
@st.cache_resource
def conectar_mysql():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="senha",
        database="starcomex_dm"
    )

# Carregar dados da view unificada
@st.cache_data
def carregar_dados():
    conn = conectar_mysql()
    query = """
    SELECT 
    f.valor_monetario,
    f.quantidade,
    f.taxa_cambio,
    f.data_id,
    f.pais_origem_id,
    f.pais_destino_id,
    f.produto_id,
    f.tipo_transacao_id,
    f.transporte_id,
    f.moeda_origem_id,
    po.nome AS pais_origem,
    pd.nome AS pais_destino,
    p.descricao AS produto_desc,
    tt.descricao AS tipo_transacao,
    t.ano AS ano,
    t.data AS data_formatada,
    po.bloco_economico AS bloco_origem,
    m.codigo_iso AS moeda,
    tp.descricao AS transporte
FROM fato_transacoes f
JOIN dim_pais po ON f.pais_origem_id = po.id
JOIN dim_pais pd ON f.pais_destino_id = pd.id
JOIN dim_produto p ON f.produto_id = p.id
JOIN dim_tipo_transacao tt ON f.tipo_transacao_id = tt.id
JOIN dim_tempo t ON f.data_id = t.id
JOIN dim_moeda m ON f.moeda_origem_id = m.id
JOIN dim_transporte tp ON f.transporte_id = tp.id;

    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

try:
    df = carregar_dados()

    # Pré-processamentos
    df_exportacao = df.groupby("pais_origem")["valor_monetario"].sum().reset_index().sort_values(by="valor_monetario", ascending=False)
    df_produtos = df.groupby("produto_desc")["quantidade"].sum().reset_index().sort_values(by="quantidade", ascending=False)
    df_blocos = df.groupby(["data_formatada", "bloco_origem"])["valor_monetario"].sum().reset_index()
    df_parceiros = df.groupby(["pais_origem", "pais_destino"])["valor_monetario"].sum().reset_index()
    unique_nodes = list(pd.unique(df_parceiros[["pais_origem", "pais_destino"]].values.ravel()))
    node_indices = {node: i for i, node in enumerate(unique_nodes)}
    df_cambio = df.groupby("data_formatada")["taxa_cambio"].mean().reset_index()
    df_transacoes = df.groupby("data_formatada")["valor_monetario"].sum().reset_index()
    df_transporte = df.groupby("transporte")["valor_monetario"].sum().reset_index()

    # Gráficos
    fig1 = px.bar(df_exportacao, x="pais_origem", y="valor_monetario", title="Exportações por País")
    fig2 = px.bar(df_produtos, x="produto_desc", y="quantidade", title="Produtos Mais Comercializados")
    
    fig3 = go.Figure()
    for bloco in df_blocos["bloco_origem"].unique():
        df_bloco = df_blocos[df_blocos["bloco_origem"] == bloco]
        fig3.add_trace(go.Scatter(
            x=df_bloco["data_formatada"], 
            y=df_bloco["valor_monetario"],
            mode="lines",
            name=bloco
        ))
    fig3.update_layout(title="Comércio por Bloco Econômico")

    fig4 = go.Figure(go.Sankey(
        node=dict(label=unique_nodes, pad=15, thickness=20),
        link=dict(
            source=[node_indices[x] for x in df_parceiros["pais_origem"]],
            target=[node_indices[x] for x in df_parceiros["pais_destino"]],
            value=df_parceiros["valor_monetario"].tolist()
        )
    ))
    fig4.update_layout(title="Fluxos de Comércio")

    fig5 = go.Figure()
    fig5.add_trace(go.Scatter(x=df_cambio["data_formatada"], y=df_cambio["taxa_cambio"], name="Taxa de Câmbio", line=dict(color="blue")))
    fig5.add_trace(go.Scatter(x=df_transacoes["data_formatada"], y=df_transacoes["valor_monetario"], name="Volume Transações", line=dict(color="green")))
    fig5.update_layout(title="Taxa de Câmbio x Volume de Transações")

    fig6 = px.pie(df_transporte, names="transporte", values="valor_monetario", title="Participação por Tipo de Transporte")

    # Indicadores
    total_exportado = df_exportacao["valor_monetario"].sum()
    media_por_pais = df_exportacao["valor_monetario"].mean()
    maior_exportacao = df_exportacao["valor_monetario"].max()

    # ---------------- NAVIGATION ----------------
    if menu == "🏠 Visão Geral":
        st.subheader("📊 Indicadores Gerais")
        kpi_cards_html = f"""
        <div style="display: flex; flex-wrap: wrap; justify-content: space-around;">
            {kpi_card("Total Exportado", f"${total_exportado:,.2f}", "#0E6BA8", "💰")}
            {kpi_card("Média por País", f"${media_por_pais:,.2f}", "#218380", "🌍")}
            {kpi_card("Maior Exportação", f"${maior_exportacao:,.2f}", "#3D348B", "🚀")}
        </div>
        """
        st.markdown(kpi_cards_html, unsafe_allow_html=True)

    elif menu == "📈 Gráficos":
        col1, col2 = st.columns(2)
        with col1:
            st.plotly_chart(fig1, use_container_width=True)
        with col2:
            st.plotly_chart(fig2, use_container_width=True)

        st.plotly_chart(fig3, use_container_width=True)

    elif menu == "🌐 Sankey & Câmbio":
        col3, col4 = st.columns(2)
        with col3:
            st.plotly_chart(fig4, use_container_width=True)
        with col4:
            st.plotly_chart(fig5, use_container_width=True)

    elif menu == "🚚 Transporte":
        st.plotly_chart(fig6, use_container_width=True)

except Exception as e:
    st.error(f"Erro ao conectar ou carregar os dados: {e}")
