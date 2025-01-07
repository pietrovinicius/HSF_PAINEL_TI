# 03/01/2025
# @PLima
# HFS - PAINEL DE DIVERSOS DADOS E INDICADORES
# Analítico SLA - Ordem de Serviço
# RELATORIO 1618 - HSF - Analítico SLA - Ordem de Serviço (EXCEL)

import streamlit as st
import pandas as pd
import os
import oracledb
import locale
import datetime
import plotly.express as px
import io
import plotly.colors as pc
import random

# Configuração da página Streamlit
st.set_page_config(layout="wide", initial_sidebar_state="expanded", page_title="Analítico SLA - Ordem de Serviço")

# Aumentando exibição do DataFrame no Streamlit
pd.set_option("styler.render.max_elements", 1249090)




def obter_timestamp_atual():
    """Retorna o timestamp atual no formato YYYY-MM-DD HH-MM-SS."""
    return datetime.datetime.now().strftime("%Y-%m-%d %H-%M-%S")


def encontrar_diretorio_instantclient(
        nome_pasta="instantclient-basiclite-windows.x64-23.6.0.24.10\\instantclient_23_6"):
    """
        Encontra o diretório do Instant Client Oracle.
        Args:
            nome_pasta (str): Nome da pasta contendo o Instant Client.
        Returns:
            str: Caminho completo para o diretório do Instant Client ou None se não encontrado.
    """
    diretorio_atual = os.path.dirname(os.path.abspath(__file__))
    caminho_instantclient = os.path.join(diretorio_atual, nome_pasta)

    if os.path.exists(caminho_instantclient):
        return caminho_instantclient
    else:
        st.error(f"A pasta '{nome_pasta}' não foi encontrada na raiz do aplicativo.")
        return None


@st.cache_data
def obter_dados_relatorio_1618():
    """
        Obtém os dados do relatório 1618 do banco de dados Oracle.
        Returns:
            pandas.DataFrame: DataFrame contendo os dados do relatório.
    """
    try:
        caminho_instantclient = encontrar_diretorio_instantclient()
        if caminho_instantclient:
            oracledb.init_oracle_client(lib_dir=caminho_instantclient)
        else:
            st.error("Erro ao localizar o Instant Client. Verifique o nome da pasta e o caminho.")
            return pd.DataFrame()  # Retorna um DataFrame vazio em caso de erro

        connection = oracledb.connect(user="TASY", password="aloisk", dsn="192.168.5.9:1521/TASYPRD")

        with connection.cursor() as cursor:
            sql = """
               SELECT 
                    ATP.NR_SEQUENCIA AS NR_ORDEM,
                   
                    ABREVIA_NOME(INITCAP(OBTER_NOME_USUARIO(ATP.NM_USUARIO_EXEC)), 'A') AS ANALISTA,
                    EXTRACT(YEAR FROM ATP.DT_ORDEM_SERVICO) AS ANO,
                    TO_CHAR(ATP.DT_ORDEM_SERVICO, 'Month') AS MES,
                    EXTRACT(MONTH FROM ATP.DT_ORDEM_SERVICO) AS OS_MES,
                    TO_CHAR(ATP.DT_ORDEM_SERVICO, 'dd/mm/yyyy hh24:mi') AS DATA,
                    DECODE(ATP.IE_PRIORIDADE, 'A', 'Alta', 'M', 'Média', 'E','Emergência', 'Fora da Prioridade') AS DS_PRIORIDADE, 
                    DECODE(ATP.IE_PRIORIDADE, 'A', 240, 'M', 360, 'E', 10) AS META_SLA,
                    OBTER_DIF_DATA(ATP.DT_ORDEM_SERVICO, DT_FIM_REAL, 'TM') AS TEMPO_TOTAL,
                    TO_CHAR(ATP.DT_INICIO_REAL ,'dd/mm/yyyy hh24:mi') AS DT_INICIO_REAL,
                    TO_CHAR(ATP.DT_FIM_REAL,'dd/mm/yyyy hh24:mi') AS DT_FIM_REAL,
                    CASE  
                        WHEN DECODE(ATP.IE_PRIORIDADE, 'A', 240, 'M', 360, 'E', 10) >=  OBTER_DIF_DATA(ATP.DT_ORDEM_SERVICO, ATP.DT_FIM_REAL, 'TM') THEN 'Atendido'
                        WHEN DECODE(ATP.IE_PRIORIDADE, 'A', 240, 'M', 360, 'E', 10) <  OBTER_DIF_DATA(ATP.DT_ORDEM_SERVICO, ATP.DT_FIM_REAL, 'TM') THEN 'Excedido'
                        ELSE 'Fora do SLA'
                    END AS SLA,
                    SAT.CD_SETOR_ATENDIMENTO,
                    INITCAP(SAT.DS_SETOR_ATENDIMENTO) AS SETOR_ATENDIMENTO,
                    (
                        SELECT 
                            DS_TIPO
                        FROM MAN_TIPO_ORDEM_SERVICO
                        WHERE NR_SEQUENCIA = ATP.NR_SEQ_TIPO_ORDEM
                    ) AS DESCRICAO,
                    DECODE(
                    --Todas=0,Aberta=1,Processo=2,Encerradas=3,
                        ATP.IE_STATUS_ORDEM,
                        0, 'Todas',
                        1,'Aberta',
                        2,'Processo',
                        3,'Encerradas'
                    ) AS STATUS_ORDEM   
                FROM MAN_ORDEM_SERVICO ATP
                INNER JOIN MAN_GRUPO_TRABALHO SA ON SA.NR_SEQUENCIA = ATP.NR_GRUPO_TRABALHO
                INNER JOIN MAN_LOCALIZACAO ML ON ML.NR_SEQUENCIA = ATP.NR_SEQ_LOCALIZACAO
                INNER JOIN SETOR_ATENDIMENTO SAT ON SAT.CD_SETOR_ATENDIMENTO = ML.CD_SETOR
                LEFT JOIN MAN_GRUPO_PLANEJAMENTO MGP ON MGP.NR_SEQUENCIA = ATP.NR_GRUPO_PLANEJ
                WHERE ATP.IE_STATUS_ORDEM = 3
                AND ATP.NM_USUARIO_EXEC IN (
                    'amcabral' -- Alex De Mendonca Cabral
                    ,'aptsilva'-- Alexandro Pinheiro Tavares Silva
                    ,'acmmeireles' --Ana Carolina Mendonca Meireles Pelegrino
                    ,'iffialho' -- Ingrid Firmino Fialho
                    ,'jbfilho' --João Batista Gomes De Sousa Filho
                    ,'kloliveira' -- Kevin Lourenco De Oliveira
                    ,'lsojunqueira' -- Lucas Souza De Oliveira Junqueira
                    ,'pvplima'--	Pietro Vinicius Da Penha De Lima
                    , 'ymfcastro' --YAGO MATTOS FELIPPE DE CASTRO
                )
                ORDER BY
                    EXTRACT(YEAR FROM ATP.DT_ORDEM_SERVICO) DESC,
                    EXTRACT(MONTH FROM ATP.DT_ORDEM_SERVICO) ASC,
                    ATP.NR_SEQUENCIA ASC
                """
            cursor.execute(sql)
            results = cursor.fetchall()
            df = pd.DataFrame(results, columns=[desc[0] for desc in cursor.description])
            print(f"\nDF Tamanho:{df.shape}")
            print(f"DF Colunas:\n{df.columns}")
            print(f"\nDF Head(5):\n{df.head(5)}")
            st.success(f"DataFrame gerado com sucesso! - {obter_timestamp_atual()}")
            return df
    except oracledb.Error as e:
        st.error(f"Erro no Oracle: {e}. {obter_timestamp_atual()}")
        return pd.DataFrame()  # Retorna um DataFrame vazio em caso de erro
    except Exception as erro:
        st.error(f"Erro Inesperado: {erro}. {obter_timestamp_atual()}")
        return pd.DataFrame()  # Retorna um DataFrame vazio em caso de erro
    
def sla_cor_status(val):
    """Aplica estilos de cor com base no status SLA."""
    if val == 'Excedido':
        return 'background-color: yellow; color: black ; font-weight: bold'
    elif val == 'Em análise':
        return 'background-color: lightblue; color: black ; font-weight: bold'
    elif val == 'Sim':
        return 'background-color: sandybrown; color: black ; font-weight: bold;'
    else:
        return ''

def calcular_indicadores(df):
    """Calcula os indicadores de SLA."""
    if df.empty:
        return {
            "total_ordens": 0,
            "total_ordens_no_sla": 0,
            "total_ordens_fora_sla": 0,
            "percentual_ordens_no_sla": 0,
            "percentual_ordens_fora_sla": 0,
            "media_tempo_total": 0,
            "media_tempo_por_prioridade": pd.DataFrame(columns=['DS_PRIORIDADE', 'TEMPO_TOTAL']),
              "media_tempo_total_em_horas" : 0
        }

    total_ordens = len(df)
    total_ordens_no_sla = len(df[df['SLA'] == 'Atendido'])
    total_ordens_fora_sla = len(df[df['SLA'] == 'Excedido'])
    percentual_ordens_no_sla = (total_ordens_no_sla / total_ordens) * 100 if total_ordens > 0 else 0
    percentual_ordens_fora_sla = (total_ordens_fora_sla / total_ordens) * 100 if total_ordens > 0 else 0
    df['TEMPO_TOTAL'] = df['TEMPO_TOTAL'].astype(int)
    media_tempo_total = df['TEMPO_TOTAL'].mean()
    media_tempo_por_prioridade = df.groupby('DS_PRIORIDADE')['TEMPO_TOTAL'].mean().reset_index()
    media_tempo_total_em_horas = media_tempo_total / 60 if media_tempo_total > 0 else 0

    return {
        "total_ordens": total_ordens,
        "total_ordens_no_sla": total_ordens_no_sla,
        "total_ordens_fora_sla": total_ordens_fora_sla,
        "percentual_ordens_no_sla": percentual_ordens_no_sla,
        "percentual_ordens_fora_sla": percentual_ordens_fora_sla,
        "media_tempo_total": media_tempo_total,
        "media_tempo_por_prioridade": media_tempo_por_prioridade,
        "media_tempo_total_em_horas" : media_tempo_total_em_horas
    }

def calcular_horas_por_setor(df):
    """Calcula o tempo total em horas gasto por setor."""
    if df.empty:
        return pd.DataFrame(columns=['SETOR_ATENDIMENTO', 'HORAS'])

    df['TEMPO_TOTAL'] = df['TEMPO_TOTAL'].astype(int)
    horas_por_setor = df.groupby('SETOR_ATENDIMENTO')['TEMPO_TOTAL'].sum().reset_index()
    horas_por_setor['HORAS'] = horas_por_setor['TEMPO_TOTAL'] / 60
    horas_por_setor = horas_por_setor.drop('TEMPO_TOTAL', axis=1)
    return horas_por_setor
    
def formatar_horas(horas):
    """Formata as horas para o formato 'X horas Y minutos'."""
    horas_int = int(horas)
    minutos = int((horas - horas_int) * 60)
    return f"{horas_int} hora(s) {minutos:02} minuto(s)"

def calcular_horas_por_analista(df):
    """Calcula o tempo total em horas gasto por analista."""
    if df.empty:
        return pd.DataFrame(columns=['ANALISTA', 'HORAS', 'N° DE O.S'])

    df['TEMPO_TOTAL'] = df['TEMPO_TOTAL'].astype(int)
    horas_por_analista = df.groupby('ANALISTA').agg(
        HORAS=pd.NamedAgg(column='TEMPO_TOTAL', aggfunc='sum'),
        N_OS=pd.NamedAgg(column='NR_ORDEM', aggfunc='nunique')
    ).reset_index()

    horas_por_analista['HORAS'] = horas_por_analista['HORAS'] / 60
    return horas_por_analista

def calcular_homem_hora(df):
    """Calcula o indicador Homem x Hora."""
    if df.empty:
        return 0  # Retorna 0 se não houver dados

    total_minutos = df['TEMPO_TOTAL'].astype(int).sum()
    num_analistas = 8  # Número de analistas (fixo)
    homem_hora = (total_minutos / num_analistas) / 60 if num_analistas > 0 else 0
    return homem_hora

def formatar_horas_df(df):
    """Formata a coluna 'HORAS' de um DataFrame para 'X horas Y minutos'."""
    if df.empty:
      return df
    
    def formatar_horas_individual(horas):
        horas_int = int(horas)
        minutos = int((horas - horas_int) * 60)
        return f"{horas_int} hora(s) {minutos:02} minuto(s)"

    df['HORAS_FORMATADA'] = df['HORAS'].apply(formatar_horas_individual)
    df = df.drop('HORAS', axis=1)
    return df

def exibir_grafico_pizza(df):
    """Exibe o gráfico de pizza de distribuição de SLA."""
    if df.empty:
        st.warning("Não há dados para exibir o gráfico de pizza.")
        return
    sla_counts = df['SLA'].value_counts().reset_index()
    sla_counts.columns = ['SLA', 'count']
    
    color_map = {
        'Atendido': 'lightgreen',
        'Excedido': 'lightcoral',
        'Fora do SLA': 'gray'
    }
    
    fig = px.pie(sla_counts, 
                 names='SLA', 
                 values='count', 
                 title="Distribuição de Ordens por SLA",
                 color='SLA',
                 color_discrete_map = color_map)
    fig.update_traces(
        hovertemplate="<b>Status:</b> %{label}<br><b>Percentual:</b> %{percent:.2f}%"  # Personalizando o hovertemplate
    )

    st.plotly_chart(fig)

def exibir_grafico_barras_tempo_prioridade(indicadores_calc):
    """Exibe o gráfico de barras do tempo médio por prioridade."""
    if indicadores_calc["media_tempo_por_prioridade"].empty:
        st.warning("Não há dados para exibir o gráfico de barras de tempo por prioridade.")
        return
    
    # Formatar a coluna TEMPO_TOTAL para exibição
    indicadores_calc["media_tempo_por_prioridade"]['Tempo_Formatado'] = indicadores_calc["media_tempo_por_prioridade"]['TEMPO_TOTAL'].apply(formatar_horas)
    
    # Definir o mapa de cores
    color_map = {
        'Alta': 'red',
        'Média': 'orange',
        'Fora da Prioridade': 'gray',
        'Emergência': 'purple'
    }
    
    fig = px.bar(indicadores_calc["media_tempo_por_prioridade"], 
                 x='DS_PRIORIDADE', 
                 y='TEMPO_TOTAL',
                 title='Tempo Médio por Prioridade',
                 color='DS_PRIORIDADE',  # Usar DS_PRIORIDADE para aplicar as cores
                 color_discrete_map=color_map,
                 text_auto=False,
                 text = 'Tempo_Formatado'
                )
    
    fig.update_layout(
        legend_title_text=" ",
         margin=dict(l=20, r=20, t=60, b=20),
         #template="plotly_dark", # Paletas de cores que o plotly oferece, pode usar: "plotly_dark" ou "plotly"
        title_font=dict(size=17),
    )
    
    fig.update_xaxes(title_text='')  # Alterar o rótulo do eixo x
    fig.update_yaxes(title_text='Tempo Total (Min)') # Alterar o rótulo do eixo y
    fig.update_traces(
        textposition='outside',  
        textfont_family="Arial", 
        textfont_size=13,
        hovertemplate="<b>Prioridade:</b> %{x}<br><b>Tempo Total:</b> %{text}" # Personalizando o hovertemplate
     )
    st.plotly_chart(fig)
    
def exibir_tipos_os(df):
    """
      Exibe um gráfico de barras da distribuição de tipos de O.S.
    Args:
        df (pd.DataFrame): DataFrame contendo os dados.
    """
    if df.empty:
       st.warning("Não há dados para exibir o gráfico de tipos de O.S")
       return
    print(f"\n=================================================\n{df['DESCRICAO']}\n{df['DESCRICAO'].head(5)}\n")
    tipos_os = df['DESCRICAO'].str.lower().copy()
    
    def categorizar_tipo_os(texto):
      if texto is None:
        return 'Outros'
      if 'corretiva' in texto:
          return 'Corretiva'
      elif 'preventiva' in texto:
          return 'Preventiva'
      elif 'desenvolvimento' in texto:
          return 'Desenvolvimento'
      elif 'projeto' in texto:
          return 'Projetos'
      else:
          return 'Outros'
    
    df['TIPO_OS'] = tipos_os.apply(categorizar_tipo_os)

    tipos_contagem = df['TIPO_OS'].value_counts().reset_index()
    tipos_contagem.columns = ['TIPO_OS', 'QUANTIDADE']
    
    color_map = {
        'Corretiva': 'skyblue',
        'Preventiva': 'lightgreen',
        'Desenvolvimento': 'orange',
        'Projetos': 'lightcoral',
        'Outros': 'gray'
    }
    
    fig = px.bar(tipos_contagem, 
                 x='TIPO_OS', 
                 y='QUANTIDADE', 
                 title='Distribuição de Tipos de O.S',
                 color='TIPO_OS',
                 color_discrete_map = color_map,
                 text_auto=True
                )
                
    fig.update_layout(
        legend_title_text="",
         margin=dict(l=20, r=20, t=60, b=20),
         #template="plotly_dark", # Paletas de cores que o plotly oferece, pode usar: "plotly_dark" ou "plotly"
        title_font=dict(size=17),
    )

    fig.update_xaxes(title_text='')  # Remover o rótulo do eixo x
    fig.update_yaxes(title_text='Quantidade') 
    fig.update_traces(
        textposition='outside',  
        textfont_family="Arial", 
        textfont_size=13,
        hovertemplate="<b>Tipo:</b> %{x}<br><b>Quantidade:</b> %{y}" # Personalizando o hovertemplate
     )
    st.plotly_chart(fig)

def exibir_principais_setores(df, top_n=10):
    """
        Exibe os principais setores que abriram mais O.S. em um gráfico de barras.
        Args:
            df (pd.DataFrame): DataFrame contendo os dados.
            top_n (int): Número de setores a serem exibidos.
    """
    if df.empty:
        st.warning("Não há dados para exibir o gráfico de setores.")
        return

    setores_contagem = df['SETOR_ATENDIMENTO'].value_counts().nlargest(top_n).reset_index()
    setores_contagem.columns = ['SETOR_ATENDIMENTO', 'QUANTIDADE']

    num_setores = len(setores_contagem)
    
    # Gerar cores aleatórias, agora em loop, para garantir que sempre terá uma cor para cada item.
    color_scale = pc.qualitative.Set1  # Selecione a paleta de cores que preferir
    color_list = []
    for i in range(num_setores):
        color_list.append(color_scale[i % len(color_scale)])


    color_map = dict(zip(setores_contagem['SETOR_ATENDIMENTO'], color_list))
    
    fig = px.bar(setores_contagem, 
                 x='SETOR_ATENDIMENTO', 
                 y='QUANTIDADE', 
                 title=f'Top {top_n} Setores com Mais O.S',
                 color = 'SETOR_ATENDIMENTO',
                 color_discrete_map = color_map,
                 text_auto=True
                )
        
    fig.update_layout(
        legend_title_text=" ",
         margin=dict(l=20, r=20, t=60, b=20),
         #template="plotly_dark", # Paletas de cores que o plotly oferece, pode usar: "plotly_dark" ou "plotly"
        title_font=dict(size=17),
    )
    fig.update_xaxes(title_text='')  # Remover o rótulo do eixo x
    fig.update_yaxes(title_text='')  # Remover o rótulo do eixo y
    
    fig.update_traces(
      hovertemplate="<b>Setor:</b> %{x}<br><b>Quantidade:</b> %{y}"  # Personalizando o hovertemplate
    )
    
    st.plotly_chart(fig)
    

def calcular_custo_materiais(df):
    """Calcula o custo total e a quantidade de requisições de materiais."""
    if df.empty:
        return 0,0

    #valores ficticios para testes:
    custo_total = 5300.97
    quantidade_requisicoes = 34


    #para calculo real:
    #custo_total = df['CUSTO_MATERIAL'].sum() # supondo que você tenha essa coluna
    #quantidade_requisicoes = len(df) #para quantidade de requisições

    return custo_total, quantidade_requisicoes

def preparar_download_excel(df, filename="dados.xlsx"):
    """Converte um DataFrame em um arquivo Excel na memória para download."""
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, sheet_name='Sheet1', index=False)
    return output.getvalue()

logo_path = 'HSF_LOGO_-_1228x949_001.png'

if __name__ == "__main__":
    print(f'__main__')
    locale.setlocale(locale.LC_ALL, 'pt_BR.utf8')
    st.logo(logo_path,size="large")

    st.write('# Analítico SLA - Ordem de Serviço')
    
    st.write('Rel 1618 - HSF - Analítico SLA - Ordem de Serviço (EXCEL)')

    df_rel_1618 = obter_dados_relatorio_1618()
    # Tratamento de valores null:
    df_rel_1618 = df_rel_1618.fillna('-')

    # Tratamento de Formatação de Números
    for col in ['NR_ORDEM', 'ANO']:
        if col in df_rel_1618.columns:
            df_rel_1618[col] = df_rel_1618[col].apply(lambda x: f"{x:.0f}" if pd.notnull(x) else x)

    # tratamento do valor com .0:
    df_rel_1618['META_SLA'] = df_rel_1618['META_SLA'].astype(str).str.replace('.0', '')
    
######################################################################################################################
    with st.sidebar:
        # Obtendo a lista de anos distintos
        anos_distintos = sorted(df_rel_1618['ANO'].unique(), reverse=True)
        # Filtra os anos, mantendo apenas os iguais ou superiores a 2022
        anos_distintos = [ano for ano in anos_distintos if int(ano) >= 2022]
        anos_distintos = anos_distintos[:6]
        
        
        # Inicializa o ano mais recente
        if 'ano_selecionado' not in st.session_state:
            st.session_state['ano_selecionado'] = anos_distintos[0] if anos_distintos else None
        
        st.write("---")
        
        # Cria os botões para selecionar o ano
        if anos_distintos:
            st.session_state['ano_selecionado'] = st.selectbox("Selecione o Ano", anos_distintos)
        else:
            st.warning("Não há dados para exibir os filtros de anos.")
        
        # Filtrando o Data Frame pelo ano selecionado
        if st.session_state['ano_selecionado'] is not None:
            df_filtered_ano = df_rel_1618[df_rel_1618['ANO'] == st.session_state['ano_selecionado']]
        else:
            df_filtered_ano = df_rel_1618.copy()
        
        # Obtendo a lista de meses distintos para o ano selecionado
        meses_distintos = sorted(df_filtered_ano['OS_MES'].unique())
    
        # Inicializa o mês selecionado, usando o primeiro mês disponível
        if 'mes_selecionado' not in st.session_state:
            st.session_state['mes_selecionado'] = meses_distintos[0] if meses_distintos else None
        
        # Criando os botões para selecionar o mês
        if meses_distintos:
             #inserido botao de todos e botoes para cada mes
             meses_nomes = ["Todos"] + [datetime.date(1900, int(mes), 1).strftime('%B') for mes in meses_distintos]
             st.session_state['mes_selecionado'] = st.selectbox("Selecione o Mês", meses_nomes)
             
             if st.session_state['mes_selecionado'] == "Todos":
                st.session_state['mes_selecionado'] = None
             else:
                try:
                    mes_selecionado_os_mes = meses_distintos[meses_nomes.index(st.session_state['mes_selecionado']) - 1]
                    st.session_state['mes_selecionado'] = mes_selecionado_os_mes
                except ValueError:
                    pass
        else:
            st.warning("Não há dados para exibir os filtros de meses.")
######################################################################################################################
    st.write("---")
    
    # Filtrando o data frame pelo mes selecionado
    if st.session_state['mes_selecionado'] is None:
        df_filtered_mes = df_filtered_ano
    else:
        df_filtered_mes = df_filtered_ano[df_filtered_ano['OS_MES'] == st.session_state['mes_selecionado']]
        print(f'\nElse do st.session_state:')
        print(f"Ano selecionado: {st.session_state['ano_selecionado']}")
        print(f"Nome do mes selecionado: {datetime.date(1900, int(st.session_state['mes_selecionado']), 1).strftime('%B')}")

    # Calculo de Indicadores
    indicadores_calc = calcular_indicadores(df_filtered_mes)
    print('\n===============================================\n')
    print(f'indicadores_calc: \n{indicadores_calc}')
    print('\n===============================================\n')

    
    
    
    # Colunas para exibir os indicadores
    col1, col2, col3, col4 = st.columns(4)

    # Exibição dos Indicadores
    with col1:
        st.metric("Total de Ordens", value=indicadores_calc["total_ordens"])

    with col2:
        st.metric("Ordens no SLA",
                  value=f'{indicadores_calc["total_ordens_no_sla"]} ({indicadores_calc["percentual_ordens_no_sla"]:.2f}%)')

    with col3:
        st.metric("Ordens Fora do SLA",
                  value=f'{indicadores_calc["total_ordens_fora_sla"]} ({indicadores_calc["percentual_ordens_fora_sla"]:.2f}%)')

    with col4:
         st.metric("Tempo Médio de Atendimento", value=f'{indicadores_calc["media_tempo_total_em_horas"]:.2f} horas')
    
    st.write("---")

    #Exibir graficos:
    col5, col6, col7 = st.columns(3)

    with col5:
        # Chamando o Grafico de Pizza
        exibir_grafico_pizza(df_filtered_mes)

    with col6:
        # Chamando o Gráfico de Barras com Tempo por prioridade:
        exibir_grafico_barras_tempo_prioridade(indicadores_calc)

    with col7:
        # Chamando o Gráfico de tipo de OS:
        exibir_tipos_os(df_filtered_mes)


    st.write("---")
    # Exibindo os principais setores, segregação por tipo de setor, Calculo de horas por seto
    #col8, colVazio0 = st.columns(2)
    #with col8:  
    #    exibir_principais_setores(df_filtered_mes)
    exibir_principais_setores(df_filtered_mes)
    st.write("---")
    col10, col11, colHOMEM_X_HORA = st.columns(3)
    
    
        
    with col10:
        horas_por_setor = calcular_horas_por_setor(df_filtered_mes)
        if not horas_por_setor.empty:
            st.subheader("Horas Gastas por Setor:")
            horas_por_setor['HORAS_FORMATADA'] = horas_por_setor['HORAS'].apply(formatar_horas)
            horas_por_setor = horas_por_setor[['SETOR_ATENDIMENTO', 'HORAS_FORMATADA']].rename(
                    columns={
                                'SETOR_ATENDIMENTO': 'Setor',
                                'HORAS_FORMATADA': 'Horas'
                            }
            )
            st.dataframe(horas_por_setor, hide_index=True, use_container_width=True)
        else:
            st.warning("Não há dados para exibir as horas por setor.")  
    with col11:  
        # Calculo de Horas por analista:
        horas_por_analista = calcular_horas_por_analista(df_filtered_mes)
        if not horas_por_analista.empty:
            st.subheader("Horas Gastas por Analista")
            horas_por_analista = formatar_horas_df(horas_por_analista)
            horas_por_analista = horas_por_analista[['ANALISTA', 'N_OS', 'HORAS_FORMATADA']].rename(
                columns={
                            'ANALISTA': 'Analista', 
                            'N_OS': 'Nº O.S.', 
                            'HORAS_FORMATADA': 'Horas'
                        }
            )
            st.dataframe(horas_por_analista, hide_index=True, use_container_width=True)
            print(f"DF horas_por_analista: \n{horas_por_analista.head(5)}")
        else:
            st.warning("Não há dados para exibir as horas por analista.")
    with colHOMEM_X_HORA:
        #TODO: exibir KPI de HOMEM X HORA
        homem_hora = calcular_homem_hora(df_filtered_mes)
        st.subheader("HOMEM X HORA:")
        st.write(f"{homem_hora:.2f} Horas/Homem")

    # Principais Motivos para Abertura de OS:

     # Custo por requisição de materiais:
    st.write("---")
    
    colCustos, colVazio0 = st.columns(2)
            
    with colCustos:
        custo_total, quantidade_requisicoes = calcular_custo_materiais(df_filtered_mes)
        st.subheader("Custo por Solicitação de Materiais")
        st.write(f"Valor Total: R$ {custo_total:.2f}")
        st.write(f"Quantidade de Requisições: {quantidade_requisicoes}")
        


    # Criar uma nova linha abaixo dos indicadores para o botão de download
    st.write("---")
    st.write('## Data Frame completo das O.S.:')
    
    #Renomeando colunas antes de exibir o data frame:
    df_filtered_mes = df_filtered_mes[
                                        [
                                            'NR_ORDEM', 'ANALISTA', 'ANO','DATA','DS_PRIORIDADE', 
                                            'TEMPO_TOTAL','DT_INICIO_REAL', 'DT_FIM_REAL', 'SLA',
                                            'SETOR_ATENDIMENTO','STATUS_ORDEM'
                                        ]
                                    ].rename(columns={
                                                    'NR_ORDEM' : ' Nº O.S.', 
                                                    'ANALISTA' : 'Analista', 
                                                    'ANO' : 'Ano', 
                                                    'DATA' : 'Data', 
                                                    'DS_PRIORIDADE' : 'Prioridade',
                                                    'TEMPO_TOTAL' : 'Minutos', 
                                                    'DT_INICIO_REAL' : 'Início', 
                                                    'DT_FIM_REAL' : 'Fim', 
                                                    'SLA' : 'SLA',
                                                    'SETOR_ATENDIMENTO' : 'Setor',
                                                    'STATUS_ORDEM' : 'Status'
                                                    })
    #st.dataframe(df_filtered_mes, hide_index=True, use_container_width=True)
    
    # Estilo do DataFrame
    df_styled = df_filtered_mes.style.applymap(sla_cor_status, subset=['SLA'])

    # Exibindo o dataframe:
    st.dataframe(df_styled, hide_index=True, height=680, use_container_width=True)

    # Disponibilizar o botão de download
    download_xlsx = preparar_download_excel(df_filtered_mes)
    st.download_button(
        label="Download em XLSX",
        data=download_xlsx,
        file_name='dados_sla.xlsx',
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    # Criar uma nova linha abaixo do botão de download
    st.write("---")