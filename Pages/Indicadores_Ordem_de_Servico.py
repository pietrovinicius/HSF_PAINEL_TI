# 03/01/2025
# @PLima
# HFS - PAINEL DE DIVERSOS DADOS E INDICADORES
# Indicadores de Ordem de Servico
# RELATORIO 1507 - HSF - Indicadores Ordem de Servico

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
st.set_page_config(layout="wide", initial_sidebar_state="expanded",
                   page_title="Indicadores Ordem de Servico")

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
def REL_1507_Banda_Geral_Tipo_OS():
    try:
        # Chamar a função para obter o caminho do Instant Client
        caminho_instantclient = encontrar_diretorio_instantclient()

        # Usar o caminho encontrado para inicializar o Oracle Client
        if caminho_instantclient:
            oracledb.init_oracle_client(lib_dir=caminho_instantclient)
        else:
            st.error("Erro ao localizar o Instant Client. Verifique o nome da pasta e o caminho.")
            return pd.DataFrame()
        
        connection = oracledb.connect(user="TASY", password="aloisk", dsn="192.168.5.9:1521/TASYPRD")
        
        with connection.cursor() as cursor:
            sql = """
                             
                    SELECT 
                        5 AS ORDEM, 
                        'HSF-GERAL-TIPO' AS LOCAL, 
                        EXTRACT(YEAR FROM MOSA.DT_ATIVIDADE) AS ANO,
                        EXTRACT(MONTH FROM MOSA.DT_ATIVIDADE) AS MES,
                        TO_CHAR(MOSA.DT_ATIVIDADE, 'Month') AS MES_TEXTO,
                        DECODE(ATP.IE_STATUS_ORDEM, 1, 'Aberta', 2, 'Processo', 3, 'Encerrada') AS STATUS,
                        COUNT(DISTINCT ATP.NR_SEQUENCIA) AS ORDEM_SERVICO_TOTAL,
                        SUM(MOSA.QT_MINUTO) AS MINUTOS_TOTAL, 
                        ROUND(SUM(MOSA.QT_MINUTO) / 60) AS HORAS_TOTAL,
                        LPAD(FLOOR((SUM(MOSA.QT_MINUTO) / 60) / COUNT(DISTINCT ATP.NR_SEQUENCIA)), 2, '0') AS HORA_HOMEM,
                        RPAD(MOD(ROUND(SUM(MOSA.QT_MINUTO) / COUNT(DISTINCT ATP.NR_SEQUENCIA)), 60), 2, '0') AS MINUTOS_HOMEM,
                        LPAD(FLOOR((SUM(MOSA.QT_MINUTO) / 60) / COUNT(DISTINCT ATP.NR_SEQUENCIA)), 2, '0') || ' horas e ' ||
                        RPAD(MOD(ROUND(SUM(MOSA.QT_MINUTO) / COUNT(DISTINCT ATP.NR_SEQUENCIA)), 60), 2, '0') || ' minutos' AS HORAS_MINUTOS_HOMEM,
                        MTOS.DS_TIPO AS TIPO,
                        MGP.DS_GRUPO_PLANEJ AS GRUPO_PLANEJAMENTO
                    FROM	MAN_ORDEM_SERVICO ATP
                    INNER JOIN MAN_GRUPO_TRABALHO SA ON SA.NR_SEQUENCIA = ATP.NR_GRUPO_TRABALHO
                    INNER JOIN MAN_LOCALIZACAO ML ON ML.NR_SEQUENCIA = ATP.NR_SEQ_LOCALIZACAO
                    INNER JOIN SETOR_ATENDIMENTO SAT ON SAT.CD_SETOR_ATENDIMENTO = ML.CD_SETOR
                    INNER JOIN MAN_GRUPO_PLANEJAMENTO MGP ON MGP.NR_SEQUENCIA = ATP.NR_GRUPO_PLANEJ
                    INNER JOIN MAN_ORDEM_SERV_ATIV MOSA ON MOSA.NR_SEQ_ORDEM_SERV = ATP.NR_SEQUENCIA
                    INNER JOIN MAN_TIPO_ORDEM_SERVICO MTOS ON MTOS.NR_SEQUENCIA = ATP.NR_SEQ_TIPO_ORDEM
                    WHERE MOSA.DT_ATIVIDADE IS NOT NULL
                    AND MGP.NR_SEQUENCIA = 22 
                    GROUP BY  MOSA.DT_ATIVIDADE, ATP.IE_STATUS_ORDEM, MTOS.DS_TIPO, MGP.DS_GRUPO_PLANEJ
                    ORDER BY 
                        EXTRACT(YEAR FROM MOSA.DT_ATIVIDADE) DESC,
                        EXTRACT(MONTH FROM MOSA.DT_ATIVIDADE) DESC,
                        MTOS.DS_TIPO ASC
                    """
            cursor.execute(sql)
            results = cursor.fetchall()
            df = pd.DataFrame(results, columns=[desc[0] for desc in cursor.description])
            st.success("Gerado as: " + obter_timestamp_atual())
            return df
    except oracledb.Error as e:
        st.error(f"Erro no Oracle: {e}. {obter_timestamp_atual()}")
        return pd.DataFrame()  # Retorna um DataFrame vazio em caso de erro
    except Exception as erro:
        st.error(f"Erro Inesperado: {erro}. {obter_timestamp_atual()}")
        return pd.DataFrame()  # Retorna um DataFrame vazio em caso de erro


@st.cache_data
def REL_1507_Banda_Setor():
    try:
        # Chamar a função para obter o caminho do Instant Client
        caminho_instantclient = encontrar_diretorio_instantclient()

        # Usar o caminho encontrado para inicializar o Oracle Client
        if caminho_instantclient:
           oracledb.init_oracle_client(lib_dir=caminho_instantclient)
        else:
            st.error("Erro ao localizar o Instant Client. Verifique o nome da pasta e o caminho.")
            return pd.DataFrame()

        connection = oracledb.connect(user="TASY", password="aloisk", dsn="192.168.5.9:1521/TASYPRD")

        with connection.cursor() as cursor:
            sql = """              
                    SELECT
                        4 AS ORDEM,
                        SAT.DS_SETOR_ATENDIMENTO AS LOCAL,
                        --LAST_DAY(TRUNC(MOSA.DT_ATIVIDADE)) AS DATA,
                        EXTRACT(YEAR FROM MOSA.DT_ATIVIDADE) AS ANO,
                        EXTRACT(MONTH FROM MOSA.DT_ATIVIDADE) AS MES,
                        TO_CHAR(MOSA.DT_ATIVIDADE, 'Month') AS MES_TEXTO,
                        DECODE(ATP.IE_STATUS_ORDEM, 1, 'Aberta', 2, 'Processo', 3, 'Encerrada') AS STATUS,
                        COUNT(DISTINCT ATP.NR_SEQUENCIA) AS ORDEM_SERVICO_TOTAL,
                        SUM(MOSA.QT_MINUTO) AS MINUTOS_TOTAL,
                        ROUND(SUM(MOSA.QT_MINUTO) / 60) AS HORAS_TOTAL,
                        LPAD(FLOOR((SUM(MOSA.QT_MINUTO) / 60) / COUNT(DISTINCT ATP.NR_SEQUENCIA)), 2, '0') AS HORA_HOMEM,
                        RPAD(MOD(ROUND(SUM(MOSA.QT_MINUTO) / COUNT(DISTINCT ATP.NR_SEQUENCIA)), 60), 2, '0') AS MINUTOS_HOMEM,
                        LPAD(FLOOR((SUM(MOSA.QT_MINUTO) / 60) / COUNT(DISTINCT ATP.NR_SEQUENCIA)), 2, '0') || ' horas e ' ||
                        RPAD(MOD(ROUND(SUM(MOSA.QT_MINUTO) / COUNT(DISTINCT ATP.NR_SEQUENCIA)), 60), 2, '0') || ' minutos' AS HORAS_MINUTOS_HOMEM
                    FROM    MAN_ORDEM_SERVICO ATP
                    INNER JOIN    MAN_GRUPO_TRABALHO SA ON SA.NR_SEQUENCIA = ATP.NR_GRUPO_TRABALHO
                    INNER JOIN    MAN_LOCALIZACAO ML ON ML.NR_SEQUENCIA = ATP.NR_SEQ_LOCALIZACAO
                    INNER JOIN    SETOR_ATENDIMENTO SAT ON SAT.CD_SETOR_ATENDIMENTO = ML.CD_SETOR
                    LEFT JOIN    MAN_GRUPO_PLANEJAMENTO MGP ON MGP.NR_SEQUENCIA = ATP.NR_GRUPO_PLANEJ
                    LEFT JOIN    MAN_ORDEM_SERV_ATIV MOSA ON MOSA.NR_SEQ_ORDEM_SERV = ATP.NR_SEQUENCIA
                    LEFT JOIN    MAN_TIPO_ORDEM_SERVICO MTOS ON MTOS.NR_SEQUENCIA = ATP.NR_SEQ_TIPO_ORDEM
                    --WHERE    TRUNC(MOSA.DT_ATIVIDADE) BETWEEN sysdate - 365 AND sysdate --:DT_INICIAL AND :DT_FINAL
                    --PROTECAO DE VALORES NULL
                    WHERE MOSA.DT_ATIVIDADE IS NOT NULL
                    GROUP BY
                        SAT.DS_SETOR_ATENDIMENTO,
                        LAST_DAY(TRUNC(MOSA.DT_ATIVIDADE)),
                        TO_CHAR(MOSA.DT_ATIVIDADE, 'Month'),
                        EXTRACT(YEAR FROM MOSA.DT_ATIVIDADE),
                        EXTRACT(MONTH FROM MOSA.DT_ATIVIDADE),
                        ATP.IE_STATUS_ORDEM
                    ORDER BY
                        SAT.DS_SETOR_ATENDIMENTO ASC,
                        EXTRACT(YEAR FROM MOSA.DT_ATIVIDADE) DESC,
                        EXTRACT(MONTH FROM MOSA.DT_ATIVIDADE) ASC
                    """
            cursor.execute(sql)
            results = cursor.fetchall()
            df = pd.DataFrame(results, columns=[desc[0] for desc in cursor.description])
            st.success("Gerado as: " + obter_timestamp_atual())
            return df
    except oracledb.Error as e:
       st.error(f"Erro no Oracle: {e}. {obter_timestamp_atual()}")
       return pd.DataFrame()
    except Exception as erro:
        st.error(f"Erro Inesperado: {erro}. {obter_timestamp_atual()}")
        return pd.DataFrame()

def calcular_indicadores(df):
    """Calcula os indicadores de SLA, extrai status e tipos distintos."""
    print(f'\n*****Calcula os indicadores de SLA, extrai status e tipos distintos')
    if df.empty:
        return {
            "total_ordens": 0,
            "total_ordens_Encerrada": 0,
            "total_ordens_Processo": 0,
            "status_distintos": [],
            "tipos_distintos": []
        }

    total_ordens = len(df)
    total_ordens_Encerrada = len(df[df['STATUS'] == 'Encerrada'])
    total_ordens_Processo = len(df[df['STATUS'] == 'Processo'])
    
    # Extraindo distintos
    status_distintos = df['STATUS'].unique().tolist()
    print(f"Status Distintos: {status_distintos}")
    
    tipos_distintos = df['TIPO'].unique().tolist()
    print(f'Tipos Distintos: {tipos_distintos}')

    # Contagem de ordens por tipo
    contagem_por_tipo = df['TIPO'].value_counts().to_dict()
    print(f'contagem_por_tipo: {contagem_por_tipo}')
    
    # Desestruturando a contagem por tipo em variáveis
    Corretiva = contagem_por_tipo.get('Corretiva', 0)
    Ronda_Inspecao = contagem_por_tipo.get('Ronda/Inspeção', 0)
    Cadastro = contagem_por_tipo.get('Cadastro', 0)
    Suporte = contagem_por_tipo.get('Suporte', 0)
    print(f'Corretiva: {Corretiva}')
    print(f'Ronda_Inspecao: {Ronda_Inspecao}')
    print(f'Cadastro: {Cadastro}')
    print(f'Suporte: {Suporte}')


    return {
        "total_ordens": total_ordens,
        "total_ordens_Encerrada": total_ordens_Encerrada,
        "total_ordens_Processo": total_ordens_Processo,
        "tipos_distintos": tipos_distintos,
        "contagem_por_tipo": contagem_por_tipo,
        "Corretiva": Corretiva,
        "Ronda_Inspecao": Ronda_Inspecao,
        "Cadastro": Cadastro,
        "Suporte": Suporte
    }

def formatar_horas(horas):
    """Formata as horas para o formato 'X horas Y minutos'."""
    horas_int = int(horas)
    minutos = int((horas - horas_int) * 60)
    return f"{horas_int} hora(s) {minutos:02} minuto(s)"

def calcular_homem_hora(df):
    """Calcula o indicador Homem x Hora."""
    if df.empty:
        return 0  # Retorna 0 se não houver dados

    total_minutos = df['MINUTOS_TOTAL'].astype(int).sum()
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

    df['HORAS_FORMATADA'] = df['HORAS_TOTAL'].apply(formatar_horas_individual)
    df = df.drop('HORAS_TOTAL', axis=1)
    return df

def preparar_download_excel(df, filename="dados.xlsx"):
    """Converte um DataFrame em um arquivo Excel na memória para download."""
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, sheet_name='Sheet1', index=False)
    return output.getvalue()

def exibir_grafico_barras_tempo_prioridade(indicadores_calc):
    """Exibe o gráfico de barras do tempo médio por prioridade."""
    if indicadores_calc["media_tempo_por_prioridade"].empty:
        st.warning("Não há dados para exibir o gráfico de barras de tempo por prioridade.")
        return
    
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
    
    fig.update_xaxes(title_text='Prioridade')  # Alterar o rótulo do eixo x
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

    setores_contagem = df['LOCAL'].value_counts().nlargest(top_n).reset_index()
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
logo_path = 'HSF_LOGO_-_1228x949_001.png'

if __name__ == "__main__":
    print('Indicadores de Ordem de Servico')
    st.logo(logo_path,size="large")
    try:
        st.write('# Indicadores de Ordem de Servico')
        
########################################################################################
        with st.sidebar:
            # Obtendo a lista de anos distintos
            df_rel_1507_Banda_Geral_Tipo_OS = REL_1507_Banda_Geral_Tipo_OS()
            anos_distintos = sorted(df_rel_1507_Banda_Geral_Tipo_OS['ANO'].unique(), reverse=True)
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
                df_filtered_ano = df_rel_1507_Banda_Geral_Tipo_OS[df_rel_1507_Banda_Geral_Tipo_OS['ANO'] == st.session_state['ano_selecionado']]
            else:
                df_filtered_ano = df_rel_1507_Banda_Geral_Tipo_OS.copy()
        
            # Obtendo a lista de meses distintos para o ano selecionado
            meses_distintos = sorted(df_filtered_ano['MES'].unique())
            
            # Obtendo a lista de meses por extenso
            meses_textos_distintos = sorted(df_filtered_ano['MES_TEXTO'].unique())
            print(f'meses_textos_distintos:\n{meses_textos_distintos}')
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
                         # Use a lista de meses por extenso para encontrar o mes selecionado
                         mes_selecionado_os_mes = meses_distintos[meses_nomes.index(st.session_state['mes_selecionado']) - 1]
                         st.session_state['mes_selecionado'] = mes_selecionado_os_mes
                     except ValueError:
                         pass
            else:
                st.warning("Não há dados para exibir os filtros de meses.")
########################################################################################

        st.write("---")
        st.write('## Banda Geral Tipo O.S.:')
        
        #Geracao de Data Frame:
        df_rel_1507_Banda_Geral_Tipo_OS = REL_1507_Banda_Geral_Tipo_OS()
        
        #Tratamento de valores null:
        df_rel_1507_Banda_Geral_Tipo_OS = df_rel_1507_Banda_Geral_Tipo_OS = df_rel_1507_Banda_Geral_Tipo_OS.fillna('-')
        
         # Filtrando o data frame pelo ano selecionado
        if st.session_state['ano_selecionado'] is not None:
            df_rel_1507_Banda_Geral_Tipo_OS = df_rel_1507_Banda_Geral_Tipo_OS[df_rel_1507_Banda_Geral_Tipo_OS['ANO'] == st.session_state['ano_selecionado']]
        
        # Filtrando o data frame pelo mes selecionado
        if st.session_state['mes_selecionado'] is not None:
            df_rel_1507_Banda_Geral_Tipo_OS = df_rel_1507_Banda_Geral_Tipo_OS[df_rel_1507_Banda_Geral_Tipo_OS['MES'] == st.session_state['mes_selecionado']]
        
        #tratamento de valores com casa decimal:
        df_rel_1507_Banda_Geral_Tipo_OS['ANO'] = df_rel_1507_Banda_Geral_Tipo_OS['ANO'].apply(lambda x: "{:.0f}".format(x))
        #df_rel_1507_Banda_Geral_Tipo_OS['MINUTOS_TOTAL'] = df_rel_1507_Banda_Geral_Tipo_OS['MINUTOS_TOTAL'].apply(lambda x: "{:.0f}".format(x))
        
        st.write("---")  # Linha separadora
        
        #TODO: adicionar cartoes de indicadores:
        
        # Calculo de Indicadores
        indicadores_calc = calcular_indicadores(df_rel_1507_Banda_Geral_Tipo_OS)
        print('\n===============================================\n')
        print(f'indicadores_calc: \n{indicadores_calc}')
        print('\n===============================================\n')
        
        st.metric("Total de Ordens de Serviço", value=indicadores_calc["total_ordens"])
        print(f'Total de Ordens: {indicadores_calc["total_ordens"]}')
        
        st.write("---")  # Linha separadora
        #Status label:
        colStatus , colTipo = st.columns(2)
        with colStatus:
            st.write('### Status:')
        with colTipo:
            st.write('### Tipos:')
                
        col10 , col20 , col30 , col40 , col50 , col60 , col70 , col80 = st.columns(8)
        #Tipos de OS:
        with col10:
            st.metric("Encerradas", value=indicadores_calc["total_ordens_Encerrada"])
            print(f'Encerradas: {indicadores_calc["total_ordens_Encerrada"]}')
        with col20:
            st.metric(f'Processo', value=indicadores_calc["total_ordens_Processo"])
            print(f'Processo: {indicadores_calc["total_ordens_Processo"]}')
        with col30:
            st.write("")
        with col40:
            st.write("")
        #Tipos de OS:
        with col50:
            st.metric("Cadastro", value=indicadores_calc["Cadastro"])
            print(f'col4 Cadastro: {indicadores_calc["Cadastro"]}')
        with col60:
            st.metric("Corretiva", value=indicadores_calc["Corretiva"])
            print(f'col5 Corretiva: {indicadores_calc["Corretiva"]}')
        with col70:
            st.metric("Ronda / Inspeção", value=indicadores_calc["Ronda_Inspecao"])
            print(f'col6 Ronda_Inspecao: {indicadores_calc["Ronda_Inspecao"]}')
        with col80:
            st.metric("Suporte", value=indicadores_calc["Suporte"])
            print(f'col7 Suporte: {indicadores_calc["Suporte"]}')
        
        
        #TODO: inserir grafico de pizza
        
        
        st.write("---")  # Linha separadora
        st.subheader("Geral por tipo de O.S.:")
        st.dataframe(df_rel_1507_Banda_Geral_Tipo_OS,hide_index=True, use_container_width=True)
        
        # Disponibilizar o botão de download
        download_xlsx = preparar_download_excel(df_rel_1507_Banda_Geral_Tipo_OS)
        st.download_button(
            label="Download em XLSX",
            data=download_xlsx,
            file_name='dados_sla.xlsx',
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
        
        # Criar uma nova linha abaixo dos indicadores para o botão de download
        st.write("---")  # Linha separadora
        
    except Exception as err: 
        print(f"Inexperado:\n {err=}, {type(err)=}")