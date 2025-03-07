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
import time
import re

#
#✅ Ícones para Status Gerais
#✅ ✅ Concluído
#⚠️ ⚠️ Atenção
#❌ ❌ Erro / Falha
#🚀 🚀 Em Progresso
#📌 📌 Importante
#🏆 🏆 Meta Atingida
#🔄 🔄 Atualizando
#🔥 🔥 Urgente
#🕒 🕒 Tempo Decorrido
#📊 📊 Relatório
#📈 📈 Aumento
#📉 📉 Queda
#🏥 🏥 Hospital
#


# Configuração da página Streamlit
st.set_page_config(layout="wide", initial_sidebar_state="collapsed", page_title="Indicadores Ordem de Servico")

# Aumentando exibição do DataFrame no Streamlit
pd.set_option("styler.render.max_elements", 1249090)

def obter_timestamp_atual():
    """Retorna o timestamp atual no formato YYYY-MM-DD HH-MM-SS."""
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

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
                    
def formatar_horas(horas):
    """Formata as horas para o formato 'X horas Y minutos'."""
    horas_int = int(horas)
    minutos = int((horas - horas_int) * 60)
    return f"{horas_int} hora(s) {minutos:02} minuto(s)"

def formatar_ano_dia_mes_vazios(valor):
     try:
         return "{:.0f}".format(float(valor))  # Tenta converter para float e formatar
     except (ValueError, TypeError):
         return ""  # Retorna string vazia em caso de erro

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

def executar_query_oracle(sql):
    """
    Executa uma query no Oracle, gerencia a conexão e retorna um DataFrame.
    """
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
            cursor.execute(sql)
            results = cursor.fetchall()
            df = pd.DataFrame(results, columns=[desc[0] for desc in cursor.description])
            return df
    except oracledb.Error as e:
        st.error(f"\nexecutar_query_oracle(sql)\noracledb.Error: {e}. {obter_timestamp_atual()}")
        return pd.DataFrame()
    except Exception as erro:
        st.error(f"\nexecutar_query_oracle(sql)\nException: {erro}. {obter_timestamp_atual()}")
        return pd.DataFrame()

#@st.cache_data
def REL_1507_Banda_Geral_Tipo_OS():
    sql = """
            --banda Geral / Tipo O.S.:    
            SELECT 
                ATP.NR_SEQUENCIA AS ORDEM_SERVICO,
                nvl(ABREVIA_NOME(INITCAP(OBTER_NOME_USUARIO(MOSA.NM_USUARIO_EXEC)),'A'),'') AS ANALISTA,
                NVL(
                    EXTRACT(YEAR FROM MOSA.DT_ATIVIDADE),
                    EXTRACT(YEAR FROM ATP.DT_ORDEM_SERVICO)
                ) AS ANO,
                NVL(
                    EXTRACT(MONTH FROM MOSA.DT_ATIVIDADE),
                    EXTRACT(MONTH FROM ATP.DT_ORDEM_SERVICO)
                ) AS MES,
                NVL(
                    TO_CHAR(MOSA.DT_ATIVIDADE, 'Month'),
                    TO_CHAR(ATP.DT_ORDEM_SERVICO, 'Month')
                ) AS MES_TEXTO,
                DECODE(ATP.IE_STATUS_ORDEM, 1, 'Aberta', 2, 'Processo', 3, 'Encerrada') AS STATUS,
                MTOS.DS_TIPO AS TIPO,
                
                COUNT(DISTINCT ATP.NR_SEQUENCIA) AS ORDEM_SERVICO_TOTAL,
                
                SUM(MOSA.QT_MINUTO) AS MINUTOS_TOTAL,
                
                ROUND(SUM(MOSA.QT_MINUTO) / 60) AS HORAS_TOTAL,
                LPAD(FLOOR((SUM(MOSA.QT_MINUTO) / 60) / COUNT(DISTINCT ATP.NR_SEQUENCIA)), 2, '0') AS HORA_HOMEM,
                RPAD(MOD(ROUND(SUM(MOSA.QT_MINUTO) / COUNT(DISTINCT ATP.NR_SEQUENCIA)), 60), 2, '0') AS MINUTOS_HOMEM,
                LPAD(FLOOR((SUM(MOSA.QT_MINUTO) / 60) / COUNT(DISTINCT ATP.NR_SEQUENCIA)), 2, '0') || ' horas e ' ||
                RPAD(MOD(ROUND(SUM(MOSA.QT_MINUTO) / COUNT(DISTINCT ATP.NR_SEQUENCIA)), 60), 2, '0') || ' minutos' AS HORAS_MINUTOS_HOMEM,
                MGP.DS_GRUPO_PLANEJ AS GRUPO_PLANEJAMENTO
            FROM	MAN_ORDEM_SERVICO ATP
            LEFT JOIN MAN_GRUPO_TRABALHO SA ON SA.NR_SEQUENCIA = ATP.NR_GRUPO_TRABALHO
            LEFT JOIN MAN_LOCALIZACAO ML ON ML.NR_SEQUENCIA = ATP.NR_SEQ_LOCALIZACAO
            LEFT JOIN SETOR_ATENDIMENTO SAT ON SAT.CD_SETOR_ATENDIMENTO = ML.CD_SETOR
            LEFT JOIN MAN_GRUPO_PLANEJAMENTO MGP ON MGP.NR_SEQUENCIA = ATP.NR_GRUPO_PLANEJ
            LEFT JOIN MAN_ORDEM_SERV_ATIV MOSA ON MOSA.NR_SEQ_ORDEM_SERV = ATP.NR_SEQUENCIA
            LEFT JOIN MAN_TIPO_ORDEM_SERVICO MTOS ON MTOS.NR_SEQUENCIA = ATP.NR_SEQ_TIPO_ORDEM
            WHERE EXTRACT(YEAR FROM ATP.DT_ORDEM_SERVICO) >= 2022
            AND MGP.NR_SEQUENCIA = 22
            GROUP BY 
                EXTRACT(YEAR FROM MOSA.DT_ATIVIDADE),
                EXTRACT(MONTH FROM MOSA.DT_ATIVIDADE),
                TO_CHAR(MOSA.DT_ATIVIDADE, 'Month'),
                ATP.IE_STATUS_ORDEM,
                MTOS.DS_TIPO,
                MGP.DS_GRUPO_PLANEJ,
                ATP.DT_ORDEM_SERVICO,
                ATP.NR_SEQUENCIA,
                MOSA.NM_USUARIO_EXEC
            ORDER BY 
                EXTRACT(YEAR FROM MOSA.DT_ATIVIDADE) DESC,
                EXTRACT(MONTH FROM MOSA.DT_ATIVIDADE) DESC,
                MTOS.DS_TIPO ASC,
                ATP.NR_SEQUENCIA DESC
            """
    df = executar_query_oracle(sql)
    if not df.empty:
         st.success("✅ Gerado às: " + obter_timestamp_atual())
         print(f"{obter_timestamp_atual()} - ✅ REL_1507_Banda_Geral_Tipo_OS - Gerado!" )
    return df

#@st.cache_data
def REL_1507_Banda_Geral_TP_OS_analitico():
    sql = """
             --banda Geral / Tipo O.S. ANALITICO:
            SELECT 
                ATP.NR_SEQUENCIA AS ORDEM_SERVICO,
                EXTRACT(YEAR FROM ATP.DT_ORDEM_SERVICO) AS ANO_ORDEM_SERVICO,
                EXTRACT(MONTH FROM ATP.DT_ORDEM_SERVICO) AS MES_ORDEM_SERVICO,
                EXTRACT(DAY FROM ATP.DT_ORDEM_SERVICO) AS DIA_ORDEM_SERVICO,
                TO_CHAR(ATP.DT_ORDEM_SERVICO, 'Month') AS MES__ORDEM_SERVICO_TEXTO,

                EXTRACT(YEAR FROM MOSA.DT_ATIVIDADE) AS ANO_ATIVIDADE,
                EXTRACT(MONTH FROM MOSA.DT_ATIVIDADE) AS MES_ATIVIDADE,
                EXTRACT(DAY FROM MOSA.DT_ATIVIDADE) AS DIA_ATIVIDADE,
                TO_CHAR(MOSA.DT_ATIVIDADE, 'Month') AS MES__ATIVIDADE_TEXTO,
                MTOS.DS_TIPO AS TIPO,
                DECODE(ATP.IE_STATUS_ORDEM, 1, 'Aberta', 2, 'Processo', 3, 'Encerrada') AS STATUS,
                DECODE(ATP.IE_PRIORIDADE, 'A', 'Alta', 'M', 'Média', 'E','Emergência', 'Fora da Prioridade') AS DS_PRIORIDADE, 
                ABREVIA_NOME( INITCAP(OBTER_NOME_USUARIO(MOSA.NM_USUARIO_EXEC)),'A') AS ANALISTA,
                MOSA.QT_MINUTO AS MINUTOS_TOTAL,
                MGP.DS_GRUPO_PLANEJ AS GRUPO_PLANEJAMENTO
            FROM	MAN_ORDEM_SERVICO ATP
            LEFT JOIN MAN_GRUPO_TRABALHO SA ON SA.NR_SEQUENCIA = ATP.NR_GRUPO_TRABALHO
            LEFT JOIN MAN_LOCALIZACAO ML ON ML.NR_SEQUENCIA = ATP.NR_SEQ_LOCALIZACAO
            LEFT JOIN SETOR_ATENDIMENTO SAT ON SAT.CD_SETOR_ATENDIMENTO = ML.CD_SETOR
            LEFT JOIN MAN_GRUPO_PLANEJAMENTO MGP ON MGP.NR_SEQUENCIA = ATP.NR_GRUPO_PLANEJ
            LEFT JOIN MAN_ORDEM_SERV_ATIV MOSA ON MOSA.NR_SEQ_ORDEM_SERV = ATP.NR_SEQUENCIA
            LEFT JOIN MAN_TIPO_ORDEM_SERVICO MTOS ON MTOS.NR_SEQUENCIA = ATP.NR_SEQ_TIPO_ORDEM
            --WHERE MOSA.DT_ATIVIDADE IS NOT NULL
            --AND MGP.NR_SEQUENCIA = 22 
            --AND ATP.DT_ORDEM_SERVICO BETWEEN SYSDATE - 8 AND SYSDATE
            --WHERE ATP.NR_SEQUENCIA = 158930
            --WHERE ATP.IE_STATUS_ORDEM = 1
            WHERE EXTRACT(YEAR FROM ATP.DT_ORDEM_SERVICO) >= 2022
            AND MGP.NR_SEQUENCIA = 22 -- TI
            ORDER BY 
                EXTRACT(YEAR FROM ATP.DT_ORDEM_SERVICO) DESC,
                EXTRACT(MONTH FROM ATP.DT_ORDEM_SERVICO) DESC,
                EXTRACT(DAY FROM ATP.DT_ORDEM_SERVICO),
                MTOS.DS_TIPO , MTOS.DS_TIPO
        """
    df = executar_query_oracle(sql)
    if not df.empty:
         #st.success("✅ Gerado às: " + obter_timestamp_atual())
         #print("✅ Gerado às: " + obter_timestamp_atual())
         print(f"{obter_timestamp_atual()} - ✅ REL_1507_Banda_Geral_TP_OS_analitico - Gerado!" )
    return df

def calcular_indicadores(df):
    """Calcula os indicadores de SLA, extrai status e tipos distintos."""
    #print(f'\n*****calcular_indicadores()')
    if df.empty:
        return {
            "total_ordens": 0
        }

    total_ordens = len(df)
    total_ordens_Aberta = len(df[df['STATUS'] == 'Aberta'])
    total_ordens_Encerrada = len(df[df['STATUS'] == 'Encerrada'])
    total_ordens_Processo = len(df[df['STATUS'] == 'Processo'])
    
    #print(f'======\ntotal_ordens: {total_ordens}\ntotal_ordens_Aberta: {total_ordens_Aberta}\ntotal_ordens_Encerrada: {total_ordens_Encerrada}\ntotal_ordens_Processo: {total_ordens_Processo}  \n=====\n\n')
    
    # Extraindo distintos
    status_distintos = df['STATUS'].unique().tolist()
    #print(f"Status Distintos: {status_distintos}")
    
    tipos_distintos = df['TIPO'].unique().tolist()
    #print(f'Tipos Distintos: {tipos_distintos}')

    # Contagem de ordens por tipo
    contagem_por_tipo = df['TIPO'].value_counts().to_dict()
    #print(f'\ncontagem_por_tipo: {contagem_por_tipo}')
    
    # Desestruturando a contagem por tipo em variáveis
    Corretiva = contagem_por_tipo.get('Corretiva', 0)
    Ronda_Inspecao = contagem_por_tipo.get('Ronda/Inspeção', 0)
    Cadastro = contagem_por_tipo.get('Cadastro', 0)
    Suporte = contagem_por_tipo.get('Suporte', 0)
    Relatorio = contagem_por_tipo.get('Relatório', 0)
    Desenvolvimento = contagem_por_tipo.get('Desenvolvimento', 0)
    
    #print(f'Corretiva: {Corretiva}')
    #print(f'Ronda_Inspecao: {Ronda_Inspecao}')
    #print(f'Cadastro: {Cadastro}')
    #print(f'Suporte: {Suporte}')
    #print(f'Relatório: {Relatorio}')
    #print(f'Desenvolvimento: {Desenvolvimento}')

    total_minutos = df['MINUTOS_TOTAL'].sum()
    #print(f"============================================================================================")
    
    #Precisei arredondar o número e depois o transformar em inteiro:
    #print(f"total_minutos: {int(round(total_minutos))}")
    total_horas = total_minutos // 60
    minutos_restantes = total_minutos % 60
    
    #Precisei arredondar o número e depois o transformar em inteiro:
    total_horas = str(int(round(total_horas))) + 'h'
    
    #Precisei arredondar o número e depois o transformar em inteiro:
    minutos_restantes = str(int(round(minutos_restantes))) + 'm'
    #print(f"Total: {total_horas}")
    #print(f"minutos_restantes: {minutos_restantes}")
    
    #print(f"============================================================================================\n")
    return {
        "total_ordens": total_ordens,
        "total_ordens_Aberta": total_ordens_Aberta,
        "total_ordens_Encerrada": total_ordens_Encerrada,
        "total_ordens_Processo": total_ordens_Processo,
        "tipos_distintos": tipos_distintos,
        "contagem_por_tipo": contagem_por_tipo,
        "Corretiva": Corretiva,
        "Ronda_Inspecao": Ronda_Inspecao,
        "Cadastro": Cadastro,
        "Suporte": Suporte,
        "Relatorio": Relatorio,
        "Desenvolvimento": Desenvolvimento,
        "total_horas": total_horas,
        "minutos_restantes": minutos_restantes
    } 
    
def calcular_indicadores_por_analista(df):
    #print(f"\n=====calcular_indicadores_por_analista")
    
    #Colunas:
    #Index(['ORDEM_SERVICO', 'ANO', 'MES', 'MES_TEXTO', 'TIPO', 'STATUS',
    #    'DS_PRIORIDADE', 'ANALISTA', 'MINUTOS_TOTAL', 'GRUPO_PLANEJAMENTO'],
    #    dtype='object')
    
    if df.empty:
       return {
           "total_atividades": 0
       }
    total_atividades = len(df)
    #print(f"total_atividades: {total_atividades}")
    
    # Extraindo distintos
    status_distintos = df['STATUS'].unique().tolist()
    #print(f"Status Distintos: {status_distintos}")
    
    tipos_distintos = df['TIPO'].unique().tolist()
    #print(f'Tipos Distintos: {tipos_distintos}')

    # Contagem de ordens por tipo
    contagem_por_tipo = df['TIPO'].value_counts().to_dict()
    #print(f'contagem_por_tipo: {contagem_por_tipo}')
    
    # Desestruturando a contagem por tipo em variáveis
    Corretiva = contagem_por_tipo.get('Corretiva', 0)
    Ronda_Inspecao = contagem_por_tipo.get('Ronda/Inspeção', 0)
    Cadastro = contagem_por_tipo.get('Cadastro', 0)
    Suporte = contagem_por_tipo.get('Suporte', 0)
    #print(f'Corretiva: {Corretiva}')
    #print(f'Ronda_Inspecao: {Ronda_Inspecao}')
    #print(f'Cadastro: {Cadastro}')
    #print(f'Suporte: {Suporte}')
    
    total_minutos = df['MINUTOS_TOTAL'].sum()
    #print(f"============================================================================================")
    #Precisei arredondar o número e depois o transformar em inteiro:
    #print(f"total_minutos: {int(round(total_minutos))}")
    total_horas = total_minutos // 60
    minutos_restantes = total_minutos % 60

    #Precisei arredondar o número e depois o transformar em inteiro:
    total_horas = str(int(round(total_horas))) + 'h'
    
    #Precisei arredondar o número e depois o transformar em inteiro:
    minutos_restantes = str(int(round(minutos_restantes))) + 'm'
    
    #print(f"Total Horas: {total_horas}")
    #print(f"minutos_restantes: {minutos_restantes}")

    # Obtendo os valores distintos da coluna 'ANALISTA'
    analistas_distintos = df['ANALISTA'].unique()

    # Exibindo os valores distintos em console
    #print(f"Valores distintos da coluna 'ANALISTA':\n{analistas_distintos}")
    # Agrupar por analista e somar os minutos
    analistas_minutos = df.groupby('ANALISTA')['MINUTOS_TOTAL'].sum()
    #print(f'analistas_minutos: {analistas_minutos}')
    # Dicionário para armazenar as horas por analista
    analistas_horas = {}

    for analista, total_minutos in analistas_minutos.items():
        # Converter minutos para horas e minutos
        total_horas_analista = total_minutos // 60
        minutos_restantes_analista = total_minutos % 60
        #Precisei arredondar o número e depois o transformar em inteiro:
        horas_minutos = f"{int(round(total_horas_analista))}h {int(round(minutos_restantes_analista))}m"
        analistas_horas[analista] = horas_minutos
        
    #print(f"*******\nAnalistas_horas: \n{analistas_horas}\n*******")

    #print(f"============================================================================================\n")
    return {
        "total_atividades": total_atividades,
        "tipos_distintos": tipos_distintos,
        "contagem_por_tipo": contagem_por_tipo,
        "Corretiva": Corretiva,
        "Ronda_Inspecao": Ronda_Inspecao,
        "Cadastro": Cadastro,
        "Suporte": Suporte,
        "total_horas": total_horas,
        "minutos_restantes": minutos_restantes,
         "Analistas_horas": analistas_horas
    }

def exibir_cartoes_analistas(analistas_horas):
    """Exibe cartões com as horas de atividades de cada analista, ordenado por horas (decrescente)."""
    #print(f'\nexibir_cartoes_analistas()')
    if not analistas_horas:
      st.warning("Não há dados para exibir os cartões dos analistas")
      st.empty()
      return

    num_colunas = 4  # Quantidade de colunas por linha
    
    # Converte o dicionário em uma lista de tuplas (analista, horas_minutos)
    analistas = list(analistas_horas.items())
    
    # Ordena a lista de analistas por quantidade de horas (decrescente)
    analistas_ordenados = sorted(analistas, 
                                key=lambda item: int(re.findall(r'\d+', item[1])[0]) if re.findall(r'\d+', item[1]) else 0,
                                reverse=True)
    
    #print(f'\n\n*****analistas_ordenados:\n{analistas_ordenados} \n\n')
    
    for i in range(0, len(analistas_ordenados), num_colunas):
        # Criar colunas para cada linha de cartões
        cols = st.columns(num_colunas)
        
        for j in range(num_colunas):
            if i + j < len(analistas_ordenados): # Garantir que nao pegue analista fora do range da lista
                analista, horas_minutos = analistas_ordenados[i + j]
                with cols[j]: # Adicionar cartao na coluna
                    st.metric(label=f"{analista}", value=f"{horas_minutos}")

def calcular_homem_hora(df):
    """Calcula o indicador Homem x Hora."""
    if df.empty:
        return 0  # Retorna 0 se não houver dados

    total_minutos = df['MINUTOS_TOTAL'].astype(int).sum()
    num_analistas = 8  # Número de analistas (fixo)
    homem_hora = (total_minutos / num_analistas) / 60 if num_analistas > 0 else 0
    return homem_hora

def exibir_grafico_pizza(df):
    """Exibe o gráfico de pizza da distribuição de status."""
    if df.empty:
        st.warning("Não há dados para exibir o gráfico de pizza.")
        return

    # Contagem de ordens por status
    status_counts = df['STATUS'].value_counts().reset_index()
    status_counts.columns = ['STATUS', 'count']
    #print(f"Status counts:\n{status_counts}")

    # Calcula o percentual
    total_count = status_counts['count'].sum()
    status_counts['percent'] = (status_counts['count'] / total_count) * 100
    
    # Mapeamento de cores
    color_map = {
        'Aberta': 'skyblue',
        'Processo': 'orange',
        'Encerrada': 'lightgreen'
    }

    # Criando o gráfico de pizza com Plotly
    fig = px.pie(status_counts,
                 names='STATUS',
                 values='count',
                 title="Distribuição de Ordens por Status",
                 color='STATUS',
                 color_discrete_map=color_map,
                 )
    fig.update_traces(
        hovertemplate="<b>Status:</b> %{label}"
    )
    st.plotly_chart(fig)

def exibir_grafico_barras(df):
    """Exibe o gráfico de barras da distribuição de status."""
    if df.empty:
        st.warning("Não há dados para exibir o gráfico de barras.")
        return

    # Contagem de ordens por status
    status_counts = df['STATUS'].value_counts().reset_index()
    status_counts.columns = ['STATUS', 'count']
    #print(f"Status counts:\n{status_counts}")

    # Calcula o percentual (opcional para gráfico de barras, mas pode ser útil)
    total_count = status_counts['count'].sum()
    status_counts['percent'] = (status_counts['count'] / total_count) * 100
    
    # Mapeamento de cores
    color_map = {
        'Aberta': 'skyblue',
        'Processo': 'orange',
        'Encerrada': 'lightgreen'
    }

    # Criando o gráfico de barras com Plotly
    fig = px.bar(status_counts,
                 x='STATUS',
                 y='count',
                 title="Distribuição de Ordens por Status",
                 color='STATUS',
                 color_discrete_map=color_map,
                 text_auto=True  # Mostra os valores diretamente nas barras
                 )
    fig.update_traces(
         hovertemplate="<b>Status:</b> %{x}<br><b>Quantidade:</b> %{y}<br><b>Percentual:</b> %{customdata[0]:.2f}%",
        customdata=status_counts[['percent']]
    )
    fig.update_layout(
                        yaxis_title="Quantidade de Ordens",
                        showlegend=False,
                      )
    fig.update_xaxes(title_text='Status')  # Alterar o rótulo do eixo x
    fig.update_yaxes(title_text='Ordens de Serviços')  # Alterar o rótulo do eixo y
    st.plotly_chart(fig)

def exibir_grafico_barras_tipo_os(indicadores_calc):
    """Exibe o gráfico de barras da contagem de ordens de serviço por tipo."""

    # Extraindo dados do dicionário de indicadores
    tipos_os = ['Corretiva', 'Ronda_Inspecao', 'Cadastro', 'Suporte', 'Relatorio','Desenvolvimento']
    contagens = [indicadores_calc.get(tipo, 0) for tipo in tipos_os]

    # Criando um DataFrame para o Plotly Express
    df_tipos = pd.DataFrame({'Tipo': tipos_os, 'Contagem': contagens})
    
    #print(f'\n\n*****df_tipos:\n{df_tipos}\n\n')

    # Mapeamento de cores (opcional, se quiser cores customizadas)
    color_map = {
        'Corretiva': 'skyblue',
        'Ronda_Inspecao': 'orange',
        'Cadastro': 'lightgreen',
        'Suporte': 'lightcoral',
        'Relatorio': 'green',
        'Desenvolvimento': 'lightblue',
    }

    fig = px.bar(df_tipos,
                x='Tipo',
                y='Contagem',
                title='Contagem de Ordens de Serviço por Tipo',
                color='Tipo',  # Usar Tipo para aplicar as cores
                color_discrete_map=color_map,
                text_auto=True #Habilitar os valores sobre as barras
               )

    fig.update_layout(
        showlegend=False,
        legend_title_text=" ",
        margin=dict(l=20, r=20, t=60, b=20),
        title_font=dict(size=17),
    )

    fig.update_xaxes(title_text='Tipos de O.S')  # Alterar o rótulo do eixo x
    fig.update_yaxes(title_text='Número de Ordens')  # Alterar o rótulo do eixo y
    fig.update_traces(
        textposition='outside',
        textfont_family="Arial",
        textfont_size=13,
        hovertemplate="<b>Tipo:</b> %{x}<br><b>Total:</b> %{y}"  # Personalizando o hovertemplate
    )

    st.plotly_chart(fig)

def gerar_dataframe_para_grafico_barras_analistas(df):
    """Gera o DataFrame para o gráfico de barras dos analistas."""
    #print(f'\ngerar_dataframe_para_grafico_barras_analistas()')
    if df.empty:
        return pd.DataFrame({'Analista': [], 'Ordens': []})
    
    #print(f'\ndf:\n{df[["ORDEM_SERVICO", "ANALISTA"]]} \n')
    
    # Remover duplicadas ANTES de agrupar
    df_unique = df[["ORDEM_SERVICO", "ANALISTA"]].drop_duplicates()
    #print(f'\n\ndrop_duplicates()\ndf:\n{df_unique} \n')
    
    # Agrupar por analista e contar as ordens de serviço distintas
    df_analistas = df_unique.groupby('ANALISTA')['ORDEM_SERVICO'].count().reset_index(name='Ordens')
    
    # Ordenar o DataFrame pela contagem de ordens em ordem decrescente
    df_analistas = df_analistas.sort_values(by='Ordens', ascending=False)
    
    #print(f'\ndf_analistas: \n{df_analistas}')
    
    return df_analistas

def exibir_grafico_barras_analistas(df):
    """Exibe o gráfico de barras das horas de atividades por analista."""
    
    if df.empty:
        st.warning("Não há dados para exibir o gráfico de barras dos analistas")
        return
    
    # Cria o DataFrame para o Plotly Express
    df_analistas = gerar_dataframe_para_grafico_barras_analistas(df)
    
    if df_analistas.empty:
        st.warning("Não há dados para exibir o gráfico de barras dos analistas")
        return

    # Mapeamento de cores (opcional, se quiser cores customizadas)
    color_map = pc.qualitative.Plotly # Cores padrão do Plotly

    fig = px.bar(df_analistas,
                x='ANALISTA',
                y='Ordens',
                title='Quantidade de Ordens de Serviço por Analista',
                color='ANALISTA',  # Usar Analista para aplicar as cores
                color_discrete_sequence=color_map,
                text_auto=True, #Habilitar os valores sobre as barras
                )

    fig.update_layout(
        showlegend=False,
        legend_title_text=" ",
        margin=dict(l=20, r=20, t=60, b=20),
        title_font=dict(size=17),
    )

    fig.update_xaxes(title_text='Analistas')  # Alterar o rótulo do eixo x
    fig.update_yaxes(title_text='Quantidade de Ordens')  # Alterar o rótulo do eixo y
    fig.update_traces(
        textposition='outside',
        textfont_family="Arial",
        textfont_size=13,
        hovertemplate="<b>Analista:</b> %{x}<br><b>Total:</b> %{y} Ordens"  # Personalizando o hovertemplate
    )
    st.plotly_chart(fig)
     
#=================================== MAIN #===================================
logo_path = 'HSF_LOGO_-_1228x949_001.png'

if __name__ == "__main__":
    #print('Indicadores de Ordem de Servico')    
    st.logo(logo_path,size="large")
    st.title('Indicadores de Ordem de Servico')
    # Configuração da atualização automática (em segundos)
    atualizar_a_cada = 600 # Atualiza a cada 10 minutos

    while True:
        print(f'\n{obter_timestamp_atual()} - Indicadores de Ordem de Servico - if __name__ == "__main__" ')
        try:
            with st.sidebar:
                # Obtendo a lista de anos distintos
                df_ordens_geral = REL_1507_Banda_Geral_Tipo_OS()
                anos_distintos = sorted(df_ordens_geral['ANO'].unique(), reverse=True)
                # Filtra os anos, mantendo apenas os iguais ou superiores a 2022
                anos_distintos = [ano for ano in anos_distintos if int(ano) >= 2024]
                anos_distintos = anos_distintos[:6]
                
                # Inicializa o ano mais recente
                if 'ano_selecionado' not in st.session_state:
                    st.session_state['ano_selecionado'] = anos_distintos[0] if anos_distintos else None
                
                st.write("---")
                st.write('Padrão carrega o ano e mês atual:')
            
                # Cria os botões para selecionar o ano
                if anos_distintos:
                    st.session_state['ano_selecionado'] = st.selectbox("Selecione o Ano", anos_distintos)
                else:
                    st.warning("Não há dados para exibir os filtros de anos.")
                
                # Filtrando o Data Frame pelo ano selecionado
                if st.session_state['ano_selecionado'] is not None:
                    df_filtered_ano = df_ordens_geral[df_ordens_geral['ANO'] == st.session_state['ano_selecionado']]
                else:
                    df_filtered_ano = df_ordens_geral.copy()
            
                # Obtendo a lista de meses distintos para o ano selecionado
                meses_distintos = sorted(df_filtered_ano['MES'].unique())
                
                # Obtendo a lista de meses por extenso
                meses_textos_distintos = sorted(df_filtered_ano['MES_TEXTO'].unique())
                #print(f'meses_textos_distintos:\n{meses_textos_distintos}')
                # Inicializa o mês selecionado, usando o primeiro mês disponível
                if 'mes_selecionado' not in st.session_state:
                    st.session_state['mes_selecionado'] = meses_distintos[0] if meses_distintos else None
                
                # Criando os botões para selecionar o mês
                if meses_distintos:
                    #inserido botoes para cada mes
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
        
        
            #Geracao de Data Frame:
            #df_ordens_geral = REL_1507_Banda_Geral_Tipo_OS()
            
            #Tratamento de valores null:
            #df_ordens_geral = df_ordens_geral = df_ordens_geral.fillna('-')
            
            # Filtrando o data frame pelo ano selecionado
            if st.session_state['ano_selecionado'] is not None:
                df_ordens_geral = df_ordens_geral[df_ordens_geral['ANO'] == st.session_state['ano_selecionado']]
            
            # Filtrando o data frame pelo mes selecionado
            if st.session_state['mes_selecionado'] is not None:
                df_ordens_geral = df_ordens_geral[df_ordens_geral['MES'] == st.session_state['mes_selecionado']]
            
            #tratamento de valores com casa decimal:
            df_ordens_geral['ANO'] = df_ordens_geral['ANO'].apply(lambda x: "{:.0f}".format(x))
            #df_ordens_geral['MINUTOS_TOTAL'] = df_ordens_geral['MINUTOS_TOTAL'].apply(lambda x: "{:.0f}".format(x))
            
            #st.write('# Indicadores de Ordem de Servico')
            st.write("---")
            st.write('## 📊Ordens de Serviço:')
            
            st.write("---")  # Linha separadora
                    
            # Calculo de Indicadores
            indicadores_calc = calcular_indicadores(df_ordens_geral)
            
            col1,col2,col3,col4,col5,col6,col7,col8 = st.columns(8)
            with col1:
                st.metric("📉Total de Ordens de Serviço", value=indicadores_calc["total_ordens"])
            with col2:
                st.write("")
            with col3:
                st.metric("🕒Horas", value=indicadores_calc["total_horas"])
            with col4:
                st.metric("🕒Minutos", value=indicadores_calc["minutos_restantes"])
            with col5:
                st.write("")
            with col6:
                st.write("")
            with col7:
                st.write("")
            with col8:
                st.write("")
            
            
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
                st.metric("🏆Encerradas", value=indicadores_calc["total_ordens_Encerrada"])
            with col20:
                st.metric('🚀Processo', value=indicadores_calc["total_ordens_Processo"])
            with col30:
                st.metric('⚠️Aberta', value=indicadores_calc["total_ordens_Aberta"])
            with col40:
                st.write("")
            #Tipos de OS:
            with col50:
                st.metric("Cadastro", value=indicadores_calc["Cadastro"])
            with col60:
                st.metric("Corretiva", value=indicadores_calc["Corretiva"])
            with col70:
                st.metric("Ronda / Inspeção", value=indicadores_calc["Ronda_Inspecao"])
            with col80:
                st.metric("Suporte", value=indicadores_calc["Suporte"])
            
            st.write("---")  # Linha separadora
            
            col1,col2 = st.columns(2)
            with col1:
                #exibir o gráfico de barras das ordens por status
                exibir_grafico_barras(df_ordens_geral)
            with col2:
                #exibir o gráfico de barras dos tipos de OS
                exibir_grafico_barras_tipo_os(indicadores_calc)
            
            # Criar uma nova linha abaixo dos indicadores para o botão de download
            st.write("---")  # Linha separadora
            st.write("---")  # Linha separadora
            
            #Geracao de Data Frame:
            df_rel_1507_Tipo_OS_Analitico = REL_1507_Banda_Geral_TP_OS_analitico()
            
            # Filtrando o data frame pelo ano selecionado
            if st.session_state['ano_selecionado'] is not None:
                df_rel_1507_Tipo_OS_Analitico = df_rel_1507_Tipo_OS_Analitico[df_rel_1507_Tipo_OS_Analitico['ANO_ATIVIDADE'] == st.session_state['ano_selecionado']]
            
            # Filtrando o data frame pelo mes selecionado
            if st.session_state['mes_selecionado'] is not None:
                df_rel_1507_Tipo_OS_Analitico = df_rel_1507_Tipo_OS_Analitico[df_rel_1507_Tipo_OS_Analitico['MES_ATIVIDADE'] == st.session_state['mes_selecionado']]
            
            #tratamento de valores com casa decimal:
            df_rel_1507_Tipo_OS_Analitico['ANO_ATIVIDADE'] = df_rel_1507_Tipo_OS_Analitico['ANO_ATIVIDADE'].apply(lambda x: "{:.0f}".format(x))
            df_rel_1507_Tipo_OS_Analitico['ANO_ORDEM_SERVICO'] = df_rel_1507_Tipo_OS_Analitico['ANO_ORDEM_SERVICO'].apply(lambda x: "{:.0f}".format(x))
            df_rel_1507_Tipo_OS_Analitico['ORDEM_SERVICO'] = df_rel_1507_Tipo_OS_Analitico['ORDEM_SERVICO'].apply(lambda x: "{:.0f}".format(x))
            
            #formatando Ano da atividade com funcao:
            df_rel_1507_Tipo_OS_Analitico['ANO_ATIVIDADE'] = df_rel_1507_Tipo_OS_Analitico['ANO_ATIVIDADE'].apply(formatar_ano_dia_mes_vazios)
            
            #formatando Mes da atividade com funcao:
            df_rel_1507_Tipo_OS_Analitico['MES_ATIVIDADE'] = df_rel_1507_Tipo_OS_Analitico['MES_ATIVIDADE'].apply(formatar_ano_dia_mes_vazios)
            
            #formatando Mes da atividade com funcao:
            df_rel_1507_Tipo_OS_Analitico['DIA_ATIVIDADE'] = df_rel_1507_Tipo_OS_Analitico['DIA_ATIVIDADE'].apply(formatar_ano_dia_mes_vazios)
            
            st.write("---")
            st.write('## Atividades por Analistas:')
            
            st.write("---")  # Linha separadora
            indicadores_calc_analitico = calcular_indicadores_por_analista(df_rel_1507_Tipo_OS_Analitico) 
            #Exibir os cartões dos analistas:
            col1,col2,col3,col4,col5,col6,col7,col8 = st.columns(8)
            with col1:
                st.metric("📊Total de Atividades:", value=indicadores_calc_analitico["total_atividades"])
            with col2:
                st.write("")
            with col3:
                st.metric("🕒Horas", value=indicadores_calc_analitico["total_horas"])
                st.write("")
            with col4:
                st.metric("🕒Minutos", value=indicadores_calc_analitico["minutos_restantes"])
                st.write("")
            with col5:
                st.write("")
            with col6:
                st.write("")
            with col7:
                st.write("")
            with col8:
                st.write("")
            
            #Exibe os cartoes de cada analista:
            exibir_cartoes_analistas(indicadores_calc_analitico["Analistas_horas"])  
            st.write("---")  # Linha separadora 
            
            #Grafico com horas de cada analista:
            exibir_grafico_barras_analistas(df_rel_1507_Tipo_OS_Analitico)
            
            st.write("---")  # Linha separadora
            st.write("---")  # Linha separadora
            st.subheader("Geral por Atividades:")
            
            #Escolhendo e depois Renomeando colunas:
            df_rel_1507_AJUSTADO = df_rel_1507_Tipo_OS_Analitico[[
                                                                    'ORDEM_SERVICO', 'ANO_ATIVIDADE', 'MES__ATIVIDADE_TEXTO', 'TIPO', 
                                                                    'STATUS', 'DS_PRIORIDADE', 'ANALISTA', 'MINUTOS_TOTAL'
                                                                ]].rename(columns={
                                                                    'ORDEM_SERVICO': 'OS',
                                                                    'ANO_ATIVIDADE': 'Ano',
                                                                    'MES__ATIVIDADE_TEXTO': 'Mês',
                                                                    'TIPO': 'Tipo',
                                                                    'STATUS': 'Status',
                                                                    'DS_PRIORIDADE': 'Prioridade',
                                                                    'ANALISTA': 'Responsável',
                                                                    'MINUTOS_TOTAL': 'Minutos'
                                                                })

            st.dataframe(df_rel_1507_AJUSTADO,hide_index=True, use_container_width=True)

        except Exception as err: 
            print(f"Inexperado:\n {err=}, {type(err)=}")
        
        print(f'{obter_timestamp_atual()} - time.sleep({atualizar_a_cada})')
        time.sleep(atualizar_a_cada)
        print(f'{obter_timestamp_atual()} - st.rerun()\n')
        st.rerun()