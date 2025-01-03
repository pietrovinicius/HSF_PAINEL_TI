import streamlit as st
import pandas as pd
import numpy as np
import os
import oracledb
import pandas as pd
import time
import locale
import datetime
import plotly.express as px
import io  # Para lidar com arquivos na memória

# Configurando pagina para exibicao em modo WIDE:
st.set_page_config(layout="wide", initial_sidebar_state="collapsed", page_title="Analítico SLA - Ordem de Serviço")

# Aumentando exiubição do dataframe no Streamlit:
pd.set_option("styler.render.max_elements", 1249090)


def agora():
    agora = datetime.datetime.now()
    agora = agora.strftime("%Y-%m-%d %H-%M-%S")
    return str(agora)


# apontamento para usar o Think Mod
def encontrar_diretorio_instantclient(
        nome_pasta="instantclient-basiclite-windows.x64-23.6.0.24.10\\instantclient_23_6"):
    # Obtém o diretório do script atual
    diretorio_atual = os.path.dirname(os.path.abspath(__file__))

    # Constrói o caminho completo para a pasta do Instant Client
    caminho_instantclient = os.path.join(diretorio_atual, nome_pasta)

    # Verifica se a pasta existe
    if os.path.exists(caminho_instantclient):
        return caminho_instantclient
    else:
        print(f"A pasta '{nome_pasta}' nao foi encontrada na raiz do aplicativo.")
        return None


# @st.cache_data
@st.cache_data
def REL_1618():
    try:
        # Chamar a função para obter o caminho do Instant Client
        caminho_instantclient = encontrar_diretorio_instantclient()

        # Usar o caminho encontrado para inicializar o Oracle Client
        if caminho_instantclient:
            print(f'if caminho_instantclient:\n')
            print(f'oracledb.init_oracle_client(lib_dir=caminho_instantclient)\n')
            oracledb.init_oracle_client(lib_dir=caminho_instantclient)
        else:
            print("Erro ao localizar o Instant Client. Verifique o nome da pasta e o caminho.")

        connection = oracledb.connect(user="TASY", password="aloisk", dsn="192.168.5.9:1521/TASYPRD")

        with connection:
            print(f'with oracledb.connect(user=un, password=pw, dsn=cs) as connection\n')

            print(f'\nconnection.current_schema: {connection.current_schema}')

            with connection.cursor() as cursor:
                print(f'with connection.cursor() as cursor:\n')

                #####################################################################################
                # QUERY:
                sql = """              
                    SELECT 
                    ATP.NR_SEQUENCIA AS NR_ORDEM,
                    INITCAP(OBTER_NOME_USUARIO(ATP.NM_USUARIO_EXEC)) AS Analista,
                    --ATP.DT_ORDEM_SERVICO,
                    EXTRACT(YEAR FROM ATP.DT_ORDEM_SERVICO) AS ANO,
                    TO_CHAR(ATP.DT_ORDEM_SERVICO, 'Month') AS MES,
                    EXTRACT(MONTH FROM ATP.DT_ORDEM_SERVICO) AS OS_MES,
                    TO_CHAR(ATP.DT_ORDEM_SERVICO, 'dd/mm/yyyy hh24:mi') AS DATA,
                    DECODE(ATP.IE_PRIORIDADE, 'A', 'Alta', 'M', 'Média', 'E','Emergência', 'Fora da Prioridade') AS DS_PRIORIDADE, 
                    DECODE(ATP.IE_PRIORIDADE, 'A', 240, 'M', 360, 'E', 10) AS META_SLA,
                    OBTER_DIF_DATA(ATP.DT_ORDEM_SERVICO, DT_FIM_REAL, 'TM') AS TEMPO_TOTAL,

                    TO_CHAR(ATP.DT_FIM_REAL,'dd/mm/yyyy') AS DT_FIM_REAL,

                    CASE  
                    WHEN DECODE(ATP.IE_PRIORIDADE, 'A', 240, 'M', 360, 'E', 10) >=  OBTER_DIF_DATA(ATP.DT_ORDEM_SERVICO, ATP.DT_FIM_REAL, 'TM') THEN 'Atendido'
                    WHEN DECODE(ATP.IE_PRIORIDADE, 'A', 240, 'M', 360, 'E', 10) <  OBTER_DIF_DATA(ATP.DT_ORDEM_SERVICO, ATP.DT_FIM_REAL, 'TM') THEN 'Excedido'
                    ELSE 'FORA DO SLA'
                    END AS SLA
                FROM MAN_ORDEM_SERVICO ATP
                INNER JOIN MAN_GRUPO_TRABALHO SA ON SA.NR_SEQUENCIA = ATP.NR_GRUPO_TRABALHO
                INNER JOIN MAN_LOCALIZACAO ML ON ML.NR_SEQUENCIA = ATP.NR_SEQ_LOCALIZACAO
                INNER JOIN SETOR_ATENDIMENTO SAT ON SAT.CD_SETOR_ATENDIMENTO = ML.CD_SETOR
                LEFT JOIN MAN_GRUPO_PLANEJAMENTO MGP ON MGP.NR_SEQUENCIA = ATP.NR_GRUPO_PLANEJ
                --WHERE ATP.DT_ORDEM_SERVICO	BETWEEN sysdate -90  and sysdate --:DT_INICIAL AND :DT_FINAL
                --AND ATP.IE_STATUS_ORDEM = 3
                WHERE ATP.IE_STATUS_ORDEM = 3

                ORDER BY
                    EXTRACT(YEAR FROM ATP.DT_ORDEM_SERVICO) DESC,
                    EXTRACT(MONTH FROM ATP.DT_ORDEM_SERVICO) ASC,
                    ATP.NR_SEQUENCIA ASC
                    """
                #####################################################################################

                # Executando a query:
                # print(f'cursor.execute(sql)\n{sql}')
                cursor.execute(sql)
                results = cursor.fetchall()
                df = pd.DataFrame(results, columns=[desc[0] for desc in cursor.description])

                # Visualizar os primeiros 5 registros
                print(f'data_frame:\n{df.head(5)}')
                print(f"\nExemplo:\n{df.sample()}")
                print(f"\nTamanho:{df.shape}")

                print("DataFrame gerado com sucesso!")

    except Exception as erro:
        print(f"Erro Inexperado:\n{erro}")

    return df


def sla_cor_status(val):
    if val == 'Excedido':
        return 'background-color: yellow; color: black ; font-weight: bold'  # Amarelo com texto preto para melhor contraste
    elif val == 'Em análise':
        return 'background-color: lightblue; color: black ; font-weight: bold'  # Verde claro com texto preto
    elif val == 'Sim':
        return 'background-color: sandybrown; color: black ; font-weight: bold;'  # Amarelo com texto preto para melhor contraste
    else:
        return ''


def indicadores(df_rel_1618):
    # Total de ordens
    total_ordens = len(df_rel_1618)
    
    # Total de Ordens dentro do SLA
    total_ordens_no_sla = len(df_rel_1618[df_rel_1618['SLA'] == 'Atendido'])

    # Total de Ordens fora do SLA
    total_ordens_fora_sla = len(df_rel_1618[df_rel_1618['SLA'] == 'Excedido'])

    # Percentual de Ordens no SLA
    percentual_ordens_no_sla = (total_ordens_no_sla / total_ordens) * 100 if total_ordens > 0 else 0

    # Percentual de Ordens fora do SLA
    percentual_ordens_fora_sla = (total_ordens_fora_sla / total_ordens) * 100 if total_ordens > 0 else 0

    # Tempo médio total
    df_rel_1618['TEMPO_TOTAL'] = df_rel_1618['TEMPO_TOTAL'].astype(int)  # Garante que a coluna seja numérica
    media_tempo_total = df_rel_1618['TEMPO_TOTAL'].mean()

    # Tempo médio por prioridade
    media_tempo_por_prioridade = df_rel_1618.groupby('DS_PRIORIDADE')['TEMPO_TOTAL'].mean().reset_index()
    print(f"total_ordens: {total_ordens}")
    print(f"total_ordens_no_sla: {total_ordens_no_sla}")
    print(f"total_ordens_fora_sla: {total_ordens_fora_sla}")
    print(f"percentual_ordens_no_sla: {percentual_ordens_no_sla}")
    print(f"percentual_ordens_fora_sla: {percentual_ordens_fora_sla}")
    print(f"media_tempo_total: {media_tempo_total}")
    print(f"media_tempo_por_prioridade: {media_tempo_por_prioridade}")
    return {
        "total_ordens": total_ordens,
        "total_ordens_no_sla": total_ordens_no_sla,
        "total_ordens_fora_sla": total_ordens_fora_sla,
        "percentual_ordens_no_sla": percentual_ordens_no_sla,
        "percentual_ordens_fora_sla": percentual_ordens_fora_sla,
        "media_tempo_total": media_tempo_total,
        "media_tempo_por_prioridade": media_tempo_por_prioridade
    }


def grafico_pizza(df_rel_1618):
    sla_counts = df_rel_1618['SLA'].value_counts().reset_index()
    sla_counts.columns = ['SLA', 'count']

    fig = px.pie(sla_counts, names='SLA', values='count', title="Distribuição de Ordens por SLA")
    st.plotly_chart(fig)


def grafico_barras_tempo_prioridade(indicadores_calc):
    fig = px.bar(indicadores_calc["media_tempo_por_prioridade"], x='DS_PRIORIDADE', y='TEMPO_TOTAL',
                 title='Tempo Médio por Prioridade')
    st.plotly_chart(fig)


# Função para transformar DataFrame em Excel e disponibilizar o download
def download_dataframe_as_excel(df, filename="dados.xlsx"):
    """
    Converte um DataFrame em um arquivo Excel na memória e prepara para download.

    Args:
        df (pd.DataFrame): O DataFrame a ser convertido.
        filename (str): Nome do arquivo para download.

    Returns:
        bytes: Arquivo Excel no formato bytes.
    """
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, sheet_name='Sheet1', index=False)
    processed_data = output.getvalue()
    return processed_data


logo_path = 'HSF_LOGO_-_1228x949_001.png'

if __name__ == "__main__":
    # Configurando o idioma
    locale.setlocale(locale.LC_ALL, 'pt_BR.utf8')
    
    st.logo(logo_path, size="large")
    st.write('# Analítico SLA - Ordem de Serviço')

    # Geracao de Data Frame:
    df_rel_1618 = REL_1618()

    # Tratamento de valores null:
    df_rel_1618 = df_rel_1618.fillna('-')

    # tratamento de valores com casa decimal:
    df_rel_1618['NR_ORDEM'] = df_rel_1618['NR_ORDEM'].apply(lambda x: "{:.0f}".format(x))
    df_rel_1618['ANO'] = df_rel_1618['ANO'].apply(lambda x: "{:.0f}".format(x))

    # tratamento do valor com .0:
    df_rel_1618['META_SLA'] = df_rel_1618['META_SLA'].astype(str).str.replace('.0', '')

    # Obtendo a lista de anos distintos
    anos_distintos = df_rel_1618['ANO'].unique()
    print(f'\n\nanos distintos: {anos_distintos}\n\n')

    anos_distintos = sorted(anos_distintos, reverse=True)
    print(f'\n\nanos distintos sorted: {anos_distintos}\n\n')

    # Limita a lista de anos aos 3 primeiros:
    anos_distintos = anos_distintos[:3]
    print(f'\n\nanos distintos[:3]: {anos_distintos}\n\n')

    # Inicializa o ano selecionado com o ano mais recente
    if 'ano_selecionado' not in st.session_state:
        st.session_state['ano_selecionado'] = anos_distintos[0]

    # Criando os botões para selecionar o ano
    col_anos = st.columns(len(anos_distintos))
    for col, ano in zip(col_anos, anos_distintos):
        if col.button(str(ano), key=f"btn_{ano}"):
            st.session_state['ano_selecionado'] = ano

    # Filtrando o Data Frame pelo ano selecionado:
    df_filtered_ano = df_rel_1618[df_rel_1618['ANO'] == st.session_state['ano_selecionado']]

    # Obtendo a lista de meses distintos para o ano selecionado:
    meses_distintos = sorted(df_filtered_ano['OS_MES'].unique())
    
    # Inicializa o mês selecionado, usando o primeiro mês disponível
    if 'mes_selecionado' not in st.session_state:
      st.session_state['mes_selecionado'] = meses_distintos[0] if meses_distintos else None
    
    # Criando os botões para selecionar o mês
    if meses_distintos:
      #Converte os números para nome do mês
      meses_nomes = [datetime.date(1900, int(mes), 1).strftime('%B') for mes in meses_distintos]
      
      col_meses = st.columns(len(meses_distintos))
      for col, mes, nome_mes in zip(col_meses, meses_distintos, meses_nomes):
          if col.button(str(nome_mes), key=f"btn_mes_{mes}"):
              st.session_state['mes_selecionado'] = mes

      # Filtrando o data frame pelo mes selecionado
      df_filtered_mes = df_filtered_ano[df_filtered_ano['OS_MES'] == st.session_state['mes_selecionado']]
    else:
      df_filtered_mes = df_filtered_ano
      st.write("Não há dados para esse ano")

    # Calculo de Indicadores
    print(f"Calculo de Indicadores com o df_filtered_mes:\n{df_filtered_mes.head(5)}")
    indicadores_calc = indicadores(df_filtered_mes)

    # colunas para exibir os indicadores:
    col1, col2, col3 , col4 = st.columns(4)

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
        st.metric("Tempo Médio de Atendimento", value=f'{indicadores_calc["media_tempo_total"]:.2f}')

    
    col10, col20 = st.columns(2)
    with col10:
        # Chamando o Grafico de Pizza
        grafico_pizza(df_filtered_mes)
        
    with col20:
        # Chamando o Gráfico de Barras com Tempo por prioridade:
        grafico_barras_tempo_prioridade(indicadores_calc)

    # Criar uma nova linha abaixo dos indicadores para o botão de download
    st.write("---")  # Linha separadora
    
    # Estilo do DataFrame
    df_styled = df_filtered_mes.style.applymap(sla_cor_status, subset=['SLA'])
    
    # Exibindo o dataframe:
    st.dataframe(df_styled, hide_index=True, use_container_width=True)
    
    # Disponibilizar o botão de download
    download_xlsx = download_dataframe_as_excel(df_filtered_mes)
    st.download_button(
        label="Download em XLSX",
        data=download_xlsx,
        file_name='dados_sla.xlsx',
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

    # Criar uma nova linha abaixo dos indicadores para o botão de download
    st.write("---")  # Linha separadora