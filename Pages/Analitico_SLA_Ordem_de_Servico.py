#03/01/2025
#@PLima
#HFS - PAINEL DE DIVERSOS DADOS E INDICADORES
#Analítico SLA - Ordem de Serviço


import streamlit as st
import pandas as pd
import numpy as np
import os
import oracledb
import pandas as pd
import time
import locale
import datetime

#Configurando pagina para exibicao em modo WIDE:
st.set_page_config(layout="wide",initial_sidebar_state="expanded",page_title="Analítico SLA - Ordem de Serviço")

def agora():
    agora = datetime.datetime.now()
    agora = agora.strftime("%Y-%m-%d %H-%M-%S")
    return str(agora)

#apontamento para usar o Think Mod
def encontrar_diretorio_instantclient(nome_pasta="instantclient-basiclite-windows.x64-23.6.0.24.10\\instantclient_23_6"):
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

#@st.cache_data 
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
        
        connection = oracledb.connect( user="TASY", password="aloisk", dsn="192.168.5.9:1521/TASYPRD")
        
        with connection:
            print(f'with oracledb.connect(user=un, password=pw, dsn=cs) as connection\n')
            
            print(f'\nconnection.current_schema: {connection.current_schema}')
            
            with connection.cursor() as cursor:
                print(f'with connection.cursor() as cursor:\n')
                
                #####################################################################################
                #QUERY:
                sql = """              
                    SELECT 
                    ATP.NR_SEQUENCIA AS NR_ORDEM,
                    INITCAP(OBTER_NOME_USUARIO(ATP.NM_USUARIO_EXEC)) AS LOCAL,
                    --ATP.DT_ORDEM_SERVICO,
                    EXTRACT(YEAR FROM ATP.DT_ORDEM_SERVICO) AS ORDEM_SERVICO_ANO,
                    EXTRACT(MONTH FROM ATP.DT_ORDEM_SERVICO) AS ORDEM_SERVICO_MES,
                    TO_CHAR(ATP.DT_ORDEM_SERVICO, 'dd/mm/yyyy hh24:mi:ss') AS ORDEM_SERVICO,
                    TO_CHAR(ATP.DT_ORDEM_SERVICO, 'Month') AS MES_TEXTO,
                    DECODE(ATP.IE_PRIORIDADE, 'A', 'ALTA', 'M', 'MEDIA', 'E','EMERGÊNCIA', 'FORA DA PRIORIDADE') AS DS_PRIORIDADE, 
                    DECODE(ATP.IE_PRIORIDADE, 'A', 240, 'M', 360, 'E', 10) AS META_SLA,
                    OBTER_DIF_DATA(ATP.DT_ORDEM_SERVICO, DT_FIM_REAL, 'TM') AS TEMPO_TOTAL,
                    TO_CHAR(ATP.DT_FIM_REAL) AS DT_FIM_REAL,
                    CASE  
                    WHEN DECODE(ATP.IE_PRIORIDADE, 'A', 240, 'M', 360, 'E', 10) >=  OBTER_DIF_DATA(ATP.DT_ORDEM_SERVICO, ATP.DT_FIM_REAL, 'TM') THEN 'ATENDIDO'
                    WHEN DECODE(ATP.IE_PRIORIDADE, 'A', 240, 'M', 360, 'E', 10) <  OBTER_DIF_DATA(ATP.DT_ORDEM_SERVICO, ATP.DT_FIM_REAL, 'TM') THEN 'EXCEDIDO'
                    ELSE 'FORA DO SLA'
                    END AS SLA
                FROM MAN_ORDEM_SERVICO ATP
                INNER JOIN MAN_GRUPO_TRABALHO SA ON SA.NR_SEQUENCIA = ATP.NR_GRUPO_TRABALHO
                INNER JOIN MAN_LOCALIZACAO ML ON ML.NR_SEQUENCIA = ATP.NR_SEQ_LOCALIZACAO
                INNER JOIN SETOR_ATENDIMENTO SAT ON SAT.CD_SETOR_ATENDIMENTO = ML.CD_SETOR
                LEFT JOIN MAN_GRUPO_PLANEJAMENTO MGP ON MGP.NR_SEQUENCIA = ATP.NR_GRUPO_PLANEJ
                WHERE ATP.DT_ORDEM_SERVICO	BETWEEN sysdate -90  and sysdate --:DT_INICIAL AND :DT_FINAL
                AND ATP.IE_STATUS_ORDEM = 3

                ORDER BY
                    EXTRACT(YEAR FROM ATP.DT_ORDEM_SERVICO) DESC,
                    EXTRACT(MONTH FROM ATP.DT_ORDEM_SERVICO) ASC,
                    ATP.NR_SEQUENCIA ASC
                    """
                #####################################################################################
                
                #Executando a query:
                #print(f'cursor.execute(sql)\n{sql}')
                cursor.execute(sql)
                results = cursor.fetchall()
                df = pd.DataFrame(results, columns=[desc[0] for desc in cursor.description])
                
                # Visualizar os primeiros 5 registros
                print(f'data_frame:\n{df.head(5)}')
                
                print("DataFrame gerado com sucesso!")

    except Exception as erro:
        print(f"Erro Inexperado:\n{erro}")
    
    return df

# Caminho da sua imagem (ajuste conforme a sua estrutura de pastas)
logo_path = 'HSF_LOGO_-_1228x949_001.png'

if __name__ == "__main__":
    print('Analítico SLA - Ordem de Serviço')
    st.logo(logo_path,size="large")
    try:
        st.write('# Analítico SLA - Ordem de Serviço')
        
        #Geracao de Data Frame:
        df_rel_1618 = REL_1618()
        
        #Tratamento de valores null:
        df_rel_1618 = df_rel_1618 = df_rel_1618.fillna('-')
        
        #tratamento de valores com casa decimal:
        #df_rel_1618['ANO'] = df_rel_1618['ANO'].apply(lambda x: "{:.0f}".format(x))
        #df_rel_1618['MINUTOS_TOTAL'] = df_rel_1618['MINUTOS_TOTAL'].apply(lambda x: "{:.0f}".format(x))
        
        st.dataframe(df_rel_1618,hide_index=True, height=680,use_container_width=True)
        
    except Exception as err: 
        print(f"Inexperado {err=}, {type(err)=}")