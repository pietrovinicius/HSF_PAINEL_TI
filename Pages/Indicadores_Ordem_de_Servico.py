#03/01/2025
#@PLima
#HFS - PAINEL DE DIVERSOS DADOS E INDICADORES
#Indicadores de Ordem de Servico

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
st.set_page_config(layout="wide",initial_sidebar_state="expanded",page_title="Indicadores Ordem de Servico")

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
def REL_1507_Banda_Setor():
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
                        4 AS ORDEM,
                        SAT.DS_SETOR_ATENDIMENTO AS LOCAL,
                        --LAST_DAY(TRUNC(MOSA.DT_ATIVIDADE)) AS DATA,
                        EXTRACT(YEAR FROM MOSA.DT_ATIVIDADE) AS ANO,
                        EXTRACT(MONTH FROM MOSA.DT_ATIVIDADE) AS MES,
                        TO_CHAR(MOSA.DT_ATIVIDADE, 'Month') AS MES_TEXTO,
                        DECODE(ATP.IE_STATUS_ORDEM, 1, 'Aberta', 2, 'Processo', 3, 'Encerrada') AS STATUS,
                        COUNT(DISTINCT ATP.NR_SEQUENCIA) AS ORDEM_SERVICO_TOTAL,
                        SUM(MOSA.QT_MINUTO) AS MINUTOS_TOTAL,
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
                #####################################################################################
                
                #Executando a query:
                #print(f'cursor.execute(sql)\n{sql}')
                cursor.execute(sql)
                
                # Imprimir os resultados da consulta para verificar
                print(f'results = cursor.fetchall()\n')
                results = cursor.fetchall()
        
                #Exibindo redultado no console:
                #print(f'Exibindo redultado no console:\n')    
                #for r in cursor.execute(sql):
                #    print(r)
                
                #Inserindo resultado em um data frame:
                #df = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
                
                print(f'df = pd.DataFrame(results, columns=[desc[0] for desc in cursor.description])')
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
    print('Indicadores de Ordem de Servico')
    st.logo(logo_path,size="large")
    try:
        st.write('# Indicadores de Ordem de Servico')
        
        #Geracao de Data Frame:
        df_rel_1507 = REL_1507_Banda_Setor()
        
        #Tratamento de valores null:
        df_rel_1507 = df_rel_1507 = df_rel_1507.fillna('-')
        
        #tratamento de valores com casa decimal:
        df_rel_1507['ANO'] = df_rel_1507['ANO'].apply(lambda x: "{:.0f}".format(x))
        df_rel_1507['MINUTOS_TOTAL'] = df_rel_1507['MINUTOS_TOTAL'].apply(lambda x: "{:.0f}".format(x))
        
        st.dataframe(df_rel_1507,hide_index=True, height=680,use_container_width=True)
        
    except Exception as err: 
        print(f"Inexperado {err=}, {type(err)=}")