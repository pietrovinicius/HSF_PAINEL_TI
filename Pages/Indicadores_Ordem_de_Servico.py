#03/01/2025
#@PLima
#HFS - PAINEL DE DIVERSOS DADOS E INDICADORES
#Indicadores de Ordem de Servico

import streamlit as st
import time

#Configurando pagina para exibicao em modo WIDE:
st.set_page_config(layout="wide",initial_sidebar_state="expanded",page_title="Indicadores Ordem de Servico")
#st.set_page_config(layout="wide",initial_sidebar_state="collapsed",page_title="Indicadores Ordem de Servico")
# Caminho da sua imagem (ajuste conforme a sua estrutura de pastas)


if __name__ == "__main__":
    try:
        st.write('# Indicadores de Ordem de Servico')
        
    except Exception as err: 
        print(f"Inexperado {err=}, {type(err)=}")