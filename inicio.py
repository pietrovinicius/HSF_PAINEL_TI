#26/11/2023
#@PLima
#HFS - PAINEL DE DIVERSOS DADOS E INDICADORES

import streamlit as st


#Configurando pagina para exibicao em modo WIDE:
st.set_page_config(layout="wide",initial_sidebar_state="expanded")

# Caminho da sua imagem (ajuste conforme a sua estrutura de pastas)
logo_path = 'HSF_LOGO_-_1228x949_001.png'

if __name__ == "__main__": 
    print('\n\n__main__')
    st.logo(logo_path,size="large")
    #st.sidebar.markdown("# HOME")
    st.image(logo_path,width=400)
    st.write('\n\n\n\n')
    st.write('\n\n\n\n')
    st.write('## Dashboard - TI')
    st.write('\n\n\n\n')
    st.write('Destinado para exibição...')