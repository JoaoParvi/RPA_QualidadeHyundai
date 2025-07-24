import time
import os
import pandas as pd
import logging
import urllib
from datetime import date
from sqlalchemy import create_engine
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from webdriver_manager.chrome import ChromeDriverManager

# === CONFIGURAÇÕES ===
URL = "https://routepesquisa.com.br/hgsi/PreviaNPS/"
LOGIN = "aline_maria"
SENHA = "Pateomandabem@2024"
TABELA_SQL = "QualidadeHyundai"
PASTA_DOWNLOAD = r"C:\Users\joao.mendes\Downloads"

# Configurar logging
logging.basicConfig(
    filename='coleta_route.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

try:
    logging.info("Inicializando o navegador...")

    options = webdriver.ChromeOptions()
    prefs = {"download.default_directory": PASTA_DOWNLOAD}
    options.add_experimental_option("prefs", prefs)
    options.add_argument("--start-maximized")

    navegador = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    navegador.get(URL)
    navegador.implicitly_wait(10)

    # === LOGIN ===
    navegador.find_element(By.CSS_SELECTOR, "input#Usuario.form-control").send_keys(LOGIN)
    navegador.find_element(By.CSS_SELECTOR, "input#Senha.form-control").send_keys(SENHA)
    navegador.find_element(By.CSS_SELECTOR, "input.btn.btn-midnightblue.btn-lg.btn-block").click()

    # === FILTRO ===
    navegador.find_element(By.ID, 'btnFiltros').click()
    time.sleep(10)

    periodo = navegador.find_element(By.ID, 'Perido')
    periodo.click()
    
    tipo_quest = navegador.find_element(By.ID, 'TipoQuest')
    tipo_quest.click()
    time.sleep(8)
    tipo_quest.send_keys(Keys.ARROW_DOWN) 
    tipo_quest.send_keys(Keys.ENTER)
    time.sleep(7)

    navegador.find_element(By.ID, 'Filtrar').click()
    time.sleep(10)

    navegador.find_element(By.CSS_SELECTOR, "#mdlAlertaComunicado > div.modal-dialog > div > div.modal-footer > a").click()
    time.sleep(10)
    
    # === NAVEGAÇÃO ATÉ RELATÓRIO ===
    navegador.find_element(By.XPATH, '//nav/ul/li[3]/a/span').click()
    time.sleep(4)
    navegador.find_element(By.XPATH, '//nav/ul/li[3]/ul/li[4]/a').click()
    time.sleep(6)

    # === CLICA NO BOTÃO DE DOWNLOAD DO XLS ===
    print("Clicando no botão de download do Excel...")
    navegador.find_element(By.CSS_SELECTOR, "a.btn.btn-success.btn-lg").click()
    time.sleep(8)

    # === ACHA O ARQUIVO MAIS RECENTE XLS ===
    arquivos = [f for f in os.listdir(PASTA_DOWNLOAD) if f.endswith(".xls") and not f.endswith(".crdownload")]
    arquivos.sort(key=lambda x: os.path.getmtime(os.path.join(PASTA_DOWNLOAD, x)), reverse=True)
    arquivo_excel = os.path.join(PASTA_DOWNLOAD, arquivos[0])
    print(f"Arquivo baixado: {arquivo_excel}")

    # === LEITURA COMPLETA DO EXCEL ===
    df_raw = pd.read_excel(arquivo_excel, engine="xlrd", header=None)

    # === OBTÉM A LINHA DO "NACIONAL" (linha 14, índice 13) ===
    linha_nacional = df_raw.iloc[12, [3, 6, 11]]  # D14, G14, L14
    df_nacional = pd.DataFrame([linha_nacional.values], columns=['Descricao', 'Indice_HGSI', 'Recomendacao_Montadora'])
    
    # Define o nome "Nacional" explicitamente (caso a célula esteja vazia)
    df_nacional['Descricao'] = df_nacional['Descricao'].fillna('Nacional')

    # === LÊ AS LINHAS DE 21 A 28 (índices 20 a 27) ===
    df_dados = df_raw.iloc[19:28, [3, 6, 11]]  # colunas: D, G, L
    df_dados.columns = ['Descricao', 'Indice_HGSI', 'Recomendacao_Montadora']

    # === Preenche descrições vazias com valor de B14 (se necessário)
    descricao_ref = df_raw.iloc[13, 1]  # B14
    df_dados['Descricao'] = df_dados['Descricao'].fillna(descricao_ref)

    # === COMBINA O NACIONAL COM O RESTANTE ===
    df_dados = pd.concat([df_nacional, df_dados], ignore_index=True)

    # Remove linhas onde tudo está vazio
    df_dados = df_dados.dropna(subset=['Descricao', 'Indice_HGSI', 'Recomendacao_Montadora'], how='all')
    df_dados[['Recomendacao_Montadora']] = df_dados[['Recomendacao_Montadora']].round(2)

    # Adiciona colunas fixas
    df_dados["Segmento"] = "Pos Vendas"
    df_dados["data_atualizacao"] = pd.to_datetime("today").date()

    print("\nDados extraídos com sucesso:")
    print(df_dados)

except Exception as e:
    logging.error(f"Erro: {str(e)}")
    print("Erro:", str(e))

finally:
    try:
        navegador.quit()
        print("Navegador fechado com sucesso.")
    except:
        print("Erro ao fechar o navegador.")

    # === SALVA NO BANCO DE DADOS ===
    try:
        print("Conectando ao banco de dados...")
        user = 'rpa_bi'
        password = 'Rp@_B&_P@rvi'
        host = '10.0.10.243'
        port = '54949'
        database = 'stage'

        params = urllib.parse.quote_plus(
            f'DRIVER=ODBC Driver 17 for SQL Server;SERVER={host},{port};DATABASE={database};UID={user};PWD={password}'
        )
        connection_str = f'mssql+pyodbc:///?odbc_connect={params}'
        engine = create_engine(connection_str)

        with engine.connect() as connection:
            df_dados.to_sql(TABELA_SQL, con=connection, if_exists='replace', index=False)

        print(f"Dados inseridos com sucesso na tabela '{TABELA_SQL}'!")
        logging.info(f"Dados inseridos com sucesso na tabela '{TABELA_SQL}'.")

    except Exception as e:
        logging.exception("Erro ao inserir dados no banco: %s", str(e))
        print("Erro ao inserir dados no banco:", str(e))
