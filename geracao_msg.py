from time import sleep
import time
import pandas as pd
from datetime import datetime, timedelta
import os, glob
from re import search
import json
from datetime import datetime
import pandas as pd
from openpyxl import load_workbook
import sqlite3
import dataframe_image as dfi
import PyPDF2
import numpy as np
import re
import math
import win32com.client as win32
import win32com.client
import win32process
import psutil
import matplotlib.pyplot as plt
from matplotlib import ticker
import datetime as dt
import imgkit
import json
from PIL import Image, ImageDraw, ImageFont
import io
import warnings
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from urllib.parse import quote_plus
import polars as pl
from shapely.geometry import Point
import contextily as ctx
import geopandas as gpd
import os
from datetime import datetime
import inspect
import warnings
#warnings.filterwarnings("ignore")


def verificar_base_atualizada(file_path):
    """
    Verifica qual das pastas contém o arquivo mais atualizado. Retorna o caminho da pasta que contém
    o arquivo mais recente entre todas as verificadas.
    Também imprime logs de início, atualização e finalização.

    :param file_path: Caminho inicial do arquivo ou diretório.
    :return: Caminho da pasta mais atualizada com base no arquivo mais recente ou mensagem de erro se nenhum for encontrado.
    """
    # Nome da função chamadora
    funcao_principal = inspect.stack()[1].function
    print(f"\n\nIniciando função {funcao_principal}")

    # Função interna para obter a última modificação de um arquivo ou pasta
    def obter_data_atualizacao(file):
        if os.path.exists(file):
            return os.path.getmtime(file)  # Retorna timestamp
        return None

    # Função interna para exibir os logs de atualização
    def mostrar_atualizacao_base(file):
        if os.path.exists(file):
            last_modified_time = os.path.getmtime(file)
            last_update = datetime.fromtimestamp(last_modified_time).strftime('%Y-%m-%d %H:%M:%S')
            #print(f"Última atualização da base '{file}': {last_update}")
        else:
            print(f"Arquivo ou pasta não encontrado: {file}")

    # Função interna para verificar o arquivo mais recente dentro de uma pasta
    def verificar_arquivo_mais_recente_na_pasta(diretorio):
        arquivos = os.listdir(diretorio)
        arquivo_mais_recente = None
        data_mais_recente = None

        for arquivo in arquivos:
            caminho_arquivo = os.path.join(diretorio, arquivo)
            if os.path.isfile(caminho_arquivo):  # Considera apenas arquivos
                data_modificacao = obter_data_atualizacao(caminho_arquivo)
                if data_modificacao and (data_mais_recente is None or data_modificacao > data_mais_recente):
                    arquivo_mais_recente = caminho_arquivo
                    data_mais_recente = data_modificacao

        if arquivo_mais_recente is None:  # Caso a pasta esteja vazia, retorna a data da própria pasta
            return obter_data_atualizacao(diretorio)
        return data_mais_recente  # Retorna apenas a data do arquivo mais recente

    # Gera variações de caminhos a serem verificadas
    caminhos_para_verificar = [file_path]  # Inclui o caminho inicial
    if r"\MinhaTI\MinhaTI" in file_path:
        caminhos_para_verificar.append(file_path.replace(r"\MinhaTI\MinhaTI", r"\MinhaTI"))
    elif r"\\MinhaTI\\MinhaTI" in file_path:
        caminhos_para_verificar.append(file_path.replace(r"\\MinhaTI\\MinhaTI", r"\\MinhaTI"))
    elif r"\\MinhaTI" in file_path:
        caminhos_para_verificar.append(file_path.replace(r"\\MinhaTI", r"\\MinhaTI\\MinhaTI"))
    elif r"\MinhaTI" in file_path:
        caminhos_para_verificar.append(file_path.replace(r"\MinhaTI", r"\MinhaTI\MinhaTI"))

    # Remove duplicatas
    caminhos_para_verificar = list(set(caminhos_para_verificar))

    # Dicionário para armazenar as pastas e as datas do arquivo mais recente nelas
    pastas_atualizadas = {}
    arquivos_validos = {}  # Para armazenar arquivos válidos com suas datas

    base_encontrada = False  # Flag para verificar se algum arquivo ou pasta foi encontrado

    for caminho in caminhos_para_verificar:
        if os.path.exists(caminho):
            base_encontrada = True
            mostrar_atualizacao_base(caminho)
            if os.path.isdir(caminho):  # Se for uma pasta
                # Verifica os arquivos da pasta e obtém a data mais recente
                data_mais_recente = verificar_arquivo_mais_recente_na_pasta(caminho)
                if data_mais_recente:
                    pastas_atualizadas[caminho] = data_mais_recente
            else:  # Se for um arquivo
                arquivos_validos[caminho] = obter_data_atualizacao(caminho)

    # Organiza e encontra o arquivo ou pasta mais recente
    if arquivos_validos:
        arquivo_mais_recente = max(arquivos_validos, key=arquivos_validos.get)
        last_modified_time = os.path.getmtime(arquivo_mais_recente)
        last_update = datetime.fromtimestamp(last_modified_time).strftime('%Y-%m-%d %H:%M:%S')
        print(f"Arquivo mais recente: {arquivo_mais_recente} : {last_update}")
        return arquivo_mais_recente
    elif pastas_atualizadas:
        pasta_mais_recente = max(pastas_atualizadas, key=pastas_atualizadas.get)
        last_modified_time = os.path.getmtime(pasta_mais_recente)
        last_update = datetime.fromtimestamp(last_modified_time).strftime('%Y-%m-%d %H:%M:%S')
        print(f"Pasta mais recente: {pasta_mais_recente} : {last_update}")
        return pasta_mais_recente
    else:
        print("Erro: Nenhum arquivo ou pasta válido encontrado.")
        return file_path


def carregar_df_monitoramento_SGPA3():
    dfm = pd.read_excel(verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\CIA Analytics - BOT CIA\Extrator\SGPA3\monitoramento_sgpa3.xlsx'))
    return dfm

con = sqlite3.connect(r"C:\CIAANALYTICS\1 - Producao\1 4 - Banco\envio_msg.db")
#con = sqlite3.connect(r'\\CSCLSFSR01\Agricola$\Logistica Agroindustrial\CIA 22.23\11. Analytics\1 4 - Banco\envio_msg.db")
print('Conectado ao banco SQL')
sleep(1)
cur = con.cursor()

# Zerando envios que existam pendentes
cur.execute('''
                  UPDATE envio_msg
                    SET 
                        envio_status = 2
                    WHERE
                        envio_status = 0;
                  ''')
cur.execute('''COMMIT;''')

def calcular_tempo(row):
    try:
        if '0 DIA(S) 00:00:00' in row:
            return timedelta(0)
        row_s = row.replace(': ',':').split(' ')
        hh, mm, ss = [int(x) for x in row_s[2:][0].split(':')]
        dias = timedelta(days=int(row_s[0]), hours=hh, minutes=mm, seconds=ss)
        return dias
    except:
        return timedelta(0)

def verificar_tipo_de_contato(contato_referência):
    if len(''.join(e for e in str(contato_referência) if e.isdigit())) >= 8: # Se for Contato
        return ''.join(e for e in str(contato_referência) if e.isalnum()), 'Contato'
    elif len(''.join(e for e in str(contato_referência) if e.isalpha())) >= 4 or 'ID' in str(contato_referência): # Se for Grupo
        return str(contato_referência), 'Grupo'
    else:  # Se NÃO FOR NADA NA EXISTÊNCIA DO UNIVERSO
        return 'BOT CIA Out-Put ID999', 'Grupo'

def gravar_em_banco_para_envio(listas_de_6_valores):
    global con, cur
    # Dados do banco SQL
    #  Colunas -> gerada_por, gerada_em, para_, destino, mensagem, anexo, envio_status
    #     gerada_por -> STR Nome do robô que gerou a informação
    #     gerada_em -> Data/Hora da geração da mensagem
    #     para_ -> STR Nome/Número do contato
    #     destino -> Grupo/Contato
    #     mensagem -> STR Conteúdo a ser escrito
    #     anexo -> STR Caminho do arquivo \\ a ser enviado como anexo
    #     envio_status -> 0 Não enviado / 1 Enviado
    try: 
        cur.execute('BEGIN TRANSACTION')
        cur.executemany("INSERT INTO envio_msg VALUES(?, ?, ?, ?, ?, ?, 0)", listas_de_6_valores)
        cur.execute('COMMIT')
        # con.commit()
    except Exception as error: 
        #print(len(listas_de_6_valores))
        #print(listas_de_6_valores)
        print(f'\n--> ERRO GRAVE em gravação de info no banco de dados SQL, erro:\n{error}\nMensagem que tentamos gravar:\n{listas_de_6_valores}\n')
        sleep(1)
        con.close()
        sleep(1)
        con = sqlite3.connect(r"C:\CIAANALYTICS\1 - Producao\1 4 - Banco\envio_msg.db")
        #con = sqlite3.connect(r"\\CSCLSFSR01\Agricola$\Logistica Agroindustrial\CIA 22.23\11. Analytics\1 4 - Banco\envio_msg.db")
        print('Novamente conectado ao banco SQL')
        cur = con.cursor()
        sleep(1)


# Lógica dados:
apontamentos_manutencao = [
'216 - Manutenção Corretiva',
'229 - Manutenção Corretiva Implement',
'1108 - Man Corretiva - Mecanica',
'1110 - Man Corretiva - Eletrica',
'1117 - Man Corretiva - Acidente',
'1106 - Man Corret - Aguard Pecas',
'1106 - Man Corret Aguard Pecas',
'1106 - Man Corret Aguard Peças',
'1112 - Man Corret - Borracharia',
'1114 - Man Corret - Lubrificação',
'1118 - Man Corret - Oportunidade',
'1106 - Man Corret - Aguard Peças',
'1107 - Man Preven - Aguard Peças',
'1109 - Man Preventiva - Mecanica',
'1111 - Man Preventiva - Eletrica',
'1113 - Man Preven - Borracharia',
'1115 - Man Preven - Lubrificação',
'1115 - Man Preven - Lubrificacao',
'1116 - Man Preventiva - Inspeção',
'1116 - Man Preventiva - Inspecao',
'1119 - Man Preven - Oportunidade',
'844 - Manutenção no RTK',
'844 - Manutenção TO',
'1063 - Aguardando Manutenção',
'1401 - Aguardando Manutenção TO',
'1401 - Aguardando Manutencao TO',
'1063 - Aguardando MANUTENCAO',
'1396 - Manut Cerca Eletronica']

apontamentos_manutencao_corretiva = [
'216 - Manutenção Corretiva',
'229 - Manutenção Corretiva Implement',
'1108 - Man Corretiva - Mecanica',
'1110 - Man Corretiva - Eletrica',
'1117 - Man Corretiva - Acidente',
'1106 - Man Corret - Aguard Pecas',
'1106 - Man Corret Aguard Pecas',
'1106 - Man Corret Aguard Peças',
'1112 - Man Corret - Borracharia',
'1114 - Man Corret - Lubrificação',
'1118 - Man Corret - Oportunidade',
'844 - MANUTENCAO TO',
'1106 - Man Corret - Aguard Peças',
'1063 - Aguardando Manutenção',
'844 - Manutenção no RTK',
'844 - Manutenção TO',
'844 - Manutencao TO',
'1063 - Aguardando Manutenção',
'1063 - Aguardando MANUTENCAO',
'1401 - Aguardando Manutenção TO',
'1401 - Aguardando Manutencao TO',
'1396 - Manut Cerca Eletronica']

apontamentos_manutencao_preventiva = [
'1107 - Man Preven - Aguard Peças',
'1109 - Man Preventiva - Mecanica',
'1111 - Man Preventiva - Eletrica',
'1113 - Man Preven - Borracharia',
'1115 - Man Preven - Lubrificação',
'1115 - Man Preven - Lubrificacao',
'1116 - Man Preventiva - Inspeção',
'1116 - Man Preventiva - Inspecao',
'1119 - Man Preven - Oportunidade']

contato_manutencao_torre = {
'BEN': '19 99963-7756',
'SER': '19 99676-8630',
'ZAN': '19 99676-8630',
'BAR': '19 97150-1712',
'USF': '19 99905-3381',
'GAS': '19 97151-9785',
'COP': '19 99905-3381',
'BON': '16 99792-7368',
'Sem': '19 99832-6554',
'Sem': '16 99138-5261',
'USC': '19 99905-3381',
'DDC': '19 97150-1712',
'PAR': '19 99905-3381',
'UNI': '19 99963-7756',
'DES': '19 99963-7756',
'JUN': '16 99792-7368',
'IPA': '19 97150-1712',
'MUN': '19 97151-9785',
'JAT': '19 97151-9785',
'UPA': '19 99905-3381',
'DIA': '19 97150-1712',
'EMP': '19 99832-6554',
'USH': '19 99905-3381',
'RAF': '19 99905-3381',
'Sem': '19 99832-6554',
'CAA': '19 99736-6501',
'RBR': '19 99832-6554',
'PTP': '19 99832-6554',
'VRO': '19 97103-1004',
'LPT': '19 99832-6554',
'LEM': '19 99832-6554',
'SEL': '16 99792-7368',
'CNT': '16 99792-7368',
'UMB': '19 97103-1004', }

def Controle_envio_manutencao():
    global contatos_manutencao, lista_FRENTES_envio_manutencao
    while True:
        try:
            contatos_manutencao = pd.read_excel(r"\\CSCLSFSR01\Agricola$\Logistica Agroindustrial\CIA 22.23\11. Analytics\BOT CIA\Contatos\Contatos_BOT_CIA_Manutencao.xlsx",sheet_name='Contatos')
            break
        except:
            print('erro em abrir Contatos_BOT_CIA_Manutencao.xlsx')
            sleep(1)
    contatos_manutencao = contatos_manutencao[["Frente", "T_Manutencao"]]
    lista_FRENTES_envio_manutencao = contatos_manutencao.Frente.tolist()
    contatos_manutencao = contatos_manutencao.set_index('Frente').to_dict()['T_Manutencao']
    return contatos_manutencao, lista_FRENTES_envio_manutencao

def gerar_mensagens_manutencao_unidade(data,frente):
    global df_com, lista_manutencao
    lista_manutencao = []
    
    y = data[data["Frente associada"]==(str(frente))]
        # Tenta converter "Número do Equipamento" para inteiro sem modificar valores inválidos
    try:
        y["Número do Equipamento"] = y["Número do Equipamento"].astype("Int64")  # Mantém NaN e converte números corretamente
    except:
        pass 
    for x in range(y.shape[0]):
        x += 1
        lista_manutencao.append(f"❌⚠️ *Apontamento Manutenção!*")
        lista_manutencao.append(f"*Frente:* {y.iloc[x-1]['Frente associada']}")
        lista_manutencao.append(f"*Frota:* {int(y.iloc[x-1]['Número do Equipamento'])}")
        lista_manutencao.append(f"*Tipo:* {y.iloc[x-1]['Tipo do equipamento']}")
        lista_manutencao.append(f"*Apontamento:* {y.iloc[x-1]['Atividade']}")
        lista_manutencao.append(f"⏱️ _Ultima comunicação: {y.iloc[x-1]['Registro mais recente']}_")
        lista_manutencao.append('\n')
    print('\n'.join(map(str, lista_manutencao)))

def gerar_mensagens_manutencao_unidade_prev(data,frente):
    global df_com, lista_manutencao
    lista_manutencao = []
    y = data[data["Frente associada"]==(str(frente))]
    for x in range(y.shape[0]):
        x += 1
        lista_manutencao.append(f"⚠️ *Apontamento Manutenção!*")
        lista_manutencao.append(f"*Frente:* {y.iloc[x-1]['Frente associada']}")
        lista_manutencao.append(f"*Frota:* {int(y.iloc[x-1]['Número do Equipamento'])} / Tipo: {y.iloc[x-1]['Tipo do equipamento']}")
        lista_manutencao.append(f"*Apontamento:* {y.iloc[x-1]['Atividade']}")
        lista_manutencao.append(f"⏱️ _Comunicação: {y.iloc[x-1]['Registro mais recente']}_")
        lista_manutencao.append('\n')
    print('\n'.join(map(str, lista_manutencao)))

def atualizacao_df():
    global df
    tick = 0
    while tick < 1:
        try:
            df = pd.read_excel(verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\CIA Analytics - BOT CIA\Extrator\AGRON\agron_comunicacao.xlsx'))
            tick = 1
        except:
            sleep(5)
            pass

atualizacao_df() # Necessário atualizar para termos um dataframe inicial
df.to_excel("data.xlsx")
# Declaração:
df_com = df_com = df[df["Atividade"].isin(apontamentos_manutencao)]
df_com15 = df_com 
df_com10 = df_com 
df_com5 = df_com 
   
# Variavel de controle do limite de manutenção
def controle_limite_manutencao_inicio(): # df_limitador_man e df_limitador_man_prev
    global df, df_limitador_man, df_limitador_man_prev
    Limitador_manutencao = df["Número do Equipamento"].unique()
    df_limitador_man = pd.DataFrame(Limitador_manutencao, columns = ['Frota'])
    data_base_lps = []
    for n in range(len(Limitador_manutencao)):
        data_base_lps.append(datetime(1999,3,12,7,7,7))
    df_limitador_man = df_limitador_man.assign(ultimo_envio = data_base_lps)
    df_limitador_man['ultimo_envio'] = pd.to_datetime(df_limitador_man['ultimo_envio'])
    df_limitador_man_prev = df_limitador_man
    return df_limitador_man, df_limitador_man_prev

def controle_limite_manutencao_prev():
    global df_limitador_man_prev, df_com_e_prev
    agora = datetime.now()
    agorax = agora-timedelta(minutes=30)
    env = []  
    df_env = df_limitador_man_prev[df_limitador_man_prev["ultimo_envio"] <= agorax]
    env = list(df_env["Frota"].unique())
    df_com_e_prev = df_com_e_prev[df_com_e_prev["Número do Equipamento"].isin(env)]
    for n in df_com_e_prev["Número do Equipamento"]:
        df_limitador_man_prev.loc[df_limitador_man_prev["Frota"]==n, 'ultimo_envio'] = agora

def controle_limite_manutencao():
    global df_limitador_man, df_com_e
    agora = datetime.now()
    agorax = agora-timedelta(minutes=30)
    env = []
    df_env = df_limitador_man[df_limitador_man["ultimo_envio"] <= agorax]
    env = list(df_env["Frota"].unique())
    df_com_e = df_com_e[df_com_e["Número do Equipamento"].isin(env)]
    for n in df_com_e["Número do Equipamento"]:
        df_limitador_man.loc[df_limitador_man["Frota"]==n, 'ultimo_envio'] = agora
   
df_com_e = pd.DataFrame({'' : []})
df_com_e_prev = pd.DataFrame({'' : []})
df_com_prev = pd.DataFrame({'' : []})

def gerar_mensagens_manutencao_julgamento_frente(contato_envio, frente, data,tipo_cenario):
    global lista_manutencao_julg
    numero_frentes_cct = ['615','581','583','582','703','612','705','552','663','551','701','704','613','616','611','614','553','661','662','702','706','424','363','361','465','463','362','423','461','466','464','462','364','422','421','805','833','801','802','804','803','831','832','834','835','261','262','052','137','051','002','001','138','003','005','004','136','352','524','353','351','945','514','511','513','512','753','754','755','756','946','941','942','944','750','747','760','749','523','521','525','522','531','455','457','563','913','539','409','413','432','383','381','570','560','569','571','568','565','561','915','933','937','434','415','435','329','745','746','742','744','743','533','452','454','534','489','492','387','390','384','939','930','934','935','972','938','575','940','588','493','567','537','536','323','328','330','335','775','859','597','595','999']
    procure_por_oportunidade = ['Oportunidade', 'Chuva', 'Limitação']
    if tipo_cenario == 1:
        pontoA = "REAL"
        pontoB = "OPORTUNIDADE"
        data_envio = data[~data['Atividade'].str.contains('|'.join(procure_por_oportunidade))]
        print(data)
    elif tipo_cenario == 2:
        pontoA = "OPORTUNIDADE"
        pontoB = "REAL"
        data_envio = data[data['Atividade'].str.contains('|'.join(procure_por_oportunidade))]
    if '-RE-' in frente and frente[-3:] in (numero_frentes_cct):
        frente = str(frente).replace('-RE-','-MO-')
    lista_manutencao_julg = []
    y = data_envio[data_envio["Frente associada"]==(str(frente))]
    for x in range(y.shape[0]):
        x += 1
        lista_manutencao_julg.append(f"⚠️ *ALERTA:* Verificar Apontamento X Realidade")
        lista_manutencao_julg.append(f"*Frente:* {y.iloc[x-1]['Frente associada']}")
        lista_manutencao_julg.append(f"*Frota:* {int(y.iloc[x-1]['Número do Equipamento'])} / {y.iloc[x-1]['Tipo do equipamento']}")
        lista_manutencao_julg.append(f"*Apontamento:* {y.iloc[x-1]['Atividade']}")
        lista_manutencao_julg.append(f"_Frota está apontando {pontoA} porém na frente consta {pontoB}_")
        lista_manutencao_julg.append('\n')
    contato, tipo_contato = verificar_tipo_de_contato(contato_envio)
    gravar_em_banco_para_envio([('MANUT_Apto_Compliance',datetime.now(),contato, tipo_contato,'\n'.join(lista_manutencao_julg),'')])

controle_limite_manutencao_inicio()

# Controlar contatos em planilha excel para o envio:
def Controle_envio_1f(): #sheet_names: 1f_cct  1f_comb  1f_vn
    global contatos_cct_torre, contatos_comb_torre, contatos_vn_torre, filtro_contatos_cct_torre, filtro_contatos_comb_torre, filtro_contatos_vn_torre
        # CCT
    contatos_cct_torre = pd.read_excel(r"\\CSCLSFSR01\Agricola$\Logistica Agroindustrial\CIA 22.23\11. Analytics\BOT CIA\Contatos\Contatos_BOT_CIA_Manutencao.xlsx",sheet_name='1f_cct')
    contatos_cct_torre = contatos_cct_torre[["Frente", "Envio"]]
    filtro_contatos_cct_torre = contatos_cct_torre.Frente.tolist()
    contatos_cct_torre = contatos_cct_torre.set_index('Frente').to_dict()['Envio']
        # COMBOIO
    contatos_comb_torre = pd.read_excel(r"\\CSCLSFSR01\Agricola$\Logistica Agroindustrial\CIA 22.23\11. Analytics\BOT CIA\Contatos\Contatos_BOT_CIA_Manutencao.xlsx",sheet_name='1f_comb')
    contatos_comb_torre = contatos_comb_torre[["Frente", "Envio"]]
    contatos_comb_torre = contatos_comb_torre.dropna()
    filtro_contatos_comb_torre = contatos_comb_torre.Frente.tolist()
    contatos_comb_torre = contatos_comb_torre.set_index('Frente').to_dict()['Envio']
        # VN
    contatos_vn_torre = pd.read_excel(r"\\CSCLSFSR01\Agricola$\Logistica Agroindustrial\CIA 22.23\11. Analytics\BOT CIA\Contatos\Contatos_BOT_CIA_Manutencao.xlsx",sheet_name='1f_vn')
    contatos_vn_torre = contatos_vn_torre[["Frente", "Envio"]]
    filtro_contatos_vn_torre = contatos_vn_torre.Frente.tolist()
    contatos_vn_torre = contatos_vn_torre.set_index('Frente').to_dict()['Envio']    
    return contatos_cct_torre, contatos_comb_torre, contatos_vn_torre, filtro_contatos_cct_torre, filtro_contatos_comb_torre, filtro_contatos_vn_torre

#################### MOD PROD VN BT - COMPLIANCE
apontamentos_compl_prod_vn = ['880 - Manut. por Oportunidade', '1118 - Man Corret - Oportunidade', '1119 - Man Preven - Oportunidade']
apontamentos_oportunidade_vn = ['880 - Manut. por Oportunidade', '1118 - Man Corret - Oportunidade', '1119 - Man Preven - Oportunidade', '208 - Chuva Solo Umido', '227 - Limitação Indústria', '233 - Vento', '977 - Solo Seco']
df_prod_vn = df[df["Atividade"].isin(apontamentos_compl_prod_vn)]
df_prod_5 = df_prod_vn 
df_vn_env = df_prod_vn

def mensagem_vn_1f(data,frente):
    global df_vn_env, lista_1f_vn
    lista_1f_vn = []
    y = data[data["Frente associada"]==(str(frente))]
    for x in range(y.shape[0]):
        x += 1
        lista_1f_vn.append(f"⚠️❓ *Apontamento de Oportunidade!*")
        lista_1f_vn.append(f"*Frente:* {y.iloc[x-1]['Frente associada']}")
        lista_1f_vn.append(f"*Frota:* {int(y.iloc[x-1]['Número do Equipamento'])}")
        lista_1f_vn.append(f"*Tipo:* {y.iloc[x-1]['Tipo do equipamento']}")
        lista_1f_vn.append(f"*Apontamento:* {y.iloc[x-1]['Atividade']}")
        lista_1f_vn.append(f"⏱️ _Ultima comunicação: {y.iloc[x-1]['Registro mais recente']}_")
        lista_1f_vn.append('\n')
    return '\n'.join(map(str, lista_1f_vn))

def atualizar_vn_df():
    global df, df_prod_vn, df_prod_5, apontamentos_compl_prod_vn, df_vn_env
    df_prod_5 = df_prod_vn
    df_prod_vn = df[df["Atividade"].isin(apontamentos_compl_prod_vn)]
    df_prod_vn = df_prod_vn[df_prod_vn["Frente associada"].str.contains("-VN-")]
    relativo = df_prod_5['Número do Equipamento'].unique().tolist()
    df_vn_env = df_prod_vn[~df_prod_vn["Número do Equipamento"].isin(relativo)]
    lista_envio = list(df_vn_env["Frente associada"].unique())
    for n in lista_envio:
        print("\n****Envio para:",n)
        contato, tipo_contato = verificar_tipo_de_contato(contatos_vn_torre[n[:3]])
        gravar_em_banco_para_envio([('PROD_Compliance_Vinhaca',datetime.now(),contato, tipo_contato,mensagem_vn_1f(df_vn_env,n),'')])
        contato, tipo_contato = verificar_tipo_de_contato('CIA Produção ID999') # Roberta Santiago
        gravar_em_banco_para_envio([('DEBUG_PROD_Compliance_Vinhaca',datetime.now(),contato, tipo_contato,mensagem_vn_1f(df_vn_env,n),'')])
        print("\n****Envio de mensagem feito\n")
    print('Monitoramento Vinhaça OK')

################### MOD Comboio - Falta de combustível
apontamentos_comboio = ['211 - Falta de Combustível / Lubrif.', '211 - Falta de Combustível', '211 -']
df_comb = df[df["Atividade"].str.contains('|'.join(apontamentos_comboio))]
df_comb_5 = df_comb 
df_comb_env = df_comb

def mensagem_comboio_1f(data,frente):
    global df_comb_env, lista_1f_comb
    lista_1f_comb = []
    y = data[data["Frente associada"]==(str(frente))]
    for x in range(y.shape[0]):
        x += 1
        lista_1f_comb.append(f"❌⛽ *Apontamento de PANE SECA!*")
        lista_1f_comb.append(f"*Frente:* {y.iloc[x-1]['Frente associada']}")
        lista_1f_comb.append(f"*Frota:* {y.iloc[x-1]['Número do Equipamento']}")
        lista_1f_comb.append(f"*Tipo:* {y.iloc[x-1]['Tipo do equipamento']}")
        lista_1f_comb.append(f"*Apontamento:* {y.iloc[x-1]['Atividade']}")
        lista_1f_comb.append(f"⏱️ _Ultima comunicação: {y.iloc[x-1]['Registro mais recente']}_")
        lista_1f_comb.append('\n')
    return '\n'.join(map(str, lista_1f_comb))

def atualizar_comb_df():
    global df, df_comb, df_comb_5, apontamentos_comboio, df_comb_env
    df_comb_5 = df_comb
    df_comb = df[df["Atividade"].str.contains('|'.join(apontamentos_comboio))]
    #df_comb['Tempo em atividade'] = pd.to_timedelta(df_comb['Tempo em atividade'])
    df_comb['Tempo em atividade'] = pd.to_timedelta(df_comb['Tempo em atividade']*3600*24, unit='s')
    df_comb = df_comb[(df_comb["Frente associada"].str[:3].isin(contatos_comb_torre.keys()))]# & (~df_comb["Frente associada"].str.contains('Sem '))]
    df_comb = df_comb[df_comb['Tempo em atividade'] > timedelta(minutes=10)]
    relativo = df_comb_5['Número do Equipamento'].unique().tolist()
    df_comb_env = df_comb[~df_comb["Número do Equipamento"].isin(relativo)]
    lista_envio = list(df_comb_env["Frente associada"].unique())
    for n in lista_envio:
        print("\n****Envio para:",n)
        contato, tipo_contato = verificar_tipo_de_contato('Report Pane-Seca')
        gravar_em_banco_para_envio([('COMBOIO_Pane_Seca',datetime.now(),contato, tipo_contato,mensagem_comboio_1f(df_comb_env,n),'')])
        print("\n****Envio de mensagem feito\n")
    print('Monitoramento Comboio OK')


####

def envio_mensagem_manut_1func():
    global df, df_com, df_com5, df_com10, df_com10, df_com15, apontamentos_manutencao_corretiva, df_com_e, df_com_prev, lista_FRENTES_envio_manutencao
    Controle_envio_manutencao()
    df_com15 = df_com10
    df_com10 = df_com5
    df_com5 = df_com
    df_com = df[df["Atividade"].isin(apontamentos_manutencao_corretiva)]
    df_com = df_com[df_com["Frente associada"].isin(lista_FRENTES_envio_manutencao)]
    df_com = df_com[df_com["Tipo do equipamento"] != "Caminhão"]
    df_com = df_com[df_com["Tipo do equipamento"] != "Caminhao Canavieiro"]
    relativo = df_com5['Número do Equipamento'].unique().tolist()
    df_com_e = df_com[~df_com["Número do Equipamento"].isin(relativo)]
    controle_limite_manutencao()
    #controle_limite_manutencao_prev()
    lista_envio = list(df_com_e["Frente associada"].unique())
    # Corretiva envio:
    for n in lista_envio:
        gerar_mensagens_manutencao_unidade(df_com_e,n)
        contato, tipo_contato = verificar_tipo_de_contato(contatos_manutencao[n])
        gravar_em_banco_para_envio([('MANUT_Apto_Manutencao',datetime.now(),contato, tipo_contato,'\n'.join(lista_manutencao),'')])
        if 'JAT-' in n:
            contato, tipo_contato = verificar_tipo_de_contato('Apontamentos de manutenção ')
            gravar_em_banco_para_envio([('MANUT_Apto_Manutencao',datetime.now(),contato, tipo_contato,'\n'.join(lista_manutencao),'')])
        # Lógica julgamentos
        apontamento_caso = df_com_e[df_com_e['Frente associada']==n]
        apontamento_caso = list(apontamento_caso.Atividade)
        cenario_atual_f_julg = df[df['Frente associada'].str[3:]==n[3:]]
        # Periodo
        dia_atual_fuso_SP = str(datetime.now()-timedelta(hours=0))
        data_atual_ref_fuso_SP = dia_atual_fuso_SP[:4]+'-'+dia_atual_fuso_SP[5:7]+'-'+dia_atual_fuso_SP[8:10]+' '+dia_atual_fuso_SP[11:13]
        dia_atual_fuso_SP1 = str(datetime.now()-timedelta(hours=1))
        data_atual_ref_fuso_SP1 = dia_atual_fuso_SP1[:4]+'-'+dia_atual_fuso_SP1[5:7]+'-'+dia_atual_fuso_SP1[8:10]+' '+dia_atual_fuso_SP1[11:13]
        dia_atual_fuso_SP2 = str(datetime.now()-timedelta(hours=2))
        data_atual_ref_fuso_SP2 = dia_atual_fuso_SP2[:4]+'-'+dia_atual_fuso_SP2[5:7]+'-'+dia_atual_fuso_SP2[8:10]+' '+dia_atual_fuso_SP2[11:13]
        #cenario_atual_f_julg = cenario_atual_f_julg[cenario_atual_f_julg['Registro mais recente'].str.contains(data_atual_ref_fuso_SP+'|'+data_atual_ref_fuso_SP1+'|'+data_atual_ref_fuso_SP2+'|'+str(data_atual_ref_fuso_SP)[:14]+'|'+str(data_atual_ref_fuso_SP1)[:14]+'|'+str(data_atual_ref_fuso_SP2)[:14])]
        procure_por_oportunidade = ['Oportunidade', 'Chuva', 'Limitação']
        cenario_atual_f_julg = cenario_atual_f_julg[cenario_atual_f_julg['Atividade'].str.contains('|'.join(procure_por_oportunidade))]
        cenario_atual_f_julg = list(cenario_atual_f_julg['Número do Equipamento'])
        if 'Oportunidade' in apontamento_caso and len(cenario_atual_f_julg) == 0:
            gerar_mensagens_manutencao_julgamento_frente(contato_manutencao_torre[n[:3]],n,df_com_e,2)
        if not 'Oportunidade' in apontamento_caso and len(cenario_atual_f_julg) > 0:
            gerar_mensagens_manutencao_julgamento_frente(contato_manutencao_torre[n[:3]],n,df_com_e,1)
    sleep(0.5)
    
def envio_mensagem_manut_1func_prev():
    global df, df_com_prev, df_com_prev_5, apontamentos_manutencao_preventiva, df_com_e_prev, lista_FRENTES_envio_manutencao
    if len(df_com_prev) == 0:
        print("Comparação VAZIA OK!!!!")
        df_com_prev_5 = df[df["Atividade"].isin(apontamentos_manutencao_preventiva)]
    else:
        df_com_prev_5 = df_com_prev
    df_com_prev = df[df["Atividade"].isin(apontamentos_manutencao_preventiva)]
    Controle_envio_manutencao()
    df_com_prev = df_com_prev[df_com_prev["Frente associada"].isin(lista_FRENTES_envio_manutencao)]
    df_com_prev = df_com_prev[df_com_prev["Tipo do equipamento"] != "Caminhão"]
    df_com_prev = df_com_prev[df_com_prev["Tipo do equipamento"] != "Caminhao Canavieiro"]
    relativo = df_com_prev_5['Número do Equipamento'].unique().tolist()
    df_com_e_prev = df_com_prev[~df_com_prev["Número do Equipamento"].isin(relativo)]
    controle_limite_manutencao_prev()
    lista_envio = list(df_com_e_prev["Frente associada"].unique())
    # Corretiva envio:
    for n in lista_envio:
        gerar_mensagens_manutencao_unidade_prev(df_com_e_prev,n)
        contato, tipo_contato = verificar_tipo_de_contato(contatos_manutencao[n])
        gravar_em_banco_para_envio([('MANUT_Apto_Manutencao',datetime.now(),contato, tipo_contato,'\n'.join(lista_manutencao),'')])
        if 'JAT-' in n:
            contato, tipo_contato = verificar_tipo_de_contato('Apontamentos de manutenção ')
            gravar_em_banco_para_envio([('MANUT_Apto_Manutencao',datetime.now(),contato, tipo_contato,'\n'.join(lista_manutencao),'')])
        # Lógica julgamentos
        apontamento_caso = df_com_e_prev[df_com_e_prev['Frente associada']==n]
        apontamento_caso = list(apontamento_caso.Atividade)
        cenario_atual_f_julg = df[df['Frente associada'].str[3:]==n[3:]]
        # Periodo
        dia_atual_fuso_SP = str(datetime.now()-timedelta(hours=0))
        data_atual_ref_fuso_SP = dia_atual_fuso_SP[:4]+'-'+dia_atual_fuso_SP[5:7]+'-'+dia_atual_fuso_SP[8:10]+' '+dia_atual_fuso_SP[11:13]
        dia_atual_fuso_SP1 = str(datetime.now()-timedelta(hours=1))
        data_atual_ref_fuso_SP1 = dia_atual_fuso_SP1[:4]+'-'+dia_atual_fuso_SP1[5:7]+'-'+dia_atual_fuso_SP1[8:10]+' '+dia_atual_fuso_SP1[11:13]
        dia_atual_fuso_SP2 = str(datetime.now()-timedelta(hours=2))
        data_atual_ref_fuso_SP2 = dia_atual_fuso_SP2[:4]+'-'+dia_atual_fuso_SP2[5:7]+'-'+dia_atual_fuso_SP2[8:10]+' '+dia_atual_fuso_SP2[11:13]
        #cenario_atual_f_julg = cenario_atual_f_julg[cenario_atual_f_julg['Registro mais recente'].str.contains(data_atual_ref_fuso_SP+'|'+data_atual_ref_fuso_SP1+'|'+data_atual_ref_fuso_SP2+'|'+str(data_atual_ref_fuso_SP)[:14]+'|'+str(data_atual_ref_fuso_SP1)[:14]+'|'+str(data_atual_ref_fuso_SP2)[:14])]
        procure_por_oportunidade = ['Oportunidade', 'Chuva', 'Limitação']
        cenario_atual_f_julg = cenario_atual_f_julg[cenario_atual_f_julg['Atividade'].str.contains('|'.join(procure_por_oportunidade))]
        cenario_atual_f_julg = list(cenario_atual_f_julg['Número do Equipamento'])
        if 'Oportunidade' in apontamento_caso and len(cenario_atual_f_julg) == 0:
            gerar_mensagens_manutencao_julgamento_frente(contato_manutencao_torre[n[:3]],n,df_com_e_prev,1)
        if not 'Oportunidade' in apontamento_caso and len(cenario_atual_f_julg) > 0:
            gerar_mensagens_manutencao_julgamento_frente(contato_manutencao_torre[n[:3]],n,df_com_e_prev,1)
    sleep(0.5)

def mensagem_vn_2f_sa(data,frente):
    global lista_2f_vn_sa
    lista_2f_vn_sa = []
    y = data[data["Frente associada"]==(str(frente))]
    for x in range(y.shape[0]):
        x += 1
        lista_2f_vn_sa.append(f"⚠️ *ATENÇÃO:* Frota *Sem Apontamento*")
        lista_2f_vn_sa.append(f"*Frente:* {y.iloc[x-1]['Frente associada']}")
        lista_2f_vn_sa.append(f"*Frota:* {int(y.iloc[x-1]['Número do Equipamento'])} / *Tipo:* {y.iloc[x-1]['Tipo do equipamento']}")
        lista_2f_vn_sa.append(f"⏱️ _Ultima comunicação: {str(y.iloc[x-1]['Registro mais recente'])[:16]}_")
        lista_2f_vn_sa.append(f"⏱️ _Duração atividade: {y.iloc[x-1]['Tempo em atividade']}_")
        lista_2f_vn_sa.append('\n')
    return '\n'.join(map(str, lista_2f_vn_sa))

def atualizar_contatos_2f_prod():
    global contatos_sem_apontamento, dict_contatos_sem_apontamento, contatos_Monit_Dados, dict_contatos_Monit_Dados
    try:
        # Base REF Prod
        ref_prod = pd.read_excel(r'\\CSCLSFSR01\Agricola$\Logistica Agroindustrial\CIA 22.23\11. Analytics\BOT CIA\Contatos\REF Prod.xlsx')
        ref_prod = ref_prod[['Frente','Celular_Torre','ID_Grupo','Envio_Sem_AP','Envio_Monit_Dados']]
        # Contatos Envio Sem Apontamento
        contatos_sem_apontamento = ref_prod[ref_prod.Envio_Sem_AP == 'SIM']
        contatos_sem_apontamento = contatos_sem_apontamento[['Frente','Celular_Torre','ID_Grupo']]
        dict_contatos_sem_apontamento = dict(zip(contatos_sem_apontamento.Frente, contatos_sem_apontamento.Celular_Torre))
        # Contatos Envio Monit Dados
        contatos_Monit_Dados = ref_prod[ref_prod.Envio_Monit_Dados == 'SIM']
        contatos_Monit_Dados = contatos_Monit_Dados[['Frente','Celular_Torre','ID_Grupo']]
        dict_contatos_Monit_Dados = dict(zip(contatos_Monit_Dados.Frente, contatos_Monit_Dados.Celular_Torre))
        print(f'As {datetime.now()} | Atualizado contatos!')
    except:
        print(f'As {datetime.now()} | Não foi possível atualizar contatos!!!')
        pass

def atualizar_vn_df_2(data_frame):
    global frotas_vn_2f
    atualizar_contatos_2f_prod()
    try: frotas_vn_2f
    except: frotas_vn_2f = {x: (datetime(1999,3,12,7,7,7)) for x in list(data_frame['Número do Equipamento'].unique())}
    # Associando data antiga para resetar indice.
    df_prod_sem_ap = data_frame[data_frame["Atividade"].isin(['834 - Sem apontamento'])]
    df_prod_sem_ap = df_prod_sem_ap[df_prod_sem_ap["Frente associada"].str.contains('-MU-|-PL-|-VN-|-BT-')]
    df_prod_sem_ap = df_prod_sem_ap[df_prod_sem_ap["Frente associada"].str[:6].isin(list(dict_contatos_sem_apontamento.keys()))]
    #df_prod_sem_ap['chave_tempo'] = df_prod_sem_ap["Tempo em atividade"].str[:2]
    df_prod_sem_ap["Tempo em atividade"] = pd.to_timedelta(df_prod_sem_ap["Tempo em atividade"])
    df_prod_sem_ap_env = df_prod_sem_ap[df_prod_sem_ap["Tempo em atividade"] > timedelta(minutes=5)]
    # Aqui traz somente as frotas que estão dentro do critérios acima e que tem o tempo de envio menor que o atual
    lista_envio_2f_prod = [k for k, v in frotas_vn_2f.items() if v < datetime.now() and k in list(df_prod_sem_ap_env['Número do Equipamento'])]
    df_prod_sem_ap_env = df_prod_sem_ap_env[df_prod_sem_ap_env['Número do Equipamento'].isin(lista_envio_2f_prod)]
    # lógica para controlar envio a cada X momento (Registra momento atual) / E Realizar o envio.
    for frota in list(df_prod_sem_ap_env['Número do Equipamento']):
        # Associando o tempo atual + timer, para registrar o envio = CONFIGURAR 30 MINUTOS ENTRE MENSAGENS DO MESMO GATILHO SE ELAS ESTIVEREM COMUNICANDO!!!
        envio = df_prod_sem_ap_env[df_prod_sem_ap_env['Número do Equipamento'] == frota]
        if str(envio.iloc[0]['Registro mais recente'])[5:7] == str(datetime.now())[5:7]:
            try: ult_com_2f_prod = time.strptime(str(envio.iloc[0]['Registro mais recente']), "%Y-%m-%d %H:%M:%S")
            except: ult_com_2f_prod = datetime(1999,3,12,7,7,7)
        elif str(envio.iloc[0]['Registro mais recente'])[8:10] == str(datetime.now())[8:10]:
            try: ult_com_2f_prod = time.strptime(str(envio.iloc[0]['Registro mais recente']), "%Y-%d-%m %H:%M:%S")
            except: ult_com_2f_prod = time.strptime('1999-03-12 07:07:07', "%Y-%m-%d %H:%M:%S")
        elif str(envio.iloc[0]['Registro mais recente'])[8:10] == str(datetime.now())[8:10]:
            try: ult_com_2f_prod = time.strptime(str(envio.iloc[0]['Registro mais recente']), "%Y-%d-%m %H:%M:%S")
            except: ult_com_2f_prod = time.strptime('1999-03-12 07:07:07', "%Y-%m-%d %H:%M:%S")
        else:
            try: ult_com_2f_prod = time.strptime(str(envio.iloc[0]['Registro mais recente']), "%Y-%d-%m %H:%M:%S")
            except: ult_com_2f_prod = time.strptime('1999-03-12 07:07:07', "%Y-%m-%d %H:%M:%S")
        ult_com_2f_prod = datetime(ult_com_2f_prod[0],ult_com_2f_prod[1],ult_com_2f_prod[2],ult_com_2f_prod[3],ult_com_2f_prod[4],ult_com_2f_prod[5])
        if ult_com_2f_prod > (datetime.now()-timedelta(minutes=60)) and ult_com_2f_prod < (datetime.now()+timedelta(minutes=60)):
            frotas_vn_2f[frota] = (datetime.now()+timedelta(minutes=30)) #minutes=30
            contato, tipo_contato = verificar_tipo_de_contato(dict_contatos_sem_apontamento[envio['Frente associada'].values[0][:6]])
            gravar_em_banco_para_envio([('PROD_Sem_Apontamento',datetime.now(),contato, tipo_contato,mensagem_vn_2f_sa(envio,envio['Frente associada'].values[0]),'')])
            contato, tipo_contato = verificar_tipo_de_contato('CIA Produção ID999') # Roberta Santiago
            gravar_em_banco_para_envio([('DEBUG_PROD_Sem_Apontamento',datetime.now(),contato, tipo_contato,mensagem_vn_2f_sa(envio,envio['Frente associada'].values[0]),'')])

### Plantio Horario

def gerar_mensagen_relacao_plantio_hora():
    relacao_plantio_hora = pd.read_excel(verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\CIA Analytics - BOT CIA\Extrator\PLANTIO\Plantio_Hora.xlsx'))
    if datetime.now().hour == 0: relacao_plantio_hora = relacao_plantio_hora[(relacao_plantio_hora.FG_TP_EQUIPAMENTO == 40) & (relacao_plantio_hora.DT_LOCAL == str((datetime.now()-timedelta(days=1)).date()))]
    else: relacao_plantio_hora = relacao_plantio_hora[(relacao_plantio_hora.FG_TP_EQUIPAMENTO == 40) & (relacao_plantio_hora.DT_LOCAL == str(datetime.now().date()))]
    relacao_plantio_hora.DESC_UNIDADE = relacao_plantio_hora.DESC_UNIDADE.str.capitalize()
    relacao_plantio_hora
    mensagem_a_ser_enviada = []
    mensagem_a_ser_enviada.append('🎋 *Unidades Plantando -3h*\n')
    unidades_restantes = []

    for unidade in set([f.capitalize() for f in relacao_plantio_hora.DESC_UNIDADE.unique()]):
        corte_hora = datetime.now().hour-5 if unidade in ['Caarapó', 'Rio brilhante', 'Passatempo'] else datetime.now().hour-4
        corte_ultima_hora = datetime.now().hour-2 if unidade in ['Caarapó', 'Rio brilhante', 'Passatempo'] else datetime.now().hour-1
        slice_pl_hora = relacao_plantio_hora[(relacao_plantio_hora.DESC_UNIDADE == unidade) & (relacao_plantio_hora.HR_LOCAL > corte_hora) & (relacao_plantio_hora.CD_OPERACAO == 789)]
        value_of_und = '✅' if len(slice_pl_hora)>0 and sum(slice_pl_hora.VL_HR_OPERACIONAIS.values) > 1200 else ''
        if value_of_und == '' and sum(slice_pl_hora[slice_pl_hora.HR_LOCAL == corte_ultima_hora].VL_HR_OPERACIONAIS.values) > 600: value_of_und = '✅'

        if value_of_und == '✅': mensagem_a_ser_enviada.append(f'{value_of_und} {unidade}')
        else: unidades_restantes.append(f'⬜ {unidade}')

    for und_r in unidades_restantes: mensagem_a_ser_enviada.append(und_r)

    if datetime.now().hour == 0: mensagem_a_ser_enviada.append(f'\n_Das 21h00 até 23h59._')
    else: mensagem_a_ser_enviada.append(f'\n_Das {datetime.now().hour-3}h00 até {datetime.now().hour-1}h59._')
    if len(mensagem_a_ser_enviada)>4 and '\n'.join(mensagem_a_ser_enviada).count('✅') >= 1:
        grupo = pd.read_excel(r'\\CSCLSFSR01\Agricola$\Logistica Agroindustrial\CIA 22.23\11. Analytics\BOT CIA\Contatos\Envio_pl_hora.xlsx')
        grupo = grupo.Grupo[0]
        gravar_em_banco_para_envio([('PROD_Plantio_Horario',datetime.now(), grupo, 'Grupo', '\n'.join(mensagem_a_ser_enviada), '')])
        gravar_em_banco_para_envio([('PROD_Plantio_Horario',datetime.now(), '19998326554', 'Contato', '\n'.join(mensagem_a_ser_enviada), '')])

################################################ Parte Overview_Manut:
# Logica
def sub_atualizar_formatar_PMA(caminho_downloads=r'\\csclsfsr03\SoftsPRD\Extrator\PRD\CCT\PMA',tipo_arquivo='\*csv'):
    folder_path = caminho_downloads
    file_type =  tipo_arquivo
    files = glob.glob(folder_path+file_type)
    arquivo_mais_recente = max(files, key=os.path.getctime)
    df = pd.read_csv(arquivo_mais_recente, encoding="ISO-8859-1", sep=';') #, on_bad_lines='skip'
    df = df[(df.ORIGEM != "I") & (df.ORIGEM != "T") & (df.DS_STATUS != "Recolhido") & (~df.FRENTE.isin(["-LN-"]))]
    df.DS_OPERACAO.fillna('Sem bordo', inplace=True)
    df = df[df["DS_STATUS"]!="Concluído"]
    for emp in df[df.FRENTE.isnull()].iterrows():
        if str(emp[1][12])[:11] == '' or len(str(emp[1][12])[:11].split('-')) != 3:
            df.loc[emp[0],'FRENTE'] = 'Sem frente'
        else:
            df.loc[emp[0],'FRENTE'] = str(emp[1][12])[:11].strip()
    return df

def basePMA_Atualizada():
    folder_path = r'\\csclsfsr03\SoftsPRD\Extrator\PRD\CCT\PMA'
    file_type =  '\*csv'
    files = glob.glob(folder_path+file_type)
    arquivo_mais_recente = max(files, key=os.path.getctime)
    if datetime.fromtimestamp(os.path.getctime(arquivo_mais_recente)) > datetime.now()-timedelta(minutes=30):
        return True
    else: False

def reducao_data(data_referencia):
    if data_referencia == 'Sem Previsão':
        return 'Sem Previsão'
    else:
        pass
        try:
            if data_referencia.day != datetime.now().day and data_referencia.month == datetime.now().month:
                dia = data_referencia.day
                mes = data_referencia.month
                hora = data_referencia.hour
                minuto = data_referencia.minute
                return f'{dia}/{mes} {[hora if hora>9 else f"0{hora}"][0]}:{[minuto if minuto>9 else f"0{minuto}"][0]}'
        except:
            return 'Sem Previsão'
        else:
            hora = data_referencia.hour
            minuto = data_referencia.minute
            return f'{[hora if hora>9 else f"0{hora}"][0]}:{[minuto if minuto>9 else f"0{minuto}"][0]}'

def bases_ref_envio(tipo_base): # 0 base frentes // 1 base envios
    if tipo_base == 0:
        base_frentes = pd.read_excel(r'\\CSCLSFSR01\Agricola$\Logistica Agroindustrial\CIA 22.23\11. Analytics\BOT CIA\Contatos\Overview_Manut.xlsx', 1)
        return base_frentes
    elif tipo_base == 1:
        base_envios = pd.read_excel(r'\\CSCLSFSR01\Agricola$\Logistica Agroindustrial\CIA 22.23\11. Analytics\BOT CIA\Contatos\Overview_Manut.xlsx', 0)
        return base_envios

def envio_mensagens_PMA():
    base_PMA = sub_atualizar_formatar_PMA()
    base_frentes = bases_ref_envio(0)
    envio_grupos = bases_ref_envio(1).set_index('Unidade').to_dict('dict')
    base_frentes.dropna(inplace=True)
    base_frentes_cct = base_frentes[base_frentes.TIPO_FRENTE=='CCT']
    base_frentes_prod = base_frentes[base_frentes.TIPO_FRENTE=='PROD']
    modelos_cad = {
    'TT': ['T.Extra Pesado','T.Pneu Pesado 1','T.Pneu Pesado 2','T.Pneu Leve','Trator Rolo Compactador','Trator Pulverizador'],
    'TB': ['Transbordo','Transbordo Carroceria','Semi-Reboque Transbordo','Reboque Transbordo'],
    'CM': ['Cam. Aplicação Corretivo','Cam.Cavalo Mecanico','Cam.Oficina','Cam.Bombeiro','Cam.Munck','Cam.Quimico/Calda Pronta','Cam.Comboio','Caminhão Adubo Bazuca','Caminhão Transbordo','Cam.Adubo Liquido','Cam.Borracharia','Cam.Bau (Cargas)','Cam.Furgao (T.Pessoal)','Cam.Comercial(Carga Seca)','Caminhão Preventiva' ,'Cam.Vinhaça','Cam.Tanque','Cam.Basculante','Cam.Coleta Lixo (Caçamba)'],
    'CD': ['Colhedora'],
    'PL': ['Plantadora de Cana']}
    ##################### Loop CCT
    for unidade in base_frentes_cct.UND.unique():
        if str(envio_grupos['Grupo_CCT'][unidade]) == 'nan':
            pass # Envio foi pulado porque não existe contato para envio
        else:
            mensagem_a_ser_enviada = []
            if unidade == 'DIA':
                cort_und = base_PMA[(base_PMA.UNIDADE.str.contains('DIA|BARRA'))]
                rel_frentes_envio = base_frentes_cct[(base_frentes_cct.FRENTE.str.contains("DDC-|DIA-"))]
            else:
                cort_und = base_PMA[base_PMA.UNIDADE==unidade]
                rel_frentes_envio = base_frentes_cct[base_frentes_cct.UND==unidade]
            mensagem_a_ser_enviada.append(f'🏭 Overview Manutenção *{unidade} - Operação CCT*')
            for frente in rel_frentes_envio.FRENTE.unique():
                mensagem_a_ser_enviada.append(f'\nFRENTE: *{frente}* 🚜🎋')
                if len(cort_und[cort_und.FRENTE.str[-3:]==frente[-3:]]) == 0:
                    mensagem_a_ser_enviada.append(f'Sem ocorrências.')
                else:
                    for frota in cort_und[cort_und.FRENTE.str[-3:]==frente[-3:]].CD_EQUIPAMENTO:
                        try: num_os = int(cort_und[cort_und.CD_EQUIPAMENTO==frota].values[0][0])
                        except: num_os = 'Sem OS'
                        Modelo = str(cort_und[cort_und.CD_EQUIPAMENTO==frota].values[0][3])
                        try: Modelo = [k for k in modelos_cad.keys() if Modelo in modelos_cad[k]][0]
                        except: Modelo.split(' ')[0]
                        Apontamento = str(cort_und[cort_und.CD_EQUIPAMENTO==frota].values[0][8]).split(' ')[0]
                        if len(str(cort_und[cort_und.CD_EQUIPAMENTO==frota].values[0][12])[:10].split('-')) != 3: Motivo = str(cort_und[cort_und.CD_EQUIPAMENTO==frota].values[0][12])[0:30].replace('-',' ')
                        else: Motivo = str(cort_und[cort_und.CD_EQUIPAMENTO==frota].values[0][12])[13:88]
                        try: qru_check = search('|'.join(['1106','1108','1110','1112','1114','1117','1118','1063','info','inf.','inf ','ag ']),Motivo)[0]
                        except: qru_check = 0
                        if qru_check != 0: Motivo = '*Não informado* ❌'
                        try: Status_PMA = ' '.join(str(cort_und[cort_und.CD_EQUIPAMENTO==frota].values[0][10]).split(' ')[:2])
                        except: Status_PMA = str(cort_und[cort_und.CD_EQUIPAMENTO==frota].values[0][10])
                        if Status_PMA == 'Manutenção no': Status_PMA = 'Manutenção Implemento'
                        Ini_manut = cort_und[cort_und.CD_EQUIPAMENTO==frota].values[0][13]
                        try: Ini_manut = datetime(int(Ini_manut[:4]),int(Ini_manut[5:7]),int(Ini_manut[8:10]),int(Ini_manut[11:13]),int(Ini_manut[14:16]),int(Ini_manut[17:19]))
                        except: Ini_manut = 'n/a'
                        Prev_lib = cort_und[cort_und.CD_EQUIPAMENTO==frota].values[0][14]
                        try: Prev_lib = datetime(int(Prev_lib[:4]),int(Prev_lib[5:7]),int(Prev_lib[8:10]),int(Prev_lib[11:13]),int(Prev_lib[14:16]),int(Prev_lib[17:19]))
                        except: Prev_lib = 'Sem Previsão'
                        try: motivo_os = str(cort_und[cort_und.CD_EQUIPAMENTO==frota]['MOTIVO_ENTRADA'].values[0]).split(' ')[0]
                        except: motivo_os = '*'
                        try: emoji_prev = ['🔴' if (datetime.now()>Prev_lib) else '🟢'][0]
                        except: emoji_prev = '🔴'
                        mensagem_a_ser_enviada.append(f"- {Modelo.split(' ')[0]}: *{frota}* | OS: *{num_os}* | 🛰️ {Apontamento} | Status: {Status_PMA} | Motivo {motivo_os} | Inicio: {reducao_data(Ini_manut)} | Previsão: {reducao_data(Prev_lib)} {emoji_prev[0]} | 💬 QRU: {Motivo}")
            mensagem_a_ser_enviada = '\n'.join(mensagem_a_ser_enviada)
            contato, tipo_contato = verificar_tipo_de_contato(envio_grupos['Grupo_CCT'][unidade])
            gravar_em_banco_para_envio([('MANUT_Overview',datetime.now(),contato, tipo_contato, mensagem_a_ser_enviada, '')])
    ##################### Loop PROD
    for unidade in base_frentes_prod.UND.unique():
        if str(envio_grupos['Grupo_PROD'][unidade]) == 'nan':
            pass
            # Envio foi pulado porque não existe contato para envio
        else:
            mensagem_a_ser_enviada = []
            if unidade == 'DIA':
                cort_und = base_PMA[(base_PMA.UNIDADE.str.contains('DIA|BARRA'))]
                rel_frentes_envio = base_frentes_prod[(base_frentes_prod.FRENTE.str.contains("DDC-|DIA-"))]
            else:
                cort_und = base_PMA[base_PMA.UNIDADE==unidade]
                rel_frentes_envio = base_frentes_prod[base_frentes_prod.UND==unidade]
            mensagem_a_ser_enviada.append(f'🏭 Overview Manutenção *{unidade} - Operação PROD*')
            for frente in rel_frentes_envio.FRENTE.unique():
                mensagem_a_ser_enviada.append(f'\nFRENTE: *{frente}* 🚜🎋')
                if len(cort_und[cort_und.FRENTE==frente]) == 0:
                    mensagem_a_ser_enviada.append(f'Sem ocorrências.')
                else:
                    for frota in cort_und[cort_und.FRENTE==frente].CD_EQUIPAMENTO:
                        try: num_os = int(cort_und[cort_und.CD_EQUIPAMENTO==frota].values[0][0])
                        except: num_os = 'Sem OS'
                        Modelo = str(cort_und[cort_und.CD_EQUIPAMENTO==frota].values[0][3])
                        try: Modelo = [k for k in modelos_cad.keys() if Modelo in modelos_cad[k]][0]
                        except: Modelo.split(' ')[0]
                        Apontamento = str(cort_und[cort_und.CD_EQUIPAMENTO==frota].values[0][8]).split(' ')[0]
                        if len(str(cort_und[cort_und.CD_EQUIPAMENTO==frota].values[0][12])[:10].split('-')) != 3: Motivo = str(cort_und[cort_und.CD_EQUIPAMENTO==frota].values[0][12])[0:30].replace('-',' ')
                        else: Motivo = str(cort_und[cort_und.CD_EQUIPAMENTO==frota].values[0][12])[13:88]
                        try: qru_check = search('|'.join(['1106','1108','1110','1112','1114','1117','1118','1063','info','inf.','inf ','ag ']),Motivo)[0]
                        except: qru_check = 0
                        if qru_check != 0: Motivo = '*Não informado* ❌'
                        try: Status_PMA = ' '.join(str(cort_und[cort_und.CD_EQUIPAMENTO==frota].values[0][10]).split(' ')[:2])
                        except: Status_PMA = str(cort_und[cort_und.CD_EQUIPAMENTO==frota].values[0][10])
                        if Status_PMA == 'Manutenção no': Status_PMA = 'Manutenção Implemento'
                        Ini_manut = cort_und[cort_und.CD_EQUIPAMENTO==frota].values[0][13]
                        try: Ini_manut = datetime(int(Ini_manut[:4]),int(Ini_manut[5:7]),int(Ini_manut[8:10]),int(Ini_manut[11:13]),int(Ini_manut[14:16]),int(Ini_manut[17:19]))
                        except: Ini_manut = 'n/a'
                        Prev_lib = cort_und[cort_und.CD_EQUIPAMENTO==frota].values[0][14]
                        try: Prev_lib = datetime(int(Prev_lib[:4]),int(Prev_lib[5:7]),int(Prev_lib[8:10]),int(Prev_lib[11:13]),int(Prev_lib[14:16]),int(Prev_lib[17:19]))
                        except: Prev_lib = 'Sem Previsão'
                        try: motivo_os = str(cort_und[cort_und.CD_EQUIPAMENTO==frota]['MOTIVO_ENTRADA'].values[0]).split(' ')[0]
                        except: motivo_os = '*'
                        try: emoji_prev = ['🔴' if (datetime.now()>Prev_lib) else '🟢'][0]
                        except: emoji_prev = '🔴'
                        mensagem_a_ser_enviada.append(f"- {Modelo.split(' ')[0]}: *{frota}* | OS: *{num_os}* | 🛰️ {Apontamento} | Status: {Status_PMA} | Motivo {motivo_os} | Inicio: {reducao_data(Ini_manut)} | Previsão: {reducao_data(Prev_lib)} {emoji_prev[0]} | 💬 QRU: {Motivo}")
            mensagem_a_ser_enviada = '\n'.join(mensagem_a_ser_enviada)
            contato, tipo_contato = verificar_tipo_de_contato(envio_grupos['Grupo_PROD'][unidade])
            gravar_em_banco_para_envio([('MANUT_Overview',datetime.now(),contato, tipo_contato, mensagem_a_ser_enviada, '')])



def velocidade_CD_V2():
    cam_vel_cd_hj = verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\CIA Analytics - BOT CIA\Extrator\Azure\SGPA2_DDN_HORAS_OPERACIONAIS_ON_COLHEDORA_CCT_MO.parquet')

    def atualizar_df_velocidade_colhedoras():
        cam_vel_cd_hj_re = verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\CIA Analytics - BOT CIA\Extrator\Azure\SGPA2_DDN_LINHA_TEMPO_OPERACIONAL_HORA_FRENTE_RE.parquet')
        vel_cd_re = pd.read_parquet(cam_vel_cd_hj_re, engine='pyarrow')
        data_c = datetime.fromtimestamp(os.path.getatime(cam_vel_cd_hj))
        data_criacoa = datetime(data_c.year,data_c.month,data_c.day)
        vel_cd = pd.read_parquet(cam_vel_cd_hj, engine='pyarrow')
        vel_cd = pd.concat([vel_cd_re,vel_cd])
        vel_cd["DT_LOCAL"] = data_criacoa
        vel_cd = vel_cd[vel_cd.CD_OPERACAO == 117]
        vel_cd['VEL_P'] = vel_cd.VEL_POND / vel_cd.HR_OPERACIONAIS_VEL
        vel_cd.fillna(0)
        vel_cd.sort_values(by='DESC_GRUPO_EQUIPAMENTO', ascending=True, inplace=True)
        vel_cd.DESC_GRUPO_EQUIPAMENTO = [frente.replace('-RE-',"-MO-") for frente in vel_cd.DESC_GRUPO_EQUIPAMENTO]
        vel_cd["DT_LOCAL"] = vel_cd["DT_LOCAL"] + pd.to_timedelta(vel_cd["HR_LOCAL"].astype(int), unit='h')
        return vel_cd

    def atualizar_df_contatos_envio_cd_vel():
        data_frame_c = pd.read_excel(r'\\CSCLSFSR01\Agricola$\Logistica Agroindustrial\CIA 22.23\11. Analytics\BOT CIA\Contatos\Velocidade_CD.xlsx')
        data_frame_c.dropna(subset='Destino', inplace=True)
        return data_frame_c

    def fzt_por_duracao(fazendas, zonas, talhoes, duracoes, base_ppc_ref, unidade, meta_obz):
        def procurar_meta_PPC(base_ppc, fazenda, zona, talhao, unidade, meta_obz):
            if f'{fazenda}-{zona}-{talhao}' != '0-0-0' and fazenda in list(base_ppc.Fundo.values):
                #print(f'Procurando meta PPC para F: {fazenda} | Z: {zona} | T: {talhao}')
                #num_semana_sf = f'Semana 0{(datetime.now()+timedelta(days=3)).isocalendar().week-13}' if (datetime.now()-timedelta(days=3)).isocalendar().week-13 < 9  else f'Semana {(datetime.now()+timedelta(days=3)).isocalendar().week-13}'
                #num_semana_sf = (datetime.now()-timedelta(days=3)).isocalendar().week+39
                num_semana_sf = (datetime.now()-timedelta(days=3)).isocalendar().week-12
                if fazenda in list(base_ppc.Fundo.values) \
                and zona in list(base_ppc[base_ppc.Fundo == fazenda].Zona.values) \
                and talhao in list(base_ppc[(base_ppc.Fundo == fazenda) & (base_ppc.Zona == zona)]['Talhões'].values):
                    #print('Achamos FZT')
                    meta = base_ppc[(base_ppc.Fundo == fazenda) & (base_ppc.Zona == zona) & (base_ppc['Talhões'] == talhao)]
                    if len(meta) > 1:
                        if len(meta[meta.Semana == num_semana_sf].Semana.unique()) > 0: 
                            meta = meta[meta.Semana == num_semana_sf]['Vel. Média'].values[-1]
                        else: 
                            try: meta = round(meta['Vel. Média'].values.mean(),1)
                            except: meta = 0
                    else: meta = meta['Vel. Média'].values[-1]
                    return [meta, '']
                elif fazenda in list(base_ppc.Fundo.values) and zona in list(base_ppc[base_ppc.Fundo == fazenda].Zona.values):
                    #print('Achamos FZ')
                    meta = base_ppc[(base_ppc.Fundo == fazenda) & (base_ppc.Zona == zona)]
                    if len(meta) > 1:
                        if len(meta[meta.Semana == num_semana_sf].Semana.unique()) > 0: meta = meta[meta.Semana == num_semana_sf]['Vel. Média'].values[-1]
                        else: 
                            try: meta = round(meta['Vel. Média'].values.mean(),1)
                            except: meta = 0
                    else: meta = meta['Vel. Média'].values[-1]
                    return [meta, '*']
                elif fazenda in list(base_ppc.Fundo.values) and zona in list(base_ppc.Zona.values) and talhao in list(base_ppc['Talhões'].values):
                    #print('Achamos F')
                    meta = base_ppc[(base_ppc.Fundo == fazenda)]
                    if len(meta) > 1:
                        if len(meta[meta.Semana == num_semana_sf].Semana.unique()) > 0: meta = meta[meta.Semana == num_semana_sf]['Vel. Média'].values[-1]
                        else: 
                            try: meta = round(meta['Vel. Média'].values.mean(),1)
                            except: meta = 0
                    else: meta = meta['Vel. Média'].values[-1]
                    return [meta, '**']
                else:
                    try:
                        meta_obz = round(meta_obz[meta_obz.Unidade == unidade].Meta.values[0],1) #
                        return [meta_obz,'***']
                    except:
                        return [0,'***']
            else:
                return ['-','***']
        list_fzt = []
        gatilho_obz = 0
        for f in range(len(fazendas)):
            try: faz = fazendas[f]
            except: faz = '*'
            try: zon = zonas[f]
            except: zon = '*'
            try: tal = talhoes[f]
            except: tal = '*'
            meta_, termo_obz = procurar_meta_PPC(base_ppc_ref, fazendas[f], zonas[f], talhoes[f], unidade, meta_obz)
            if termo_obz == '***': gatilho_obz = 1
            try: meta_pond = round(duracoes[f] / sum(duracoes),2) * meta_
            except: meta_pond = 0
            list_fzt.append([faz,zon,tal,meta_pond])
        if gatilho_obz == 1: return f'{round(sum([f[3] for f in list_fzt]),1)}*'
        else: return round(sum([f[3] for f in list_fzt]),1)

    def carregar_compilado_PPC():
        while True:
            try:
                caminho = verificar_base_atualizada(r"C:\Users\ciaanalytics\MinhaTI\Metas_Gov_Indicadores - Documentos\CCT\PPC COLHEITA.xlsx")
                df_ppc = pd.read_excel(caminho, sheet_name='BASE PPC')
                return df_ppc[['Semana','Fundo','Zona','Talhões','Vel. Média']]
            except:
                sleep(1)
                print('Erro para atualizar Empilhado PPC - Velocidade Meta CCT')
                pass

    def carregar_obz():
        caminho_obz_vel_cd = verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\CIA Analytics - CCT\Parametros CD.xlsx')
        base_obz = pd.read_excel(caminho_obz_vel_cd, sheet_name='Premissas')
        base_obz = base_obz[base_obz.Informação == "Vel Corte [km/h]"]
        rename_und = {'PTP':'PASSATEMPO',
        'RBR':'RIO BRILHANTE',
        'JATAI':'JATAÍ',
        'DEST':'DESTIVALE',
        'VRO':'VALE DO ROSARIO',
        'CNT':'CONTINENTAL',
        'UNI':'UNIVALEM',
        'MUND':'MUNDIAL',
        'BENA':'BENALCOOL',
        'RAF':'RAFARD',
        'SCAND':'SANTA CÂNDIDA',
        'SEL':'SANTA ELISA',
        'BONF':'BONFIM',
        'COPI':'COSTA PINTO',
        'PARAI':'PARAISO',
        'IPA':'IPAUSSU',
        'JUN':'JUNQUEIRA',
        'LEM':'LEME',
        'LPT':'LAGOA DA PRATA',
        'CAAR':'CAARAPÓ',
        'DIA':'DIAMANTE'}
        base_obz = base_obz.replace({"Unidade": rename_und})
        dict_mes_atual = {
            1:'Jan',
            2:'Fev',
            3:'Mar',
            4:'Abr',
            5:'Mai',
            6:'Jun',
            7:'Jul',
            8:'Ago',
            9:'Set',
            10:'Out',
            11:'Nov',
            12:'Dez'}
        coluna_mes = dict_mes_atual[datetime.now().month]
        base_obz = base_obz[['Unidade',coluna_mes]]
        base_obz.rename(columns={coluna_mes:'Meta'}, inplace=True)
        return base_obz

    meta_obz = carregar_obz()
    base_ppc = carregar_compilado_PPC()

    def velocidades(dataframe):
        lista_relacao_frotas = {}
        df_temp = dataframe
        df_temp.sort_values(by='DESC_GRUPO_EQUIPAMENTO', inplace=True)
        for frota in df_temp.CD_EQUIPAMENTO.unique():
            velocidade_periodo = df_temp[df_temp.CD_EQUIPAMENTO == frota].VEL_POND.sum() / df_temp[df_temp.CD_EQUIPAMENTO == frota].HR_OPERACIONAIS_VEL.sum()
            fazenda_ = df_temp[df_temp.CD_EQUIPAMENTO == frota].CD_FAZENDA.values
            zona_ = df_temp[df_temp.CD_EQUIPAMENTO == frota].CD_ZONA.values
            talhao_ = df_temp[df_temp.CD_EQUIPAMENTO == frota].CD_TALHAO.values
            duracao_ = df_temp[df_temp.CD_EQUIPAMENTO == frota].VL_HR_OPERACIONAIS.values
            und_ = df_temp[df_temp.CD_EQUIPAMENTO == frota].DESC_UNIDADE.values[0]
            meta_ = fzt_por_duracao(fazenda_, zona_, talhao_, duracao_, base_ppc, und_, meta_obz)
            lista_relacao_frotas[frota] = [velocidade_periodo, meta_]
        vl = pd.DataFrame(lista_relacao_frotas.values(), index=lista_relacao_frotas.keys())
        #display(vl)
        vl.sort_values(by=0, ascending=True, inplace=True)
        frota_r, vel_r, frota_l, vel_l = vl.index[-1], vl[0].values[-1], vl.index[0], vl[0].values[0]
        return [frota_r, vel_r, frota_l, vel_l], lista_relacao_frotas

    data_frame_vel = atualizar_df_velocidade_colhedoras()
    contatos_envio_vel = atualizar_df_contatos_envio_cd_vel()

    for unidade in contatos_envio_vel[["Unidade","Destino"]].Unidade.unique():
        enviar_mensagem_v2 = []
        enviar_mensagem_v2.append(f'\n🏭🎋 *Velocidade Colhedoras {unidade.upper()}*\n')
        if unidade in ['CAARAPO','RIO BRILHANTE','PASSATEMPO','CAARAPÓ']: df_und = data_frame_vel[(data_frame_vel.DESC_UNIDADE == unidade) & (data_frame_vel.DT_LOCAL > datetime.now()-timedelta(hours=2.95))]
        else: df_und = data_frame_vel[(data_frame_vel.DESC_UNIDADE == unidade) & (data_frame_vel.DT_LOCAL > datetime.now()-timedelta(hours=1.95))]
        if len(df_und.CD_EQUIPAMENTO.unique()) > 1:
            placar, dict_velocidades = velocidades(df_und)
            enviar_mensagem_v2.append(f'📈 Colhedora maior desempenho: {placar[0]} - *{round(placar[1],1)} km/h*')
            enviar_mensagem_v2.append(f'📉 Colhedora menor desempenho: {placar[2]} - *{round(placar[3],1)} km/h*')
            for frente in data_frame_vel[data_frame_vel.DESC_UNIDADE == unidade].DESC_GRUPO_EQUIPAMENTO.unique():
                frotas_dessa_frente = list(df_und[df_und.DESC_GRUPO_EQUIPAMENTO == frente].CD_EQUIPAMENTO.unique())
                vel_med_frente = np.array([values[0] for key,values in zip(dict_velocidades.keys(),dict_velocidades.values()) if key in frotas_dessa_frente]).mean()
                escrita_media_frente = f' - Med. {round(vel_med_frente,1)} km/h' if vel_med_frente > 0 else ''
                enviar_mensagem_v2.append(f'\n🎋 *Frente {str(frente).split("-")[-1]}*{escrita_media_frente}')
                if len(df_und[df_und.DESC_GRUPO_EQUIPAMENTO == frente].CD_EQUIPAMENTO.unique()) > 0:
                    for frota in df_und[df_und.DESC_GRUPO_EQUIPAMENTO == frente].CD_EQUIPAMENTO.unique():
                        if len(data_frame_vel[data_frame_vel.DESC_UNIDADE==unidade].DESC_GRUPO_EQUIPAMENTO.unique()) == 0:
                            enviar_mensagem_v2.append(f'\n Sem dados.\n')
                        else:
                            try:
                                meta_ppc = float(dict_velocidades[frota][1].replace('*','')) if str(dict_velocidades[frota][1]).count('*') > 0 else float(dict_velocidades[frota][1])
                                if meta_ppc != 0:
                                    emoji_meta = '✅' if float(dict_velocidades[frota][0]) >= meta_ppc else '⚠️'
                                    enviar_mensagem_v2.append(f'CD {frota} - {emoji_meta} Vel. {round(dict_velocidades[frota][0],1)} (PPC {dict_velocidades[frota][1]})')
                                else:
                                    enviar_mensagem_v2.append(f'CD {frota} - ❔ Vel. {round(dict_velocidades[frota][0],1)} (PPC -)')
                            except:
                                enviar_mensagem_v2.append(f'CD {frota} - ❔ Vel. {round(dict_velocidades[frota][0],1)} (PPC -)')
                else: enviar_mensagem_v2.append(f'Sem informações')

            if unidade in ['CAARAPO','RIO BRILHANTE','PASSATEMPO','CAARAPÓ']: enviar_mensagem_v2.append(f"\n_Dados de {(datetime.now()-timedelta(hours=2)).hour}h00 até {(datetime.now()-timedelta(hours=2)).hour}h59._")
            else: enviar_mensagem_v2.append(f"\n_Dados de {(datetime.now()-timedelta(hours=1)).hour}h00 até {(datetime.now()-timedelta(hours=1)).hour}h59._")
            mensagem_a_ser_enviado = '\n'.join(enviar_mensagem_v2)
            contato, tipo_contato = verificar_tipo_de_contato(contatos_envio_vel[contatos_envio_vel.Unidade == unidade].Destino.values[0])
            gravar_em_banco_para_envio([('CCT_Velocidade',datetime.now(),contato, tipo_contato, mensagem_a_ser_enviado, '')])
        else: pass


################### CD IMP

def colhedoras_improdutivas_CCT():
    if not os.path.exists(os.getcwd()+'\\CD_IMP'):
        os.mkdir('CD_IMP')
    global df_colhedoras_cdimp, df_cenario_frentes, df_metas_cd_cct, controle_envio_cd_imp_cct
    def geracao_de_texto_cd_imp(mensagem_unidade, cd_prod,cd_imp,meta_cd):
        return f'⚠️ *ATENÇÃO {mensagem_unidade}*: No momento estamos com *{cd_imp} colhedoras improdutivas* e *{cd_prod} colhedoras produtivas*.\n🎯Nossa meta é ter no mínimo *{meta_cd} colhedoras* produtivas.'
    def atualizacao_metas():
        while True:
            try: 
                df_metas_cd_cct = pd.read_excel(r'\\CSCLSFSR01\Agricola$\Logistica Agroindustrial\CIA 22.23\11. Analytics\BOT CIA\Contatos\Colhedoras improdutivas.xlsx')
                convert_und = {'BARRA':'BAR','BENA':'BEN','BONF':'BON','CAAR':'CAA','CNT':'CNT',
                               'COPI':'COP','DEST':'DES','DIA':'DIA','GASA':'GAS','IPA':'IPA',
                               'JATAI':'JAT','JUN':'JUN','LEM':'LEM','LPT':'LPT','MUND':'MUN',
                               'PARAI':'UPA','PTP':'PTP','RAF':'RAF','RBR':'RBR','SCAND':'USC',
                               'SEL':'SEL','SERRA':'SER','UMB':'UMB','UNI':'UNI','USH':'USH',
                               'VRO':'VRO','ZANIN':'ZAN'}
                
                df_metas_cd_cct = df_metas_cd_cct[['Unidade','Limite Colhedoras Improdutivas','CDs produtivas']].replace(convert_und)
                return df_metas_cd_cct
               
            except:
                print('======'*2,'\nProblema para atualizar df_metas_cd_cct ','======'*2)
                sleep(1)
                pass
    def atualizar_contatos():
        try:
            df_contatos_grupo_cct = pd.read_excel(r'\\CSCLSFSR01\Agricola$\Logistica Agroindustrial\CIA 22.23\11. Analytics\BOT CIA\Contatos\lista_cd_imp_envio.xlsx')
            df_contatos_grupo_cct = dict(zip(df_contatos_grupo_cct.Unidade, df_contatos_grupo_cct.Grupo_CCT))
            return df_contatos_grupo_cct
        except:
            print('Não foi possível atualizar contatos!!!')
            pass
    def atualizar_base_cd_imp():
        while True:
            try:
                base_cd_imp = pd.read_excel(verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\CIA Analytics - BOT CIA\Extrator\AGRON\agron_comunicacao.xlsx'))
                return base_cd_imp
            except:
                print('Não foi possível atualizar base cd imp!!!')
                sleep(1)
                pass
    def controle_improdutivas_cct_limitador():
        lista_a = []
        lista_b = []
        df = atualizar_base_cd_imp()
        df_c = df[df["Tipo do equipamento"]=="Colhedora"]
        lista_unidades = list(df_c['Frente associada'].str[:3].unique())
        for n in lista_unidades:
            lista_a.append(n[:3])
            lista_b.append(datetime.now()+timedelta(minutes=15))
        controle_envio_cd_imp_cct = pd.DataFrame(list(zip(lista_a,lista_b)))
        controle_envio_cd_imp_cct.rename(columns = {0:'Unidade', 1:'Envio'}, inplace = True)
        return controle_envio_cd_imp_cct
    def desconsiderar_frentes_cd_imp():
        while True:
            try:
                path_of_file = r'\\CSCLSFSR01\Agricola$\Logistica Agroindustrial\CIA 22.23\11. Analytics\BOT CIA\Contatos\Colhedoras improdutivas.xlsx'
                desc_f_cd_imp = pd.read_excel(path_of_file, sheet_name='Desconsiderar Frente')
                return '|'.join(desc_f_cd_imp.Descon_Frente.unique())
            except Exception as error:
                print(f'Não foi possível atualizar frentes exceções CD Imp. {error}')
                pass       

    try: controle_envio_cd_imp_cct
    except NameError: controle_envio_cd_imp_cct = controle_improdutivas_cct_limitador()
    
    df_metas_cd_cct = atualizacao_metas()
    df_contatos_grupo_cct = atualizar_contatos()
    df_base = atualizar_base_cd_imp()
    #df_base['Tempo em atividade'] = pd.to_timedelta(df_base['Tempo em atividade'])
    df_base['Tempo em atividade'] = pd.to_timedelta(df_base['Tempo em atividade'], unit='s')
    df_base = df_base[df_base['Registro mais recente'].astype(str).str[:10].str.contains(f'{datetime.now().date()}|{(datetime.now()-timedelta(days=1)).date()}')]
    lista_numero_frente = []
    for n in df_base['Frente associada'].unique(): 
        if 'MO'in(n): lista_numero_frente.append(n[7:])
    df_base = (df_base[df_base['Frente associada'].str.contains('|'.join(map(re.escape, lista_numero_frente)))])
    df_base['Frente associada'] = df_base['Frente associada'].replace('-RE-', '-MO-', regex=True)
    if len(desconsiderar_frentes_cd_imp()): df_base = df_base[(df_base['Frente associada'].str.contains('MO')) & (df_base['Atividade'] != '213 - Patio / Reserva') & (df_base['Frente associada'] != 'Sem frente') & (df_base['Tipo do equipamento'] == 'Colhedora') & (~df_base['Frente associada'].str.contains(desconsiderar_frentes_cd_imp()))]
    else: df_base = df_base[(df_base['Frente associada'].str.contains('MO')) & (df_base['Atividade'] != '213 - Patio / Reserva') & (df_base['Frente associada'] != 'Sem frente') & (df_base['Tipo do equipamento'] == 'Colhedora')]
    df_base['Registro mais recente'] = pd.to_datetime(df_base['Registro mais recente'])
    apontamentos_produtivos = ['117 - Corte de Cana Mecanizado', '832 - Manobra', '208 - Chuva Solo Umido', '208 - Parada por condicoes climaticas', '227 - Limitação Indústria','1118 - Man Corret - Oportunidade','1119 - Man Preven - Oportunidade']
    df_excecao = df_base[(~df_base['Atividade'].str.contains('|'.join(apontamentos_produtivos))) & (df_base['Tempo em atividade'] < timedelta(minutes=0))] #0.01
    lista_a, lista_b, lista_c, lista_d, lista_f = [],[],[],[],[]
    for n in list(df_base['Frente associada'].str[:3].unique()):
        df_pass = df_base[df_base["Frente associada"].str.contains(n)]
        excecao = df_excecao[df_excecao["Frente associada"].str.contains(n)].value_counts().count()
        julg_o = df_pass[df_pass['Atividade'].str.contains('|'.join(map(re.escape, apontamentos_produtivos)))].value_counts().count()
        julg_r = df_pass[~df_pass['Atividade'].str.contains('|'.join(map(re.escape, apontamentos_produtivos)))].value_counts().count()
        lista_a.append(n)
        lista_b.append(julg_o+excecao)
        lista_c.append(julg_r-excecao)
        lista_d.append(int(df_metas_cd_cct[df_metas_cd_cct['Unidade']==(n[:3])]['Limite Colhedoras Improdutivas'].values.sum()))
        lista_f.append(int(df_metas_cd_cct[df_metas_cd_cct['Unidade']==(n[:3])]['CDs produtivas'].values.sum()))
    df_cenario_frentes = pd.DataFrame(list(zip(lista_a,lista_b,lista_c,lista_d,lista_f)), columns=['Unidade','Produtiva','Improdutiva','Meta_CD_Imp','CD_Prod_REF'])
    df_cenario_frentes = df_cenario_frentes.eval("sit_CD_Prod = Produtiva - CD_Prod_REF")
    df_cenario_frentes["status"] = ["OK" if s > (-1) else "Gatilho" for s in df_cenario_frentes['sit_CD_Prod']]
    caminho_da_pasta = os.getcwd()
    agora = datetime.now()
    df_cenario_frentes = pd.merge(df_cenario_frentes, controle_envio_cd_imp_cct, on=['Unidade'], how='right')
    for idx, row in df_cenario_frentes[df_cenario_frentes['status'] == 'Gatilho'].iterrows():
        if np.datetime64(row[7]) <= np.datetime64(agora):
            df_export = df_base[df_base['Frente associada'].str.contains(str(row[0]))]
            df_export = df_export.sort_values(by='Frente associada')
            df_export = df_export.rename(columns={'Número do Equipamento':'Equipamento','Tempo em atividade':'Tempo atividade'})
            df_export = df_export[~df_export['Atividade'].isin(apontamentos_produtivos)]
            try: 
                dfi.export(df_export[['Equipamento','Frente associada','Atividade','Registro mais recente', 'Tempo atividade']].style.hide(axis='index'), f'CD_IMP\\CD_IMP_{row[0]}.png')
                index_limiter_cd = controle_envio_cd_imp_cct[controle_envio_cd_imp_cct.Unidade == row.Unidade].index[0]
                controle_envio_cd_imp_cct.loc[index_limiter_cd, 'Envio'] = agora+timedelta(minutes=30)
                caminho_anexo = f'{caminho_da_pasta}\\CD_IMP\\CD_IMP_{row[0]}.png'
                mensagem_a_ser_enviada = geracao_de_texto_cd_imp((str(row[0])), round(row[1]),round(row[2]),math.ceil((row[4])))
                contato, tipo_contato = verificar_tipo_de_contato(df_contatos_grupo_cct[row[0]])
                gravar_em_banco_para_envio([('CCT_CD',datetime.now(),contato, tipo_contato,mensagem_a_ser_enviada,caminho_anexo)])
            except: print('Erro em geração de imagem cd imp')

def controle_improdutivas_cct_limitador():
    df_base = carregar_df_monitoramento_SGPA3()
    limitador = [(und,datetime.now()+timedelta(minutes=15)) for und in list(df_base['Frente associada'].str[:3].unique())]
    controle_envio_cd_imp_cct = pd.DataFrame(limitador, columns=['Unidade','Envio'])
    return controle_envio_cd_imp_cct

controle_envio_cd_imp_cct = controle_improdutivas_cct_limitador()

def colhedoras_improdutivas_CCT_SPGA3():
    # Variável global para controle de envio
    global controle_envio_cd_imp_cct
    
    # Cria diretório para imagens se não existir (mantemos por compatibilidade)
    if not os.path.exists(os.getcwd()+'\\CD_IMP'):
        os.mkdir('CD_IMP')

    def geracao_de_texto_cd_imp(mensagem_unidade, cd_prod, cd_imp, meta_cd, df_colhedoras=None):
        """
        Gera texto formatado para WhatsApp com resumo estatístico e tabela de colhedoras improdutivas.
        
        Args:
            mensagem_unidade (str): Código da unidade
            cd_prod (int): Número de colhedoras produtivas
            cd_imp (int): Número de colhedoras improdutivas
            meta_cd (int): Meta de colhedoras produtivas
            df_colhedoras (pandas.DataFrame, optional): DataFrame com detalhes das colhedoras improdutivas
        
        Returns:
            str: Mensagem formatada para WhatsApp
        """
        # Parte 1: Resumo estatístico (mantido como estava)
        mensagem = f'⚠️ *ATENÇÃO {mensagem_unidade}*: No momento estamos com *{cd_imp} colhedoras improdutivas* e *{cd_prod} colhedoras produtivas*.\n🎯Nossa meta é ter no mínimo *{meta_cd} colhedoras* produtivas.'
        
        # Parte 2: Tabela detalhada (se o DataFrame for fornecido)
        if df_colhedoras is not None and not df_colhedoras.empty:
            # Adiciona espaçamento entre o resumo e a tabela detalhada
            mensagem += "\n\n*DETALHAMENTO DAS COLHEDORAS IMPRODUTIVAS:*\n\n"
            
            # Obtém as colunas do DataFrame
            colunas = df_colhedoras.columns.tolist()
            
            # Adiciona linha de cabeçalho (usando negrito)
            cabecalho_parte1 = " | ".join(colunas[:2]) + " |* "
            cabecalho_parte2 = " | ".join(colunas[2:])
            mensagem += "*" + cabecalho_parte1 + "\n*" + cabecalho_parte2 + "*\n"
            mensagem += "-" * 30 + "\n"
            
            # Adiciona cada linha de dados
            for _, row in df_colhedoras.iterrows():
                # Formatação especial para tempo de atividade
                if 'Tempo atividade' in row:
                    tempo = str(row['Tempo atividade'])
                    if 'days' in tempo:
                        dias, tempo_restante = tempo.split(' days ')
                        horas, minutos, segundos = tempo_restante.split(':')
                        row['Tempo atividade'] = f"{dias}d {horas}h {minutos}m"
                    else:
                        horas, minutos, segundos = tempo.split(':')
                        row['Tempo atividade'] = f"{horas}h {minutos}m"
                
                # Formatação especial para data/hora (Registro mais recente)
                if 'Registro mais recente' in row and hasattr(row['Registro mais recente'], 'strftime'):
                    row['Registro mais recente'] = row['Registro mais recente'].strftime("%d/%m %H:%M")
                
                # Formata cada linha, truncando valores muito longos
                linha_formatada = []
                for idx, col in enumerate(colunas):
                    valor = str(row[col])
                    # Trunca valores muito longos
                    if len(valor) > 20 and col != 'Atividade':
                        valor = valor[:17] + "..."
                    elif len(valor) > 30 and col == 'Atividade':
                        valor = valor[:27] + "..."
                    linha_formatada.append(valor)
                
                primeira_parte = "*" + " | ".join(linha_formatada[:2]) + " |* "
                segunda_parte = " | ".join(linha_formatada[2:])
                
                # Adicionar as duas partes com quebra de linha entre elas
                mensagem += primeira_parte + "\n" + segunda_parte + "\n\n"
            
            # Adiciona rodapé com timestamp
            from datetime import datetime
            mensagem += f"\n_Relatório gerado em: {datetime.now().strftime('%d/%m/%Y %H:%M')}_"
        
        return mensagem

    def atualiza_parametros_cd_imp():
        df_parametros = pd.read_excel(
            r'C:\Users\ciaanalytics\MinhaTI\MinhaTI\Metas_Gov_Indicadores - Documentos\CCT\Parâmetros Relatórios CCT (Rotina).xlsx',
            sheet_name="Ajuste"
        )
        df_bot = pd.read_excel(
            r'\\CSCLSFSR01\Agricola$\Logistica Agroindustrial\CIA 22.23\11. Analytics\BOT CIA\Contatos\Colhedoras improdutivas.xlsx'
        )

        # Agrupar por unidade e somar a quantidade de CDs
        df_parametros = df_parametros.groupby("Und.", as_index=False)[["Qtdade. CD"]].sum()

        # Criar um dicionário de correspondência entre os nomes das unidades
        unidade_map = {
            "SCAND": "STA CANDIDA",
            "OUTRA_ABREV": "NOME COMPLETO"
        }

        # Criar coluna "Unidade Corrigida"
        df_bot["Unidade Corrigida"] = df_bot["Unidade"].replace(unidade_map)

        # Aplicar o mapeamento corrigido e preencher NaN com 0
        df_bot["Quantidade de CD'S"] = df_bot["Unidade Corrigida"].map(df_parametros.set_index("Und.")["Qtdade. CD"]).fillna(0)

        # Remover a coluna auxiliar
        df_bot.drop(columns=["Unidade Corrigida"], inplace=True)

        # Converter a coluna "%" para float (caso necessário)
        df_bot["%"] = df_bot["%"].astype(float)

        # Calcular "CDs produtivas" e "Limite Colhedoras Improdutivas"
        df_bot["CDs produtivas"] = (df_bot["Quantidade de CD'S"] * df_bot["%"]).round(0)
        df_bot["Limite Colhedoras Improdutivas"] = (df_bot["Quantidade de CD'S"] - df_bot["CDs produtivas"]).round(0)

        df_bot.to_excel(r'\\CSCLSFSR01\Agricola$\Logistica Agroindustrial\CIA 22.23\11. Analytics\BOT CIA\Contatos\Colhedoras improdutivas.xlsx')
    
    atualiza_parametros_cd_imp()
    sleep(0.5)
    # Caminhos:
    cam_metas = r'\\CSCLSFSR01\Agricola$\Logistica Agroindustrial\CIA 22.23\11. Analytics\BOT CIA\Contatos\Colhedoras improdutivas.xlsx'
    cam_contatos = r'\\CSCLSFSR01\Agricola$\Logistica Agroindustrial\CIA 22.23\11. Analytics\BOT CIA\Contatos\lista_cd_imp_envio.xlsx'
    cam_option = r'\\CSCLSFSR01\Agricola$\Logistica Agroindustrial\CIA 22.23\11. Analytics\BOT CIA\Contatos\Colhedoras improdutivas.xlsx'
    
    
    # Carregar metas do controle BOT CIA
    df_metas_cd_cct = pd.read_excel(cam_metas)
    convert_und = {'BARRA':'BAR','BENA':'BEN','BONF':'BON','CAAR':'CAA',
                   'CNT':'CNT','COPI':'COP','DEST':'DES','DIA':'DIA',
                   'GASA':'GAS','IPA':'IPA','JATAI':'JAT','JUN':'JUN',
                   'LEM':'LEM','LPT':'LPT','MUND':'MUN','PARAI':'UPA',
                   'PTP':'PTP','RAF':'RAF','RBR':'RBR','SCAND':'USC',
                   'SEL':'SEL','SERRA':'SER','UMB':'UMB','UNI':'UNI',
                   'USH':'USH','VRO':'VRO','ZANIN':'ZAN'}
    
    df_metas_cd_cct = df_metas_cd_cct[['Unidade','Limite Colhedoras Improdutivas','CDs produtivas']].replace(convert_und)
    
    # Carregar contatos BOT CIA
    df_contatos_grupo_cct = pd.read_excel(cam_contatos)
    df_contatos_grupo_cct = dict(zip(df_contatos_grupo_cct.Unidade, df_contatos_grupo_cct.Grupo_CCT))
    
    # Carregar Base Comunicação
    df_base = carregar_df_monitoramento_SGPA3()
    lista_frentes_cct = '|'.join(list(set([frente[-3:] for frente in df_base['Frente associada'] if 'MO' in frente])))
    df_base = df_base[(df_base['Frente associada'].str.contains(lista_frentes_cct))
            & (df_base["Tipo do equipamento"]=='COLHEDORA')
            & (df_base["Frente associada"].str.contains('-MO-|-RE-'))]
    
    # Processamento de dados
    df_base['Tempo em atividade'] = df_base['Tempo em atividade'].apply(calcular_tempo)
    df_base['Registro mais recente'] = pd.to_datetime(df_base['Registro mais recente'], dayfirst=True, errors='coerce')
    df_base['Registro mais recente'] = [recente+duracao if type(recente) != float else 'teste' for recente, duracao in zip(df_base['Registro mais recente'], df_base['Tempo em atividade'])]
    
    # Carregar desconsiderar fretes
    desc_f_cd_imp = pd.read_excel(cam_option, sheet_name='Desconsiderar Frente')
    desc_f_cd_imp = '|'.join(desc_f_cd_imp.Descon_Frente.unique())

    df_base['Frente associada'] = df_base['Frente associada'].replace('-RE-', '-MO-', regex=True)

    df_base['Registro mais recente'] = pd.to_datetime(df_base['Registro mais recente'])
    df_base = df_base[df_base["Atividade"]!='213 - Patio - Reserva']
    apontamentos_produtivos = ['117 - Corte de Cana Mecanizado', '832 - Manobra', '208 - Chuva Solo Umido', '208 - Parada por condicoes climaticas', '227 - Limitação Indústria','1118 - Man Corret - Oportunidade','1119 - Man Preven - Oportunidade']
    
    # Cálculo de exceções
    df_excecao = df_base[(~df_base['Atividade'].str.contains('|'.join(apontamentos_produtivos))) & (df_base['Tempo em atividade'] < timedelta(minutes=0))]
    
    # Inicialização de listas para construção do DataFrame de cenário
    lista_a, lista_b, lista_c, lista_d, lista_f = [],[],[],[],[]
    for n in list(df_base['Frente associada'].str[:3].unique()):
        df_pass = df_base[df_base["Frente associada"].str.contains(n)]
        excecao = df_excecao[df_excecao["Frente associada"].str.contains(n)].value_counts().count()
        julg_o = df_pass[df_pass['Atividade'].str.contains('|'.join(map(re.escape, apontamentos_produtivos)))].value_counts().count()
        julg_r = df_pass[~df_pass['Atividade'].str.contains('|'.join(map(re.escape, apontamentos_produtivos)))].value_counts().count()
        lista_a.append(n)
        lista_b.append(julg_o+excecao)
        lista_c.append(julg_r-excecao)
        lista_d.append(int(df_metas_cd_cct[df_metas_cd_cct['Unidade']==(n[:3])]['Limite Colhedoras Improdutivas'].values.sum()))
        lista_f.append(int(df_metas_cd_cct[df_metas_cd_cct['Unidade']==(n[:3])]['CDs produtivas'].values.sum()))
    
    # Criação do DataFrame de cenário
    df_cenario_frentes = pd.DataFrame(list(zip(lista_a,lista_b,lista_c,lista_d,lista_f)), columns=['Unidade','Produtiva','Improdutiva','Meta_CD_Imp','CD_Prod_REF'])
    df_cenario_frentes = df_cenario_frentes.eval("sit_CD_Prod = Produtiva - CD_Prod_REF")
    df_cenario_frentes["status"] = ["OK" if s > (-1) else "Gatilho" for s in df_cenario_frentes['sit_CD_Prod']]
    
    # Preparo para envio
    caminho_da_pasta = os.getcwd()
    agora = datetime.now()
    df_cenario_frentes = pd.merge(df_cenario_frentes, controle_envio_cd_imp_cct, on=['Unidade'], how='right')
    
    # Processamento de cada cenário que necessita envio
    for idx, row in df_cenario_frentes[(df_cenario_frentes['status'] == 'Gatilho') & (df_cenario_frentes['Produtiva'] != 0)].iterrows():
        if np.datetime64(row[7]) <= np.datetime64(agora):
            # Preparação do DataFrame de detalhes
            df_export = df_base[df_base['Frente associada'].str.contains(str(row[0]))]
            df_export = df_export.sort_values(by='Frente associada')
            df_export = df_export.rename(columns={'Número do Equipamento':'Equipamento','Tempo em atividade':'Tempo atividade'})
            df_export = df_export[~df_export['Atividade'].isin(apontamentos_produtivos)]
            
            try:
                # Seleciona colunas relevantes para detalhamento
                df_export_filtrado = df_export[['Equipamento','Frente associada','Atividade','Registro mais recente', 'Tempo atividade']]
                
                # Atualiza controle de tempo para evitar múltiplos envios
                index_limiter_cd = controle_envio_cd_imp_cct[controle_envio_cd_imp_cct.Unidade == row.Unidade].index[0]
                controle_envio_cd_imp_cct.loc[index_limiter_cd, 'Envio'] = agora+timedelta(minutes=30)
                
                # Gera mensagem com resumo E tabela detalhada
                mensagem_a_ser_enviada = geracao_de_texto_cd_imp(
                    str(row[0]), 
                    round(row[1]),
                    round(row[2]),
                    math.ceil(row[4]),
                    df_export_filtrado  # Passar o DataFrame filtrado para gerar a tabela
                )
                
                
                contato, tipo_contato = verificar_tipo_de_contato(df_contatos_grupo_cct[row[0]])
                gravar_em_banco_para_envio([('CCT_CD', datetime.now(), contato, tipo_contato, mensagem_a_ser_enviada, '')])
                
            except Exception as e:
                print(f'Erro ao processar alerta de colhedoras improdutivas: {str(e)}')

#################### CM IMP

def desponibilidade_caminhoes_CCT():
    ########VERIFICAR APONTAMENTO ::: '885 - Após balança de saída até CT'
    caminho_json_cm = f'{os.getcwd()}\\CM_IMP\\controle_envio_cm_imp_cct.json'
    def controle_cm_improdutivos_cct_limitador():
        df_base = carregar_df_monitoramento_SGPA3()
        df_base = df_base[df_base['Frente associada'].str.contains('-LN-')]
        limitador = [(und,datetime.now()+timedelta(minutes=15)) for und in list(df_base['Frente associada'].str[:3].unique())]
        controle_envio_cm_imp_cct = pd.DataFrame(limitador, columns=['UNIDADE','Envio'])
        controle_envio_cm_imp_cct.to_json(caminho_json_cm, orient='records', lines=True)

    if not os.path.exists(caminho_json_cm):
        controle_cm_improdutivos_cct_limitador()

    def CM_improdutivos_CCT_SPGA3(): # EDU
        #global df_export
        controle_envio_cm_imp_cct = pd.read_json(f'{os.getcwd()}\\CM_IMP\\controle_envio_cm_imp_cct.json', orient='records', lines=True)
        if not os.path.exists(os.getcwd()+'\\CM_IMP'):
            os.mkdir('CM_IMP')

        def geracao_de_texto_cm_imp(mensagem_unidade, cd_prod, cd_imp, meta_cd, df_caminhoes=None):
            """
            Gera texto formatado para WhatsApp com resumo estatístico e tabela de caminhões indisponíveis.
            
            Args:
                mensagem_unidade (str): Código da unidade
                cd_prod (int): Número de caminhões disponíveis
                cd_imp (int): Número de caminhões indisponíveis
                meta_cd (int): Meta de caminhões disponíveis
                df_caminhoes (pandas.DataFrame, optional): DataFrame com detalhes dos caminhões indisponíveis
            
            Returns:
                str: Mensagem formatada para WhatsApp
            """
            # Parte 1: Resumo estatístico (mantido como estava)
            mensagem = f'⚠️ *ATENÇÃO {mensagem_unidade}*: No momento estamos com *{cd_imp} CM indisponíveis* e *{cd_prod} CM disponíveis*.\n🎯Nossa meta é ter no mínimo *{meta_cd} CM* disponíveis.'
            
            # Parte 2: Tabela detalhada (se o DataFrame for fornecido)
            if df_caminhoes is not None and not df_caminhoes.empty:
                # Adiciona espaçamento entre o resumo e a tabela detalhada
                mensagem += "\n\n*DETALHAMENTO DOS CAMINHÕES INDISPONÍVEIS:*\n\n"
                
                # Obtém as colunas do DataFrame
                colunas = df_caminhoes.columns.tolist()
                
                # Adiciona linha de cabeçalho (usando negrito)
                cabecalho_parte1 = " | ".join(colunas[:2]) + " | "
                cabecalho_parte2 = " | ".join(colunas[2:])
                mensagem += "*" + cabecalho_parte1 + "*\n*" + cabecalho_parte2 + "*\n"
                mensagem += "-" * 30 + "\n"
                
                # Adiciona cada linha de dados
                for _, row in df_caminhoes.iterrows():
                    # Formata cada linha, truncando valores muito longos
                    linha_formatada = []
                    for idx, col in enumerate(colunas):
                        valor = str(row[col])
                        # Trunca valores muito longos
                        if len(valor) > 20 and col != 'Atividade':
                            valor = valor[:17] + "..."
                        elif len(valor) > 30 and col == 'Atividade':
                            valor = valor[:27] + "..."
                        linha_formatada.append(valor)
                    
                    primeira_parte = " | ".join(linha_formatada[:2]) + " | "
                    segunda_parte = " | ".join(linha_formatada[2:])
                    
                    # Adicionar as duas partes com quebra de linha entre elas
                    mensagem += primeira_parte + "\n" + segunda_parte + "\n\n"
                
                
                mensagem += f"\n_Relatório gerado em: {datetime.now().strftime('%d/%m/%Y %H:%M')}_"
            
            return mensagem
            
        # Caminhos:
        cam_metas = r'\\CSCLSFSR01\Agricola$\Logistica Agroindustrial\CIA 22.23\11. Analytics\BOT CIA\Contatos\CCT_CM_improdutivos.xlsx'
        cam_contatos = r'\\CSCLSFSR01\Agricola$\Logistica Agroindustrial\CIA 22.23\11. Analytics\BOT CIA\Contatos\lista_cd_imp_envio.xlsx'
        # Carregar metas do controle BOT CIA
        df_metas_cm_cct = pd.read_excel(cam_metas)
        convert_und = {'BARRA':'BAR','BENA':'BEN','BONF':'BON','CAAR':'CAA','CNT':'CNT','COPI':'COP','DEST':'DES','DIA':'DIA','GASA':'GAS','IPA':'IPA','JATAI':'JAT','JUN':'JUN','LEM':'LEM','LPT':'LPT','MUND':'MUN','PARAI':'UPA','PTP':'PTP','RAF':'RAF','RBR':'RBR','SCAND':'USC','SEL':'SEL','SERRA':'SER','UMB':'UMB','UNI':'UNI','USH':'USH','VRO':'VRO','ZANIN':'ZAN'}
        df_metas_cm_cct = df_metas_cm_cct[['UNIDADE','CM Indisponível','Necessidade CM produtivo']].replace(convert_und)
        # Carregar contatos BOT CIA
        df_contatos_grupo_cct = pd.read_excel(cam_contatos)
        ###df_contatos_grupo_cct = df_contatos_grupo_cct.loc[df_contatos_grupo_cct['Caminhões Improd'] == 'OK']
        df_contatos_grupo_cct = dict(zip(df_contatos_grupo_cct.Unidade, df_contatos_grupo_cct['Caminhões Improd']))
        # Carregar Base Comunicação
        df_base = carregar_df_monitoramento_SGPA3()
        lista_frentes_cct = '|'.join(list(set([frente[-3:] for frente in df_base['Frente associada'] if 'LN' in frente])))
        df_base = df_base[(df_base['Frente associada'].str.contains(lista_frentes_cct))
                & (df_base["Tipo do equipamento"]=='CAMINHAO CANAVIEIRO')
                & (df_base["Frente associada"].str.contains('-LN-'))]
        df_base['Tempo em atividade'] = df_base['Tempo em atividade'].apply(calcular_tempo)
        df_base['Registro mais recente'] = pd.to_datetime(df_base['Registro mais recente'], dayfirst=True, errors='coerce')
        df_base['Registro mais recente'] = [recente+duracao if type(recente) != float else 'teste' for recente, duracao in zip(df_base['Registro mais recente'], df_base['Tempo em atividade'])]
        df_base['Registro mais recente'] = pd.to_datetime(df_base['Registro mais recente'])
        ## APONTAMENTOS CONFERES
        apontamentos_produtivos = ['891 - Troca Carretas - BV Campo', '895 - Deslocamento Vazio', '888 - Balança - Saída', '889 - Balança - Entrada', '893 - Pátio interno', '882 - Pátio externo', '884 - Desloc. Apos-Descar. Hilo', '885 - Após balança de saída até CT', '892 - Sonda', '896 - Descarregamento hilo', '208 - Parada por condicoes climaticas', '227 - Limitação Indústria', '881 - Deslocamento Carregado', '779 - Carregamento', '890 - Troca Carretas - BV Pat.Ext']
        df_excecao = df_base[(~df_base['Atividade'].str.contains('|'.join(apontamentos_produtivos))) & (df_base['Tempo em atividade'] < timedelta(minutes=0))] #0.01
        lista_a, lista_b, lista_c, lista_d, lista_f = [],[],[],[],[]
        for n in list(df_base['Frente associada'].str[:3].unique()):
            df_pass = df_base[df_base["Frente associada"].str.contains(n)]
            excecao = df_excecao[df_excecao["Frente associada"].str.contains(n)].value_counts().count()
            julg_o = df_pass[df_pass['Atividade'].str.contains('|'.join(map(re.escape, apontamentos_produtivos)))].value_counts().count()
            julg_r = df_pass[~df_pass['Atividade'].str.contains('|'.join(map(re.escape, apontamentos_produtivos)))].value_counts().count()
            lista_a.append(n)
            lista_b.append(julg_o+excecao)
            lista_c.append(julg_r-excecao)
            lista_d.append(int(df_metas_cm_cct[df_metas_cm_cct['UNIDADE']==(n[:3])]['CM Indisponível'].values.sum()))
            lista_f.append(int(df_metas_cm_cct[df_metas_cm_cct['UNIDADE']==(n[:3])]['Necessidade CM produtivo'].values.sum()))
        df_cenario_frentes = pd.DataFrame(list(zip(lista_a,lista_b,lista_c,lista_d,lista_f)), columns=['UNIDADE','Produtiva','Improdutiva','Meta_CD_Imp','CD_Prod_REF'])
        df_cenario_frentes = df_cenario_frentes.eval("sit_CD_Prod = Produtiva - CD_Prod_REF")
        df_cenario_frentes["status"] = ["OK" if s > (-1) else "Gatilho" for s in df_cenario_frentes['sit_CD_Prod']]
        caminho_da_pasta = os.getcwd()
        agora = datetime.now()

        df_cenario_frentes = pd.merge(df_cenario_frentes, controle_envio_cm_imp_cct, on=['UNIDADE'], how='right')

        for idx, row in df_cenario_frentes[(df_cenario_frentes['status'] == 'Gatilho') & (df_cenario_frentes['Produtiva'] != 0)].iterrows():
            #if row[0] == 'PTP':
            if np.datetime64(row[7], 'ms') <= np.datetime64(agora):
                df_export = df_base[df_base['Frente associada'].str.contains(str(row[0]))]
                df_export = df_export.sort_values(by='Frente associada')
                df_export = df_export.rename(columns={'Número do Equipamento':'Equipamento','Tempo em atividade':'Tempo atividade'})
                df_export = df_export[~df_export['Atividade'].isin(apontamentos_produtivos)]
                
                # Aqui fazemos a formatação dos dados antes de passar para a função de geração de texto
                df_export = df_export.sort_values(by=['Atividade', 'Registro mais recente'])
                df_export['Registro mais recente'] = df_export['Registro mais recente'].apply(lambda x: x.strftime('%d/%m/%Y %H:%M') if isinstance(x, pd.Timestamp) else 'Erro')
                df_export['Tempo atividade'] = df_export['Tempo atividade'].apply(lambda x: str(x).replace('days', 'Dias').replace('1 Dias', '1 Dia').replace('0 Dias ', ''))
                
                try:
                    # Selecionamos as colunas desejadas para o relatório
                    df_export_filtrado = df_export[['Equipamento', 'Frente associada', 'Atividade', 'Registro mais recente', 'Tempo atividade']]
                    
                    # Atualizamos o controle de tempo para envio
                    index_limiter_cd = controle_envio_cm_imp_cct[controle_envio_cm_imp_cct.UNIDADE == row.UNIDADE].index[0]
                    controle_envio_cm_imp_cct.loc[index_limiter_cd, 'Envio'] = agora+timedelta(minutes=30)
                    controle_envio_cm_imp_cct.to_json(f'{os.getcwd()}\\CM_IMP\\controle_envio_cm_imp_cct.json', orient='records', lines=True)
                    
                    # Geramos a mensagem com o resumo e a tabela detalhada
                    mensagem_a_ser_enviada = geracao_de_texto_cm_imp(
                        str(row[0]), 
                        round(row[1]),
                        round(row[2]),
                        math.ceil(row[4]),
                        df_export_filtrado  # Passamos o DataFrame filtrado para gerar a tabela
                    )
                    
                    # Enviamos para cada contato na lista de contatos para esta unidade
                    contato, tipo_contato = verificar_tipo_de_contato(df_contatos_grupo_cct[row[0]])
                    contatos = contato.split(';')
                    for c in contatos:
                        # Utilizamos string vazia no lugar de None para o caminho_anexo, conforme solicitado
                        gravar_em_banco_para_envio([('CCT_CM', datetime.now(), c, tipo_contato, mensagem_a_ser_enviada, '')])
                        
                except Exception as e:
                    print(f'Erro ao processar alerta de caminhões indisponíveis: {row[0]} para {contato} - {str(e)}')
    
    CM_improdutivos_CCT_SPGA3()

#################### MOD IMP. PROD

def convert_real_date(value_str_of_date):
    #print(value_str_of_date) # Quando recente o mês vem em value 2
    # Value 1
    val1 = str(value_str_of_date).split(' ')[0].split('-')[1]
    # Value 2
    val2 = str(value_str_of_date).split(' ')[0].split('-')[2]
    # Ref month
    ref_month_str = str(datetime.now().month) if(datetime.now().month > 9) else f'0{datetime.now().month}'
    try:
        if val1 == ref_month_str:
            return datetime.strptime(str(value_str_of_date), '%Y-%m-%d %H:%M:%S')
        elif val2 == ref_month_str:
            return datetime.strptime(str(value_str_of_date), '%Y-%d-%m %H:%M:%S')
        elif int(val2) < int(val1):
            return datetime.strptime(str(value_str_of_date), '%Y-%d-%m %H:%M:%S')
        elif int(val1) < int(val2):
            return datetime.strptime(str(value_str_of_date), '%Y-%m-%d %H:%M:%S')
        elif int(val1) == int(val2):
            return datetime.strptime(str(value_str_of_date), '%Y-%m-%d %H:%M:%S')
    except:
        return datetime(1999,12,3,10,10,10)

def get_dd_mm_yy_date(input_datetime_python):
    if type(input_datetime_python) is datetime:
        second_f = input_datetime_python.second if input_datetime_python.second > 9 else f'0{input_datetime_python.second}'
        minute_f = input_datetime_python.minute if input_datetime_python.minute > 9 else f'0{input_datetime_python.minute}'
        mes_f = input_datetime_python.month if input_datetime_python.month > 9 else f'0{input_datetime_python.month}'
        return f'{input_datetime_python.day}/{mes_f}/{input_datetime_python.year} {input_datetime_python.hour}:{minute_f}:{second_f}'
    else: return input_datetime_python

controle_envio_fun2 = {}
controle_envio_fun2['FALTA'] = {}
controle_envio_fun2['REFEICAO'] = {}
controle_envio_fun2['TURNO'] = {}
controle_envio_fun2['ABASTECIMENTO'] = {}
controle_envio_fun2['ABASTEC_MUDA'] = {}
controle_envio_fun2['FALTA_MUDA'] = {}
controle_envio_fun2['SEM_APT'] = {}
pd.set_option('mode.chained_assignment', None)

def contatos_segunda_funcao_prod():
    df_seg_fun_prod = pd.read_excel(r"\\CSCLSFSR01\Agricola$\Logistica Agroindustrial\CIA 22.23\11. Analytics\BOT CIA\Contatos\Segunda_Funcao_Prod.xlsx")
    df_seg_fun_prod = df_seg_fun_prod[df_seg_fun_prod.CONTROLE_Apt_Improdutivas == 'SIM']
    df_seg_fun_prod['Frente'] = df_seg_fun_prod['Sigla_Unidade'].astype(str) + "-" + df_seg_fun_prod['Sigla_Frente'].astype(str) + "-"
    df_seg_fun_prod = df_seg_fun_prod[['Frente','Torre_Numero']]
    df_seg_fun_prod.dropna(inplace=True, axis=0)
    dict_contatos_seg_prod = dict(list(zip(df_seg_fun_prod.Frente,df_seg_fun_prod.Torre_Numero)))
    return dict_contatos_seg_prod

def mensagem_2f_prod(data):
    lista_2f_p = []
    if 'falta' in data['Atividade'].lower():
        lista_2f_p.append(f"⚠️📟 *Atenção! Apontamento {data['Atividade'].split(' - ')[1]}*")
    elif 'refeição' in data['Atividade'].lower() or 'refeicao' in data['Atividade'].lower():
        lista_2f_p.append(f"⚠️📟 *Atenção! Apontamento Refeição*")
    elif '834' in data['Atividade'].lower() or '834' in data['Atividade'].lower():
        lista_2f_p.append(f"⚠️📟 *Atenção! Sem Apontamento*")
    elif 'turno' in data['Atividade'].lower():
        lista_2f_p.append(f"⚠️📟 *Atenção! Apontamento Troca de Turno*")
    elif 'abast' in data['Atividade'].lower():
        if 'mudas plantadora' in data['Atividade'].lower(): lista_2f_p.append(f"⚠️📟 *Atenção! Apontamento Abastecimento Mudas*")
        else: lista_2f_p.append(f"⚠️📟 *Atenção! Apontamento Abastecimento {str(data['Atividade']).split(' ')[-1]}*")
    lista_2f_p.append(f"*Frente:* {data['Frente associada']}")
    lista_2f_p.append(f"*Frota:* {data['Número do Equipamento']}")
    lista_2f_p.append(f"*Tipo:* {data['Tipo do equipamento']}")
    lista_2f_p.append(f"*Comunicação:* {get_dd_mm_yy_date(convert_real_date(data['Registro mais recente']))}")
    lista_2f_p.append(f"⌛ *Duração:* {str(data['Tempo em atividade'])[-8:]}")
    #if data['Atividade'].split(' - ')[0] in '226 -|839 -|840 -':
        #lista_2f_p.append(f"\nAtenção para a regra de apontamento! Se o raio estiver acima do orçado o apontamento deve ser 'Falta de Muda Raio'.\nSe raio estiver dentro da meta e a DF estiver abaixo da meta o apontamento deve ser 'Falta de Muda DF'.\nSe nenhum dos critérios acima se encaixarem o apontamento deve ser 'Falta de Muda Operacional'.")
    return '\n'.join(map(str, lista_2f_p))

#### Geração do report email Manutenção

def geracao_relatorio_email_mautencao_OS_Aguardando_info():
    import matplotlib.pyplot as plt
    import seaborn as sns
    import base64
    from pathlib import Path
    from matplotlib.dates import DateFormatter

    caminho = r'C:\CIAANALYTICS\1 - Producao\1 4 - Banco\envio_msg.db'
    #caminho = r'\\CSCLSFSR01\Agricola$\Logistica Agroindustrial\CIA 22.23\11. Analytics\1 4 - Banco\envio_msg.db'
    conn_consulta = sqlite3.connect(caminho)
    df = pd.read_sql("""SELECT * FROM envio_msg WHERE gerada_por = 'MANUT_OS_Ag_Info'""", conn_consulta)
    conn_consulta.close()

    df["gerada_em"] = pd.to_datetime(df["gerada_em"].str[:19])
    dfa = df[(df["gerada_por"]=='MANUT_OS_Ag_Info') & (df["para_"]=='BOT CIA - Manut. & Comb.') & (df["gerada_em"] > datetime.now()-timedelta(days=60))]
    dfa[['Frota','TipoFrota', 'Frente','Tempo']] = dfa['mensagem'].apply(
        lambda msg: pd.Series(
            [msg.split('\n')[1].split(' ')[-3],  # TipoFrota
            msg.split('\n')[1].split(' ')[-1],  # TipoFrota
            msg.split('\n')[2].split(' ')[-1],  # Unidade
            msg.split('\n')[5].replace('❗ Tempo sem informação: ','') if len(msg.split('\n')) > 4 else None],  # Tempo,
        )
    )
    dfa["Frente"] = dfa["Frente"].str.replace('nan','SEM-00-00')
    dfa["Unidade"] = dfa["Frente"].str[:3]
    dfa["turno"] = ['A' if ref_hour in [7,8,9,10,11,12,13,14] else 'B' if ref_hour in [15,16,17,18,19,20,21,22] else 'C' for ref_hour in dfa["gerada_em"].dt.hour]
    dfa["data"] = [x.date() for x in dfa["gerada_em"]]
    dfa["Tempo"] = pd.to_timedelta(dfa["Tempo"])
    dfa['evento_gerado'] = [f+"&"+str(g-t)[:13] for f,g,t in zip(dfa['Frota'],dfa['gerada_em'],dfa['Tempo'])]
    relacao_evento_tempo = dfa.groupby('evento_gerado').apply(lambda x: x["Tempo"].max()).to_dict()
    dfa = dfa.sort_values(by='gerada_em')
    dfa = dfa.drop_duplicates("evento_gerado", keep='first')
    dfa["TempoMax"] = dfa["evento_gerado"].map(relacao_evento_tempo)
    dfat = dfa
    dfa = dfa[(dfa["gerada_em"] > datetime.now()-timedelta(days=7))]

    # Paleta de cores para os turnos
    turno_palette = {
        'A': '#1FC0DA',  # turno A
        'B': '#F47920',  # turno B
        'C': '#EA368E'   # turno C
    }

    # Função para salvar a imagem do gráfico e convertê-la para base64
    def save_plot_as_base64(fig, file_name):
        """Salva o gráfico como imagem e retorna o conteúdo base64"""
        fig_path = Path(f"{file_name}.png")
        fig.savefig(fig_path, format='png', bbox_inches='tight', dpi=300)  # Aumenta a qualidade da imagem
        plt.close(fig)  # Fecha o gráfico para liberar a memória
        img_data = fig_path.read_bytes()  # Lê os bytes do arquivo
        return base64.b64encode(img_data).decode('utf-8')

    # Função para criar e salvar o gráfico de forma modular
    def criar_grafico(tipo, **kwargs):
        """Cria e salva um gráfico de acordo com o tipo e parâmetros fornecidos"""
        if tipo == 'barplot':
            fig, ax = plt.subplots(figsize=kwargs.get('figsize', (8, 6)))
            sns.barplot(x=kwargs['x'], y=kwargs['y'], hue=kwargs.get('hue'), palette=turno_palette, data=kwargs['data'], ax=ax, dodge=kwargs.get('dodge', True))
        elif tipo == 'lineplot':
            fig, ax = plt.subplots(figsize=kwargs.get('figsize', (10, 6)))
            sns.lineplot(x=kwargs['x'], y=kwargs['y'], hue=kwargs.get('hue'), palette=turno_palette, data=kwargs['data'], marker='o', ax=ax)
        else:
            raise ValueError("Tipo de gráfico não suportado")
        
        ax.set_title(kwargs.get('title', ''))
        ax.set_xlabel(kwargs.get('xlabel', ''))
        ax.set_ylabel(kwargs.get('ylabel', ''))
        return fig

    # -------------------------------
    # Passo 1: Gráfico 1 - Top 3 Unidades mais Recorrentes
    # -------------------------------
    contagem_unidades = dfa['Unidade'].value_counts().nlargest(3)

    raizen_roxo = '#781E77'
    fig, (ax1, ax2, ax3) = plt.subplots(1, 3, figsize=(30, 6))  # Agora temos 3 subplots no total

    # Gráfico de barras das 3 maiores ofensoras
    sns.barplot(x=contagem_unidades.values, y=contagem_unidades.index, ax=ax1, dodge=False, color=raizen_roxo)
    for p in ax1.patches:
        ax1.annotate(f'{p.get_width():.0f}', 
                    (p.get_x() + p.get_width() / 2, p.get_y() + p.get_height() / 2),  # Centro da barra
                    ha='center', va='center', 
                    color='white', fontsize=22,
                    bbox=dict(facecolor='gray', alpha=0.5, edgecolor='none', boxstyle='round,pad=0.3'))
    ax1.set_title('Top 3 Maiores Ofensoras')
    ax1.set_xlabel('Quantidade')
    ax1.set_ylabel('Unidade')
    ax1.set_xticklabels([])  # Remove a escala de valores do eixo X

    # Gráfico de barras da distribuição por turno das 3 maiores ofensoras
    sns.countplot(x='Unidade', hue='turno', data=dfa[dfa['Unidade'].isin(contagem_unidades.index)], ax=ax2, palette=turno_palette)
    ax2.set_title('Distribuição por Turno das 3 Maiores Ofensoras')
    ax2.set_xlabel('Unidade')
    ax2.set_ylabel('Quantidade')

    # Gráfico de pizza para a soma de eventos por turno
    eventos_por_turno = dfa['turno'].value_counts()
    labels = eventos_por_turno.index
    sizes = eventos_por_turno.values
    explode = [0.05 if size == max(sizes) else 0 for size in sizes]  # Destaque para o maior segmento

    # Gráfico de pizza com percentual e contagem e personalização de texto
    wedges, texts, autotexts = ax3.pie(sizes, labels=labels, 
                                    autopct=lambda p: f'{p:.1f}%\n({int(p*sum(sizes)/100)})', 
                                    startangle=90, 
                                    explode=explode, 
                                    colors=[turno_palette[label] for label in labels])

    for text in texts: text.set_fontsize(22)  # Tamanho maior

    # Personalizar o texto dentro da pizza
    for autotext in autotexts:
        autotext.set_color('white')  # Cor branca
        autotext.set_fontsize(18)  # Tamanho maior
        autotext.set_bbox(dict(facecolor='gray', alpha=0.6, edgecolor='none', boxstyle='round,pad=0.3'))  

    ax3.set_title('Contagem Percentual por Turno')
    # Salva a imagem da figura como base64
    img_base64_fig = save_plot_as_base64(fig, 'top3_unidades')

    # -------------------------------
    # Passo 2: Gráfico 2 - Ranking de todas as Unidades
    # -------------------------------
    # Calcule a quantidade de ocorrências de cada Unidade
    dfa_unidade_contagem = dfa.groupby(['Unidade', 'turno']).size().reset_index(name='Quantidade')

    fig2 = criar_grafico(tipo='barplot', 
                        x='Quantidade', y='Unidade', hue='turno', 
                        data=dfa_unidade_contagem, dodge=True, 
                        title='Ranking de Todas as Unidades', 
                        xlabel='Quantidade', ylabel='Unidade')
    img_base64_fig2 = save_plot_as_base64(fig2, 'ranking_todas_unidades')

    # -------------------------------
    # Passo 3: Gráfico 3 - Ranking de todas as Frentes
    # -------------------------------
    # Calcule a quantidade de ocorrências de cada Frente
    dfa_frente_contagem = dfa.groupby(['Frente', 'turno']).size().reset_index(name='Quantidade')
    frentes_top = list(dfa["Frente"].value_counts().head(10).keys())
    dfa_frente_contagem = dfa_frente_contagem[dfa_frente_contagem["Frente"].isin(frentes_top)]

    fig3 = criar_grafico(tipo='barplot', 
                        x='Quantidade', y='Frente', hue='turno', 
                        data=dfa_frente_contagem, dodge=True, 
                        title='Ranking Frentes (10 Maiores Ofensoras)', 
                        xlabel='Quantidade', ylabel='Frente')
    img_base64_fig3 = save_plot_as_base64(fig3, 'ranking_todas_frentes')

    # -------------------------------
    # Passo 4: Gráfico 4 - Média tempomax
    # -------------------------------
    # Configuração do gráfico de barras em pé
    fig4, ax1 = plt.subplots(figsize=(12, 6))  # Gráfico vertical
    # Ordenando os dados de TempoMax de forma decrescente
    dfa_sorted = dfa.sort_values(by='TempoMax', ascending=False)
    # Convertendo TempoMax para um formato humano legível (horas)
    dfa_sorted['TempoMax'] = dfa_sorted['TempoMax'].dt.total_seconds() / 3600  # Converte para horas
    # === PLOT: TempoMax por Unidade === #
    df_mean = dfa_sorted.groupby(['Unidade', 'turno'])['TempoMax'].mean().reset_index()  # Calcula a média por Unidade e Turno
    sns.barplot(
        data=df_mean, 
        x='Unidade', 
        y='TempoMax', 
        hue='turno',  # Diferenciando as barras pelo turno
        palette=turno_palette,  # Usando a paleta de cores dos turnos
        ax=ax1  # Define que o gráfico será no eixo ax1
    )
    # Configurações do gráfico de barras
    ax1.set_title('Média de Horas OS Aguardando Informação por Unidade', fontsize=14)
    ax1.set_xlabel('Unidade', fontsize=12)
    ax1.set_ylabel('Duração (horas)', fontsize=12)

    # Ajustar o espaçamento entre os gráficos
    plt.tight_layout()  # Evita sobreposição dos elementos
    img_base64_fig4 = save_plot_as_base64(fig4, 'media_unidade_frente')
    # -------------------------------
    # Passo 5: Gráfico 5 - Contagem de Linhas por Turno ao Longo do Tempo
    # -------------------------------
    # Dados para contagem por hora e turno
    contagem_por_hora_turno = dfat.groupby(['data', 'turno']).size().reset_index(name='Quantidade')

    # Criação da figura e eixos
    fig5, (ax1, ax2) = plt.subplots(1, 2, figsize=(20, 6))  # Dois gráficos lado a lado

    # Gráfico de linha no ax1 com a paleta de cores personalizada
    sns.lineplot(x='data', y='Quantidade', hue='turno', data=contagem_por_hora_turno, ax=ax1, palette=turno_palette)
    ax1.set_title('Eventos por Turno ao Longo do Tempo', fontsize=22)
    ax1.set_xlabel('Data', fontsize=18)
    ax1.set_ylabel('Quantidade', fontsize=18)

    # Personalização de texto
    for label in ax1.get_xticklabels() + ax1.get_yticklabels():
        label.set_fontsize(14)

    # Ajustando o formato das datas no eixo 
    ax1.xaxis.set_major_formatter(DateFormatter('%d-%m'))  # Exemplo de formato 'dia-mês'
    ax1.tick_params(axis='x', rotation=45)  # Rotaciona os rótulos de data para 45 graus para evitar sobreposição

    # Gráfico de pizza no ax2
    # Contagem por turno para a pizza
    eventos_por_turno = dfat['turno'].value_counts()
    labels = eventos_por_turno.index
    sizes = eventos_por_turno.values
    explode = [0.05 if size == max(sizes) else 0 for size in sizes]  # Destaque para o maior segmento

    # Gráfico de pizza com as cores da paleta personalizada
    wedges, texts, autotexts = ax2.pie(sizes, labels=labels, 
                                    autopct=lambda p: f'{p:.1f}%\n({int(p*sum(sizes)/100)})', 
                                    startangle=90, 
                                    explode=explode, 
                                    colors=[turno_palette[label] for label in labels])

    # Ajustes de texto na pizza
    for text in texts:
        text.set_fontsize(22)  # Tamanho maior

    for autotext in autotexts:
        autotext.set_color('white')  # Cor branca
        autotext.set_fontsize(18)  # Tamanho maior
        autotext.set_bbox(dict(facecolor='gray', alpha=0.6, edgecolor='none', boxstyle='round,pad=0.3'))

    ax2.set_title('Distribuição por Turno', fontsize=22)

    # Salvar a imagem da figura como base64
    img_base64_fig5 = save_plot_as_base64(fig5, 'contagem_por_hora_turno')

    # -------------------------------
    # Passo 5: Geração do Relatório HTML
    # -------------------------------

    dd = datetime.now()

    """Gera o relatório HTML com os gráficos embutidos"""
    html_content = f"""<!DOCTYPE html>
    <html lang='pt-BR'>
        <head>
        <meta charset='UTF-8'>
        <title>Relatório de Recorrência de Unidades</title>
        <style>
            h1 {{ font-size: 20px; }}
            h2 {{ font-size: 16px; }}
        </style>
    </head>
    <body>
        <h1>Relatório de Recorrência de Unidades</h1>

        <p>Segue report BOT CIA das Ordens de Serviço sem Descrição informada de {(dd-timedelta(days=7)).strftime('%d/%m/%Y')} até {dd.strftime('%d/%m/%Y')}.</p>
        
        <h2>1. Contagem Eventos Top 3 Unidades Maiores Ofensoras D-7</h2>
        <img src='data:image/png;base64,{img_base64_fig}' alt='Gráfico Top 3 Unidades' width='1000'>
        
        <h2>2. Ranking da Contagem Eventos Todas as Unidades D-7</h2>
        <img src='data:image/png;base64,{img_base64_fig2}' alt='Gráfico Ranking de Todas as Unidades' width='600'>
        
        <h2>3. Ranking 10 Frentes da Contagem Eventos de Maiores Ofensoras D-7</h2>
        <img src='data:image/png;base64,{img_base64_fig3}' alt='Gráfico Ranking de Todas as Frentes' width='600'>
        
        <h2>4. Média de Duração de Evento por Unidade/Turno D-7</h2>
        <img src='data:image/png;base64,{img_base64_fig4}' alt='Gráfico Contagem por Hora' width='800'>
        
        <h2>5. Contagem Eventos por Turno ao Longo do Tempo D-60</h2>
        <img src='data:image/png;base64,{img_base64_fig5}' alt='Gráfico Contagem por Hora' width='800'>

        <p>Atenciosamente<br>BOT CIA - CIA Performance.</p>
    </body>
    </html>"""

    caminho_share_telegram = verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\CIA Analytics - BOT CIA\Extrator\Reports')
    caminho_salvar_html = os.path.join(caminho_share_telegram,'Report_Manutencao.html')
    with open(caminho_salvar_html, 'w', encoding='utf-8') as f:
        f.write(html_content)

#################### Bloqueio Despacho CIA

def bloqueio_despacho_carretas(): 
    caminho_sharepointBOTCIA = verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\CIA Analytics - BOT CIA')
    caminho_sharepointGovernanca = verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\Metas_Gov_Indicadores - Documentos')

    caminho_azure = os.path.join(caminho_sharepointBOTCIA,'Extrator','Azure')
    caminho_manutencaoLP = os.path.join(caminho_sharepointBOTCIA,'Extrator','Manutencao')
    caminho_contatosCCT = r'\\CSCLSFSR01\Agricola$\Logistica Agroindustrial\CIA 22.23\11. Analytics\BOT CIA\Contatos\CCT_Contatos.xlsx'
    cam_az_ifrotaCarretas = os.path.join(caminho_azure,'IFROTA_CARRETAS.parquet')
    caminho_msvFiltrado = os.path.join(caminho_manutencaoLP,'MSV_Filtrado.xlsx')
    cam_az_bufferReprovados = os.path.join(caminho_manutencaoLP,'buffer_eixos_reprovados_Mk75.xlsx')
    cam_expurgosCctCorporativo = os.path.join(caminho_manutencaoLP,'Expurgos BI Robô Mark7.xlsx')
    cam_cadastrosCctGov = os.path.join(caminho_sharepointGovernanca,'CCT','Cadastros.xlsx')

    dfs = pd.read_parquet(cam_az_ifrotaCarretas)
    dfb = pd.read_excel(cam_az_bufferReprovados)
    dfb = dfb.drop_duplicates(subset='Frota')
    expur = pd.read_excel(cam_expurgosCctCorporativo, sheet_name='Expurgo', header=1)
    msv = pd.read_excel(caminho_msvFiltrado)
    msv["SITUAÇÃO"] = [int(el[0]) for el in msv["SITUAÇÃO"].str.split(' ')]
    #msv = msv[((msv["NO REF"]==180) & (msv["SITUAÇÃO"]>9)) | ((msv["NO REF"]==360) & (msv["SITUAÇÃO"]>18))]
    msv = msv[msv["SITUAÇÃO"] > msv["NO REF"] * 0.05]
    msv_vencidos = list(msv["CD_EQUIP"].unique())
    expur["DATA DE TÉRMINO"] = pd.to_datetime(expur["DATA DE TÉRMINO"], errors='corerce')
    expur = expur.dropna(subset="DATA DE TÉRMINO")
    expur.loc[expur["DATA DE TÉRMINO"] < datetime.now(), "EXPURGO"] = "EXPIRADO"
    expur = expur[expur["EXPURGO"]=='APROVADO']
    expur["FROTA"] = expur["FROTA"].astype(int)
    excecoes = list(expur["FROTA"].unique())
    dfss = pd.read_excel(cam_cadastrosCctGov, sheet_name='INSTANCIAS')
    dfss = dfss[['CD_UNID_IND','Unidade Frente','Unidade_GRD']]
    dados_extras = []
    '''dados_extras = [
        (39,'COP','COSTA PINTO'),
        (45,'UPA','PARAÍSO'),
        (33,'SEL','SANTA ELISA'),
        (70,'UNI','UNIVALEM'),
        (75,'USC','SANTA CÂNDIDA'),
        (25,'RBR','RIO BRILHANTE'),
        (34,'UMB','MORRO AGUDO'),
        (91,'UNI','UNIVALEM'),
        (71,'VRO','VALE DO ROSARIO'),
        (42,'COP','COSTA PINTO'),
        (54,'COP','COSTA PINTO'),
        (35,'UMB','MORRO AGUDO'),
        (47,'COP','COSTA PINTO'),
        (66,'UPA','PARAISO'),
        (65,'BAR','BARRA'),
        (31,'ZAN','ZANIN'),
        (32,'LPT','LAGOA DA PRATA'),
        (20,'BEN','BENALCOOL'),
        (28,'CNT','CONTINENTAL'),
        (69,'JUN','JUNQUEIRA'),
        (55,'IPA','IPAUSSU'),
        (37,'LEM','LEME')]'''
    estras_dfss = pd.DataFrame(dados_extras, columns=dfss.columns)
    dfss = pd.concat([dfss,estras_dfss])
    dff = pd.merge(left=dfs, right=dfss, left_on='BASE', right_on='CD_UNID_IND', how='left')
    dff["Unidade Frente"] = dff["Unidade Frente"].fillna('SEM UNIDADE')
    dfb = dfb[~(dfb["Frota"].isin(excecoes))]
    bloqueados = list(dfb.Frota.unique())
    bloqueados = bloqueados + msv_vencidos
    contatos = pd.read_excel(caminho_contatosCCT)
    contatos1 = contatos[contatos["CTRL_BloqueioIfrota"]=='SIM'][["Sigla_Unidade","Torre_Numero"]]
    contatos2 = {und:num for und,num in zip(contatos1["Sigla_Unidade"],contatos1["Torre_Numero"])}

    # 1 Verificação: Reprovados LPI existem como liberados no lake? (manut --> lake)
    ##### conclusão: Essas carretas precisam ser bloqueadas
    df = dff[(dff["CODIGO"].isin(bloqueados)) & ((dff["UTILIZADO"]==True) & (dff["SAFRA"]==2024))]
    df = df.drop_duplicates(subset=['CODIGO','Unidade Frente'])

    # Nome do banco de dados
    DB_NAME = 'bloqueio_carretas_hist.db'
        # 0 = Precisa ser bloqueado
        # 1 = Foi bloqueado
        # 2 = Não foi bloqueado, mensagem será reenviada

    def initialize_database():
        conn = sqlite3.connect(DB_NAME) #add_record(123, 'Unidade A', '2024-11-22', 0)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS bloqueio_carretas_hist (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                int_frota INTEGER,
                str_unidade TEXT,
                dt_aviso TEXT,
                bool_status INTEGER
            )
        ''')
        conn.commit()
        conn.close()

    def add_record(int_frota, str_unidade, dt_aviso, bool_status):
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO bloqueio_carretas_hist (int_frota, str_unidade, dt_aviso, bool_status)
            VALUES (?, ?, ?, ?)
        ''', (int_frota, str_unidade, dt_aviso, bool_status))
        conn.commit()
        conn.close()

    def modify_record(id, **kwargs):
        conn = sqlite3.connect(DB_NAME) #modify_record(1, bool_status=2)
        cursor = conn.cursor()
        for column, value in kwargs.items():
            cursor.execute(f'''
                UPDATE bloqueio_carretas_hist
                SET {column} = ?
                WHERE id = ?
            ''', (value, id))
        conn.commit()
        conn.close()

    initialize_database()

    def fetch_records():
        conn = sqlite3.connect(DB_NAME)
        records = pd.read_sql('SELECT * FROM bloqueio_carretas_hist',conn)
        conn.close()
        return records

    # Exibe os registros
    hist = fetch_records()
    hist["dt_aviso"] = pd.to_datetime(hist["dt_aviso"])
    hist["int_frota"] = hist["int_frota"].astype(int)
    hist["time"] = datetime.now() - hist["dt_aviso"]
    list_repetivas = []
    for i,row in hist[hist["bool_status"]==0].iterrows():
        if timedelta(hours=3) < row["time"]:
            modify_record(row["id"], bool_status=2)
            list_repetivas.append(row)
            
    hist = fetch_records()
    hist["dt_aviso"] = pd.to_datetime(hist["dt_aviso"])
    hist["int_frota"] = hist["int_frota"].astype(int)
    existem_historico = list(hist[(hist["bool_status"]==0)]["int_frota"].unique())
    bloquear = df[~(df["CODIGO"].isin(existem_historico))]

    compilado_bloqueio_atual = []
    compilado_bloqueio_atual.append(f'🚨*Bloqueio Carretas Despacho IFROTA*')
    for frente, data in bloquear.groupby('Unidade Frente'):
        if frente in contatos2.keys():
            mensagem = []
            mensagem.append(f'🚨*Bloqueio Carretas Despacho IFROTA*')
            mensagem.append(f'_Reprovados Análise LPI da {frente}_\n')
            for i,row in data.iterrows():
                frota = row["CODIGO"]
                mensagem.append(f'Carreta: {frota}')
                compilado_bloqueio_atual.append(f'[{frente}] Carreta: {frota}')
                add_record(frota, frente, datetime.now(), 0)
            mensagem.append('\nFavor seguir com o bloqueio destas carretas via IFROTA.')
            mensagem_final = '\n'.join(mensagem)
            print(mensagem_final)
            contato_envio = contatos2[frente]
            contato, tipo_contato = verificar_tipo_de_contato(contato_envio)
            gravar_em_banco_para_envio([('CCT_BloqueioCarretas',datetime.now(),contato, tipo_contato, mensagem_final, '')])
        else:
            print(f'Bloqueio Despacho CIA [{frente}]: Pulada! Não temos destinos para este')
    
    if len(compilado_bloqueio_atual) > 1:
        # Segunda via para grupo gestão
        contato, tipo_contato = verificar_tipo_de_contato('BOT CIA - CCT')
        mensagem_final = '\n'.join(compilado_bloqueio_atual)
        gravar_em_banco_para_envio([('CCT_BloqueioCarretas',datetime.now(),contato, tipo_contato, mensagem_final, '')])

    a = r'''esquecidos = pd.DataFrame([x for x in list_repetivas])
    if not esquecidos.empty:
        mensagem_esquecidos = []
        mensagem_esquecidos.append(f'🚨 *Carretas não bloqueadas*\n')
        for frente, data in esquecidos.groupby('str_unidade'):
            if frente in contatos2.keys():
                for i,row in data.iterrows():
                    frota_1 = row["int_frota"]
                    mensagem_esquecidos.append(f'Carreta {frota_1}  [{frente}]')
        if len(mensagem_esquecidos) > 1:
            print('Temos frotas esquecidas!')
            mensagem_final = '\n'.join(mensagem_esquecidos)
            #print(mensagem_final)
            contato, tipo_contato = verificar_tipo_de_contato('BOT CIA - CCT')
            gravar_em_banco_para_envio([('CCT_BloqueioCarretas',datetime.now(),contato, tipo_contato, mensagem_final, '')])'''

#################### ABERTURA TURNO TABLET

def gatilho_iniciar_verificacao_tablet_Comboio():
    def verificar_abertura_tablets():
        def gerar_imagens_status_tablet(unidade_alvo,data_frame):
            def format_identificador_abertura(val):
                paleta_cores = {'Amarelo': '#edcb8b', 'Vermelho': '#ff9e96', 'Verde': '#b8ffb8', 'Cinza': '#f2f2f2'}
                try:
                    celula = str(val)
                    if celula == 'NÃO ABERTO':
                        color = paleta_cores['Vermelho']
                    elif celula == 'ABERTO':
                        color = paleta_cores['Verde']
                    else:
                        color = ''
                except Exception:
                    color = paleta_cores['Cinza']
                return 'background-color: {}'.format(color) 
            base = data_frame[data_frame.Unidade==unidade_alvo]
            base = base[['Comboio','Unidade','Status']]
            base = base.style.applymap(format_identificador_abertura)
            base.hide(axis="index")
            if not os.path.exists(os.getcwd()+'\\Abertura_Turno_Tablet'):
                os.mkdir('Abertura_Turno_Tablet')
            dfi.export(base,'Abertura_Turno_Tablet\\abertura_tablet_'+str(unidade_alvo)+'.png')
            return os.path.abspath('Abertura_Turno_Tablet\\abertura_tablet_'+str(unidade_alvo)+'.png')
        def sub_formatar_base_tablet(caminho_downloads,tipo_arquivo):
            folder_path = caminho_downloads
            file_type =  tipo_arquivo
            files = glob.glob(folder_path+file_type)
            arquivo_mais_recente = max(files, key=os.path.getctime)
            return arquivo_mais_recente
        def roteiros_gerados():
            ac = pd.read_excel(verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\MinhaTI\CIA Analytics - Comboio\Acompanhamento_Smart_route.xlsx'))
            ac = ac[['plannedFuelTruck','programmingUnit']]
            ac = ac.drop_duplicates()
            ac = ac.dropna()
            ac = ac.reset_index(drop=True)
            ac.plannedFuelTruck = ac.plannedFuelTruck.astype(int)
            ac.rename(columns={'plannedFuelTruck':'Comboio','programmingUnit':'Unidade'}, inplace=True)
            return ac

        rotas_realizadas = roteiros_gerados()

        caminho_tablet = r'\\csclsfsr03\SoftsPRD\Extrator\PRD\Sistema Apontamentos\Turno x Meterial'
        df = pd.read_csv(sub_formatar_base_tablet(caminho_tablet,'\*csv'), sep=';')
        df.INICIO_TURNO = pd.to_datetime(df.INICIO_TURNO, format="%Y-%m-%d %H:%M:%S")

        celula = []
        for id,row in rotas_realizadas.iterrows():
            if len(df.loc[(df.cd_equipamento==row.Comboio)&(df.INICIO_TURNO > datetime.now()-timedelta(hours=6))]) > 0:
                celula.append('ABERTO')
            else: celula.append('NÃO ABERTO')

        rotas_realizadas = pd.concat([rotas_realizadas,pd.DataFrame(celula, columns=['Status'])], axis=1)
        envio_para = {} # 'UND':'C:Caminho/Para/Arquivo'
        for und in rotas_realizadas.Unidade.unique():
            try: 
                caminho = gerar_imagens_status_tablet(und,rotas_realizadas)
                envio_para[und] = caminho
            except IndexError: pass
        return envio_para
    def momento_atual():
        escrita = str(datetime.now())[:10]
        return f'{escrita.split("-")[2]}/{escrita.split("-")[1]}/{escrita.split("-")[0]}'
    print(f'\n--> {str(datetime.now())[:16]} | Condições para lógica "aberturas turno tablet!" iniciada')
    turno_msg = ['TURNO C' if datetime.now().hour < 7 else 'TURNO A' if datetime.now().hour < 15 else 'TURNO B' if datetime.now().hour < 23 else 'TURNO C']
    envio = verificar_abertura_tablets()
    contatos_envio_tablet = pd.read_excel(r'\\CSCLSFSR01\Agricola$\Logistica Agroindustrial\CIA 22.23\11. Analytics\BOT CIA\Contatos\Envio_tablets.xlsx')
    contatos_envio_tablet = contatos_envio_tablet.dropna()

    for und,caminho in zip(envio.keys(),envio.values()):
        if 'BONF' not in und:
            mensagem_a_ser_enviada = f"⚠️ *ABERTURA TURNO TABLET: {und}*\nDATA {momento_atual()} do {turno_msg[0]}"
            anexo_a_ser_enviado = caminho
            contato, tipo_contato = verificar_tipo_de_contato(str(contatos_envio_tablet[contatos_envio_tablet.Unidade==und].values[0][1]))
            gravar_em_banco_para_envio([('COMBOIO_Abertura_Tablet',datetime.now(),contato, tipo_contato, mensagem_a_ser_enviada, anexo_a_ser_enviado)])
        else:
            print(f'**** Unidade: "{und}" não teve comboio cadastrado')
    print(f'\n--> {str(datetime.now())[:16]} | Ciclo concluído com sucesso!\n')

def baseComboioExtrator():
    folder_path = r'\\csclsfsr03\SoftsPRD\Extrator\PRD\Sistema Apontamentos\Turno x Meterial'
    file_type =  '\*csv'
    files = glob.glob(folder_path+file_type)
    arquivo_mais_recente = max(files, key=os.path.getctime)
    if datetime.fromtimestamp(os.path.getctime(arquivo_mais_recente)) > datetime.now()-timedelta(hours=3):
        return True
    else: 
        return False  

def gatilho_iniciar_verificacao_tablet_Comboio_BONF():
    def verificar_abertura_tablets():
        def gerar_imagens_status_tablet(unidade_alvo,data_frame):
            def format_identificador_abertura(val):
                paleta_cores = {'Amarelo': '#edcb8b', 'Vermelho': '#ff9e96', 'Verde': '#b8ffb8', 'Cinza': '#f2f2f2'}
                try:
                    celula = str(val)
                    if celula == 'NÃO ABERTO':
                        color = paleta_cores['Vermelho']
                    elif celula == 'ABERTO':
                        color = paleta_cores['Verde']
                    else:
                        color = ''
                except Exception:
                    color = paleta_cores['Cinza']
                return 'background-color: {}'.format(color) 
            base = data_frame[data_frame.Unidade==unidade_alvo]
            base = base[['Comboio','Unidade','Status']]
            base = base.style.applymap(format_identificador_abertura)
            base.hide(axis="index")
            if not os.path.exists(os.getcwd()+'\\Abertura_Turno_Tablet'):
                os.mkdir('Abertura_Turno_Tablet')
            dfi.export(base,'Abertura_Turno_Tablet\\abertura_tablet_'+str(unidade_alvo)+'.png')
            return os.path.abspath('Abertura_Turno_Tablet\\abertura_tablet_'+str(unidade_alvo)+'.png')
        def sub_formatar_base_tablet(caminho_downloads,tipo_arquivo):
            folder_path = caminho_downloads
            file_type =  tipo_arquivo
            files = glob.glob(folder_path+file_type)
            arquivo_mais_recente = max(files, key=os.path.getctime)
            return arquivo_mais_recente
        def roteiros_gerados():
            ac = pd.read_excel(verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\MinhaTI\CIA Analytics - Comboio\Acompanhamento_Smart_route.xlsx'))
            ac = ac[['plannedFuelTruck','programmingUnit']]
            ac = ac.drop_duplicates()
            ac = ac.dropna()
            ac = ac.reset_index(drop=True)
            ac.plannedFuelTruck = ac.plannedFuelTruck.astype(int)
            ac.rename(columns={'plannedFuelTruck':'Comboio','programmingUnit':'Unidade'}, inplace=True)
            return ac

        rotas_realizadas = roteiros_gerados()

        caminho_tablet = r'\\csclsfsr03\SoftsPRD\Extrator\PRD\Sistema Apontamentos\Turno x Meterial'
        df = pd.read_csv(sub_formatar_base_tablet(caminho_tablet,'\*csv'), sep=';')
        df.INICIO_TURNO = pd.to_datetime(df.INICIO_TURNO, format="%Y-%m-%d %H:%M:%S")

        celula = []
        for id,row in rotas_realizadas.iterrows():
            if len(df.loc[(df.cd_equipamento==row.Comboio)&(df.INICIO_TURNO > datetime.now()-timedelta(hours=6))]) > 0:
                celula.append('ABERTO')
            else: celula.append('NÃO ABERTO')

        #rotas_realizadas = pd.concat([rotas_realizadas,pd.DataFrame(celula, columns=['Status'])], axis=1)
        rotas_realizadas['Status'] = celula
        envio_para = {} # 'UND':'C:Caminho/Para/Arquivo'
        for und in rotas_realizadas.Unidade.unique():
            try: 
                caminho = gerar_imagens_status_tablet(und,rotas_realizadas)
                envio_para[und] = caminho
            except IndexError: pass
        return envio_para
    def momento_atual():
        escrita = str(datetime.now())[:10]
        return f'{escrita.split("-")[2]}/{escrita.split("-")[1]}/{escrita.split("-")[0]}'
    print(f'\n--> {str(datetime.now())[:16]} | Condições para lógica "aberturas turno tablet!" iniciada')
    turno_msg = ['TURNO C' if datetime.now().hour < 7 else 'TURNO A' if datetime.now().hour < 15 else 'TURNO B' if datetime.now().hour < 23 else 'TURNO C']
    envio = verificar_abertura_tablets()
    contatos_envio_tablet = pd.read_excel(r'\\CSCLSFSR01\Agricola$\Logistica Agroindustrial\CIA 22.23\11. Analytics\BOT CIA\Contatos\Envio_tablets.xlsx')
    contatos_envio_tablet = contatos_envio_tablet.dropna()

    for und,caminho in zip(envio.keys(),envio.values()):
        if 'BONF' in und:
            mensagem_a_ser_enviada = f"⚠️ *ABERTURA TURNO TABLET: {und}*\nDATA {momento_atual()} do {turno_msg[0]}"
            anexo_a_ser_enviado = caminho
            contato, tipo_contato = verificar_tipo_de_contato(str(contatos_envio_tablet[contatos_envio_tablet.Unidade==und].values[0][1]))
            gravar_em_banco_para_envio([('COMBOIO_Abertura_Tablet',datetime.now(),contato, tipo_contato, mensagem_a_ser_enviada, anexo_a_ser_enviado)])
        else:
            print(f'**** Unidade: "{und}" não teve comboio cadastrado')
    print(f'\n--> {str(datetime.now())[:16]} | Ciclo concluído com sucesso!\n')

#################### DDS Programado

def atualizar_envio_DDS_ID():
    # Z:\Performance\01. CIA Pessoas\1.13. SSMA
    caminho = r'\\CSCLSFSR01\Agricola$\Logistica Agroindustrial\CIA 22.23\11. Analytics\BOT CIA\Contatos\Envio_DDS_grupos.xlsx'
    env_dds_id = pd.read_excel(caminho)
    env_dds_id.dropna(inplace=True)
    return [f[0] for f in list(zip(env_dds_id.ID_Grupo_DDS))]

def atualizar_DDS():
    dds = pd.read_excel(r'\\CSCLSFSR01\Agricola$\Logistica Agroindustrial\CIA 22.23\11. Analytics\BOT CIA\DDS_envio_bot.xlsx')
    dds = dds[['Data','Tema','Texto']]
    return dds

def enviar_dds():
    df_dds_env = atualizar_envio_DDS_ID()
    hoje = datetime.now()-timedelta(hours=(datetime.now().hour),minutes=datetime.now().minute,seconds=datetime.now().second,microseconds=datetime.now().microsecond)
    dds = atualizar_DDS()
    tema_dds = dds[dds.Data==hoje].Tema.values[0]
    texto_dds = dds[dds.Data==hoje].Texto.values[0]
    mensagem_dds = f'🎋 *DDS {hoje.day}/{hoje.month} - {tema_dds}* 🏭\n\n{texto_dds}'
    for contato_para_envio in df_dds_env:
        contato, tipo_contato = verificar_tipo_de_contato(contato_para_envio)
        gravar_em_banco_para_envio([('CIA_DDS_Programado',datetime.now(),contato, tipo_contato, mensagem_dds, '')])

#################### DDS Demanda

def enviar_dds_personalizado():
    df_dds_env = atualizar_envio_DDS_ID()
    # Setando data para a atual
    hoje_ref = datetime.now()-timedelta(hours=datetime.now().hour,minutes=datetime.now().minute,seconds=datetime.now().second,microseconds=datetime.now().microsecond)
    # Carregando arquivo de programação do DDS.
    prog_dds = pd.read_excel(r'\\CSCLSFSR01\Agricola$\Logistica Agroindustrial\CIA 22.23\11. Analytics\BOT CIA\Envio_DDS\Programacao_Envio.xlsx')
    prog_dds.dropna(subset=['Data_Envio','Nome_Anexo'], inplace=True) # Somente coluna Mensagem_Anexo poderá conter valores em branco "NaN"
    turno_atual = 'TURNO C' if datetime.now().hour < 7 else 'TURNO A' if datetime.now().hour < 15 else 'TURNO B' if datetime.now().hour < 23 else 'TURNO C'
    prog_dds = prog_dds[prog_dds[str(turno_atual)] == 'X']
    # Filtrando dia atual
    prog_dds = prog_dds[(prog_dds.Data_Envio == hoje_ref)]
    if len(prog_dds) > 0:
        print('-> Temos DDS Demanda hoje!')
        mensagem_anexo = prog_dds.Mensagem_Anexo.values[0]
        path_f = '\\\CSCLSFSR01\\Agricola$\\Logistica Agroindustrial\\CIA 22.23\\11. Analytics\\BOT CIA\\Envio_DDS\\Anexos\\'
        for root, dirs, files in os.walk(path_f):
            for file in files:
                if str(prog_dds.Nome_Anexo.values[0]) in str(file):
                    caminho_arq_dds = os.path.abspath(path_f+str(file))
        for contatos_para_envio in df_dds_env:
            contato, tipo_contato = verificar_tipo_de_contato(contatos_para_envio)
            try: gravar_em_banco_para_envio([('CIA_DDS_Demanda',datetime.now(),contato, tipo_contato, mensagem_anexo, caminho_arq_dds)])   
            except IndexError as error_DDS: print(f'DDS Demanda para {contato} com erro {error_DDS}')
    else: print('-> Sem programação DDS Demanda para hoje')

############# Deslocamento

def atualizar_contatos_prod_deslocamento():
    # 'PAR-VN-123'[:-3] = 'PAR-VN-'
    acpd = pd.read_excel(r'\\CSCLSFSR01\Agricola$\Logistica Agroindustrial\CIA 22.23\11. Analytics\BOT CIA\Contatos\Segunda_Funcao_Prod.xlsx')
    acpd = acpd[(acpd.CONTROLE_Deslocamento == 'SIM') & (acpd.Area == 'PROD')]
    acpd['Frente'] = acpd.Sigla_Unidade.astype(str) + '-' + acpd.Sigla_Frente.astype(str) + '-'
    acpd = acpd[['Frente','Torre_Numero']]
    acpd.dropna(axis=0, inplace=True)
    acpd = dict(list(zip(acpd.Frente,acpd.Torre_Numero)))
    return acpd

def calc_lista_lat_lon(list_lat,list_lon,list_momento):
    from math import sin, cos, sqrt, atan2, radians
    pontos = len(list_lat)-1
    dist_pontos = []
    list_latt = list_lat if isinstance(list_lat[0], float) else [np.float64(f.replace(',', '.')) for f in list_lat]
    list_long = list_lon if isinstance(list_lon[0], float) else [np.float64(f.replace(',', '.')) for f in list_lon]
    for n in range(pontos):
        lat1 = radians(list_latt[n])
        lon1 = radians(list_long[n])
        lat2 = radians(list_latt[n+1])
        lon2 = radians(list_long[n+1])
        dlon = lon2 - lon1
        dlat = lat2 - lat1
        a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
        c = 2 * atan2(sqrt(a), sqrt(1-a))
        dist_pontos.append(6370 * c * 1000)
    duracao = list_momento[0] - list_momento[-1]
    return round(sum(dist_pontos)), duracao.astype('timedelta64[s]')

def converta_data(d):
    value = datetime.strptime(np.datetime_as_string(d,unit='s'), '%Y-%m-%dT%H:%M:%S')
    return f'{value.day}/{value.month}/{str(value.year)[-2:]} {value.hour}:{value.minute if(value.minute>9) else "0"+str(value.minute)}'

def gerar_mapa_deslocamento(lista_val_LON, lista_val_LAT, frota, contato_envio, frase_input=0, data__=0, frente='RAZ-EX-000'):
    if not os.path.exists(os.path.join(os.getcwd(),'GAT_DESLOC')):
        os.mkdir('GAT_DESLOC')

    contatos_envio_ = str(contato_envio).split(',')
    
    for contato in contatos_envio_:
        # prepare the figure
        list_latt = [float(f.replace(',', '.')) for f in lista_val_LAT] if not isinstance(lista_val_LAT[0], float) else lista_val_LAT
        list_long = [float(f.replace(',', '.')) for f in lista_val_LON] if not isinstance(lista_val_LON[0], float) else lista_val_LON
        # Criação de um GeoDataFrame a partir das coordenadas
        gdf = gpd.GeoDataFrame({'lat': list_latt, 'lon': list_long})
        gdf['geometry'] = gdf.apply(lambda row: Point(row['lon'], row['lat']), axis=1)
        gdf = gdf.set_crs(epsg=4326)
        gdf = gdf.to_crs(epsg=3857)  # Converte para o sistema de coordenadas esperado pelo contextily
        # Obter os limites do GeoDataFrame
        bounds = gdf.total_bounds
        lon_min, lat_min, lon_max, lat_max = bounds
        # Calcular a largura e a altura
        width = lon_max - lon_min
        height = lat_max - lat_min
        # Ajustar os limites para tornar o mapa quadrado
        if width > height:
            lat_max = lat_min + width
        else:
            lon_max = lon_min + height
        # Plotar o mapa
        f, ax2 = plt.subplots(figsize=(10, 10))
        gdf.plot(ax=ax2, color='yellow', edgecolor='yellow', markersize=60)
        ax2.plot(list_long, list_latt, color='yellow', linewidth=2)
        ax2.set_xlim(lon_min, lon_max)
        ax2.set_ylim(lat_min, lat_max)
        
        # Zoom Level, quando maior mais detalhes
        max_lat_lon = lon_max + width
        if max_lat_lon < 1000:
            zoom_map = 14
        elif max_lat_lon < 3000:
            zoom_map = 12
        elif max_lat_lon < 5000:
            zoom_map = 10
        elif max_lat_lon < 8000:
            zoom_map = 8
        else:
            zoom_map = 6
        ctx.add_basemap(ax2, source=ctx.providers.Esri.WorldImagery, zoom=zoom_map)
        ax2.set_title(f'{frente} - TT {frota}')
        ax2.tick_params(axis='x', colors='white')
        ax2.tick_params(axis='y', colors='white')
        if len(str(frase_input)) > 3:
            f.text(0, -0.055, frase_input,
                verticalalignment='bottom', horizontalalignment='left',
                transform=ax2.transAxes,
                color='black', fontsize=12)
        if len(str(data__)) > 3:
            f.text(0, -0.11, f'Última comunicação: {converta_data(data__)}',
                verticalalignment='bottom', horizontalalignment='left',
                transform=ax2.transAxes,
                color='black', fontsize=12)
        try:
            file_path = os.path.join('GAT_DESLOC', f'DESLOCAMENTO_{frota}_{frente}.png')
            plt.savefig(file_path)
            plt.close(f)
            print(f'{file_path} - ZOOM UTILIZADO FOI: {zoom_map}')
            sleep(0.1)
            contato, tipo_contato = verificar_tipo_de_contato(contato)
            texto_auxiliar = f'⚠️ *ALERTA DESLOCAMENTO* ⚠️\n{frente} | TT {frota}\n{frase_input}\nÚltima comunicação: {converta_data(data__)}'
            gravar_em_banco_para_envio([('PROD_Deslocamento', datetime.now(), contato, tipo_contato, texto_auxiliar, '')])
            gravar_em_banco_para_envio([('PROD_Deslocamento', datetime.now(),'11963208908', 'contato', texto_auxiliar, '')])
            # Duplicar para controle produção
            contato, tipo_contato = verificar_tipo_de_contato('CIA Produção ID999')  # Roberta Santiago
            gravar_em_banco_para_envio([('DEBUG_PROD_Deslocamento', datetime.now(), contato, tipo_contato, texto_auxiliar, '')])
            print(f'Deslocamento: {frente} - TT {frota} -> COM efeito.')
        except IndexError as error_:
            plt.close(f)
            print(f'-> Erro Desloc: \n{error_}\n')

def gerar_imagens_deslocamento():
    banco_dados = caminho_base_deslocamento
    df_bd = pd.read_parquet(banco_dados, engine='pyarrow')

    df_bd = df_bd.iloc[:, [0,1,8,12,13,15,21,5]]
    df_bd.columns = ['Equipamento','Data/Hora','Grupo','Latitude','Longitude','Operacao','Tipo de Equipamento','Estado']


    df_bd['Data/Hora'] = pd.to_datetime(df_bd['Data/Hora'], dayfirst=True, errors='coerce')
    df_bd = df_bd[~(df_bd["Grupo"].str.contains("-MO-"))]
    df_bd = df_bd[df_bd["Tipo de Equipamento"]!="Caminhão de Vinhaça"]
    df_bd = df_bd[df_bd["Tipo de Equipamento"].isin(["PLANTADORA MAG100","COLHEDORA"])]
    df_bd = df_bd.sort_values(by=['Data/Hora'], ascending=False)
    pd.set_option('mode.chained_assignment', None)
    dict_contatos = atualizar_contatos_prod_deslocamento()
    lista_env_desloc = []
    for frota in df_bd.Equipamento.unique():
        #print('Verificando a frota: ',frota)
        # Abaixo loop para cada frota, iremos fazer filtro de 15  minutos
        slice = df_bd[(df_bd.Equipamento == frota) & (df_bd['Data/Hora'] >= (df_bd[(df_bd.Equipamento == frota)]['Data/Hora'].values[0])-np.timedelta64(60,'m'))]
        #print(slice.shape[0])
        if len(slice) > 1:
            num_desloc = 1
            idx_past = False
            for idx, row in slice.iterrows():
                if row.Estado == 'DESLOCAMENTO':
                    slice.loc[idx, 'num_desloc'] = num_desloc
                elif row.Estado != 'DESLOCAMENTO' and idx_past != False and slice.loc[idx_past].Estado == 'DESLOCAMENTO':
                    num_desloc += 1
                    slice.loc[idx, 'num_desloc'] = 0
                else:
                    slice.loc[idx, 'num_desloc'] = 0
                idx_past = idx
            for deslocamento in slice[slice.num_desloc > 0].num_desloc.unique():
                # Abaixo verificando se foi continuo 22 deslocamento (E se passou de 500)
                if len(slice[slice.num_desloc == deslocamento]) > 1:
                    #display(slice[slice.num_desloc == deslocamento])
                    if '-VN-' in slice[slice.num_desloc == deslocamento]['Grupo'].values[0]:
                        maximos_metros = 800
                    elif '-PR-' in slice[slice.num_desloc == deslocamento]['Grupo'].values[0]:
                        maximos_metros = 1000
                    else: maximos_metros = 500
                    metros, duracao = calc_lista_lat_lon(slice[slice.num_desloc == deslocamento].Latitude.values,slice[slice.num_desloc == deslocamento].Longitude.values,slice[slice.num_desloc == deslocamento]['Data/Hora'].values)
                    #print(f'Metros {metros} | Duração {duracao}')
                    if metros > maximos_metros and 'Deslocamento mudanca area' not in list(slice[slice.num_desloc == deslocamento].Operacao) and 'Deslocamento Chuva' not in list(slice[slice.num_desloc == deslocamento].Operacao):
                        frase_mapa = f'Movimentou {str(metros)+" metros" if(metros<1000) else str(round(metros/1000,1))+" km"} em {str(duracao.astype(int))+" segundos" if(duracao.astype(int)<60) else str(round(duracao.astype(int)/60))+" minutos"}. ({round((metros/duracao.astype(int))*3.6)} km/h)'
                        frente = slice["Grupo"].values[0]
                        frente_envio = dict_contatos[frente[:7]] if frente[:7] in dict_contatos.keys() else False
                        #print(frente_envio, type(frente_envio))
                        if frente_envio != False:
                            lista_env_desloc.append(gerar_mapa_deslocamento(slice[slice.num_desloc == deslocamento].Longitude.values,
                                                                            slice[slice.num_desloc == deslocamento].Latitude.values, 
                                                                            frota, 
                                                                            frente_envio,
                                                                            frase_mapa,
                                                                            slice[slice.num_desloc == deslocamento]['Data/Hora'].values[0],
                                                                            frente))
                        break
    pd.reset_option("mode.chained_assignment")
    #return lista_env_desloc

######## Apontamentos TO

def TO_Apontamentos():
    def bases_funcao_TO():
        if not os.path.exists('TO_Monitoramento_Apontamentos'):
            os.makedirs('TO_Monitoramento_Apontamentos')
        if not os.path.exists('TO_Monitoramento_Apontamentos/frotas_manutencao_TO.json'):
            with open('TO_Monitoramento_Apontamentos/frotas_manutencao_TO.json', 'w') as file:
                json.dump([''], file)
        with open('TO_Monitoramento_Apontamentos/frotas_manutencao_TO.json', 'r') as file:
            ls_chave_to = json.load(file)
        # Base frente para Grupos TO
        to_grupo = pd.read_excel(r'\\CSCLSFSR01\Agricola$\Logistica Agroindustrial\CIA 22.23\11. Analytics\BOT CIA\Contatos\TO_Monitoramento_Apontamentos.xlsx', sheet_name='Grupos_TO')
        to_grupo = to_grupo[['UND_COD', 'NOME']]
        to_grupo = dict(zip(to_grupo['UND_COD'], to_grupo['NOME']))
        duplicatas = [k.split(';') for k in to_grupo.keys() if ';' in k]
        for pares in duplicatas:
            for und in pares:
                to_grupo[und] = to_grupo[';'.join(pares)]
        # Base de Apontamentos monitorados TO
        to_apts = pd.read_excel(r'\\CSCLSFSR01\Agricola$\Logistica Agroindustrial\CIA 22.23\11. Analytics\BOT CIA\Contatos\TO_Monitoramento_Apontamentos.xlsx', sheet_name='Apontamentos')
        
        # Base Apontamentos Agron
        com_agron = pd.read_excel(verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\CIA Analytics - BOT CIA\Extrator\AGRON\agron_comunicacao.xlsx'))
        com_agron['Duracao'] = pd.to_timedelta(com_agron['Tempo em atividade'], unit='s')
        com_agron = com_agron[com_agron['Frente associada'] != 'NO_WORKFRONT']
        com_agron['Frente_UND'] = [x.split('-')[0] for x in com_agron['Frente associada']]
        com_agron = com_agron[(com_agron['Atividade'].isin(to_apts['Apontamento'])) & (com_agron['Duracao'] > timedelta(seconds=30))]
        com_agron['chave'] = com_agron['Número do Equipamento'].astype(str) + '$' + com_agron['Atividade']
        com_agron_envio = com_agron[~com_agron['chave'].isin(ls_chave_to)]
        with open('TO_Monitoramento_Apontamentos/frotas_manutencao_TO.json', 'w') as file:
            json.dump(list(com_agron['chave']), file)
        return to_grupo, to_apts, com_agron_envio

    def criar_mensagem_TO(frota, tipo_frota, frente, atividade, momento, duracao):
        dur_f = str(duracao).split('.')[0].replace('days','dias')
        duf_f1 = dur_f.replace('0 dias ','') if '0 dias ' in dur_f else dur_f
        msg = []
        msg.append('*❌📡 Apontamento Manutenção TO!*')
        msg.append(f'*Frota:* {frota}')
        msg.append(f'*Frente:* {frente}')
        msg.append(f'*Tipo:* {tipo_frota}')
        msg.append(f'*Apontamento:* {atividade}')
        try:
            msg.append(f'⏱️ *Comunicação:* {momento.strftime("%d/%m/%Y %H:%M:%S")}')
        except:
            msg.append(f'⏱️ *Comunicação:* *{momento}*')
        msg.append(f'⏱️ *Duração no apontamento:* {duf_f1}')
        return '\n'.join(msg)

    to_grupo, to_apts, com_agron = bases_funcao_TO()

    for i, linha in com_agron.iterrows():
        frota, tipo_frota, frente, atividade, comunicacao, duracao = [linha['Número do Equipamento'], linha['Tipo do equipamento'], linha['Frente associada'], linha['Atividade'], linha['Registro mais recente'], linha['Duracao']]
        destino = to_grupo[frente.split('-')[0]]
        mensagem_to = criar_mensagem_TO(frota, tipo_frota, frente, atividade, comunicacao, duracao)
        contato, tipo_contato = verificar_tipo_de_contato(destino)
        gravar_em_banco_para_envio([('TO_Apontamentos', datetime.now(), contato, tipo_contato, mensagem_to, '')])
        if 'JAT-' in frente:
            contato, tipo_contato = verificar_tipo_de_contato('Apontamentos de manutenção ')
            gravar_em_banco_para_envio([('TO_Apontamentos', datetime.now(), contato, tipo_contato, mensagem_to, '')])

##### Apontamção Manutenção SPGA3
            
def apontamento_manutencao_SPGA3():
    dfn = carregar_df_monitoramento_SGPA3()
    dfn["Número do Equipamento"] = dfn["Número do Equipamento"].astype(str)
    #dfn = dfn.drop(columns="_id")

    contatos_manutencao = pd.read_excel(r"\\CSCLSFSR01\Agricola$\Logistica Agroindustrial\CIA 22.23\11. Analytics\BOT CIA\Contatos\Contatos_BOT_CIA_Manutencao.xlsx",sheet_name='Contatos')
    contatos_manutencao = contatos_manutencao[["Frente", "T_Manutencao"]]
    contatos_manutencao = contatos_manutencao.set_index('Frente').to_dict()['T_Manutencao']

    to_grupo = pd.read_excel(r'\\CSCLSFSR01\Agricola$\Logistica Agroindustrial\CIA 22.23\11. Analytics\BOT CIA\Contatos\TO_Monitoramento_Apontamentos.xlsx', sheet_name='Grupos_TO')
    to_grupo = to_grupo[['UND_COD', 'NOME']]
    to_grupo = dict(zip(to_grupo['UND_COD'], to_grupo['NOME']))
    duplicatas = [k.split(';') for k in to_grupo.keys() if ';' in k]
    for pares in duplicatas:
        for und in pares:
            to_grupo[und] = to_grupo[';'.join(pares)]
    # Base de Apontamentos monitorados TO
    to_apts = pd.read_excel(r'\\CSCLSFSR01\Agricola$\Logistica Agroindustrial\CIA 22.23\11. Analytics\BOT CIA\Contatos\TO_Monitoramento_Apontamentos.xlsx', sheet_name='Apontamentos')
    to_apts = list(to_apts.Apontamento)

    def gerar_manutencao_corr(row):
        mensagem_manut = f"""❌⚠️ *Apontamento Manutenção!*
    *Frente:* {row['Frente associada']}
    *Frota:* {row['Número do Equipamento']}
    *Tipo:* {row['Tipo do equipamento']}
    *Apontamento:* {row['Atividade']}
    ⏱️ _Ultima comunicação: {row['Registro mais recente']}_"""
        return mensagem_manut

    def gerar_mensagem_TO(row):
        mensagem_manut = f'''*❌📡 Apontamento Manutenção TO!*
        *Frota:* {row['Número do Equipamento']} | {row['Tipo do equipamento'].split(' ')[0]}
        *Frente:* {row["Frente associada"]}
        *Apontamento:* {row["Atividade"]}
        ⏱️ *Comunicação:* {row["Registro mais recente"]}'''
        return mensagem_manut

    def gerar_manutencao_prev(row):
        mensagem_manut = f"""⚠️ *Apontamento Manutenção!*
    *Frente:* {row['Frente associada']}
    *Frota:* {row['Número do Equipamento']} | {row['Tipo do equipamento'].split(' ')[0]}
    *Apontamento:* {row['Atividade']}
    ⏱️ _Ultima comunicação: {row['Registro mais recente']}_"""
        return mensagem_manut
        
    if not os.path.exists('Manutencao_Apontamentos'):
        os.mkdir('Manutencao_Apontamentos')
    if not os.path.exists('Manutencao_Apontamentos/Manutencao_Apontamentos_Corretivas.json'):
        with open('Manutencao_Apontamentos/Manutencao_Apontamentos_Corretivas.json', 'w') as file:
            json.dump(['Lista_criada_agora'], file)
    if not os.path.exists('Manutencao_Apontamentos/Manutencao_Apontamentos_Preventivas.json'):
        with open('Manutencao_Apontamentos/Manutencao_Apontamentos_Preventivas.json', 'w') as file:
            json.dump(['Lista_criada_agora'], file)

    # Lógica Preventivas
    with open('Manutencao_Apontamentos/Manutencao_Apontamentos_Preventivas.json', 'r') as file:
        frotas_manut_prev = json.loads(file.read())
        #print('gravando dados prev: \n',frotas_manut_prev,'\n')
    frotas_desconsierar = [frota for frota in frotas_manut_prev if frota not in list(dfn["Número do Equipamento"].unique())]
    df_prev = dfn[dfn.Atividade.isin(apontamentos_manutencao_preventiva)]
    frotas_prev = list(df_prev['Número do Equipamento'].unique())
    df_prev['Tempo em atividade'] = df_prev['Tempo em atividade'].apply(calcular_tempo)
    memoria_prev = frotas_prev+frotas_desconsierar
    for id,row in df_prev[~df_prev["Número do Equipamento"].isin(frotas_manut_prev)
                        & (df_prev["Frente associada"].isin(contatos_manutencao.keys()))].iterrows():
        mensagem_envio = gerar_manutencao_prev(row)
        contato_envio = contatos_manutencao[row["Frente associada"]]
        contato, tipo_contato = verificar_tipo_de_contato(contato_envio)
        gravar_em_banco_para_envio([('MANUT_Apto_Manutencao_SGPA3_Main',datetime.now(),contato, tipo_contato,mensagem_envio,'')])
        # Duplicata para time Jatai
        if 'JAT-' in row["Frente associada"]:
            contato, tipo_contato = verificar_tipo_de_contato('Apontamentos de manutenção ')
            gravar_em_banco_para_envio([('MANUT_Apto_Manutencao_SGPA3_JATAI',datetime.now(),contato, tipo_contato,mensagem_envio,'')])
        # Direcionando para TO
        if row['Atividade'] in to_apts:
            sigla_und = row["Frente associada"][:3]
            grupo_envio_TO = to_grupo[sigla_und]
            mensagem_TO = gerar_mensagem_TO(row)
            contato, tipo_contato = verificar_tipo_de_contato(grupo_envio_TO)
            gravar_em_banco_para_envio([('MANUT_Apto_Manutencao_SGPA3_TO',datetime.now(),contato, tipo_contato,mensagem_TO,'')])
    with open('Manutencao_Apontamentos/Manutencao_Apontamentos_Preventivas.json', 'w') as file:
        #print('gravando dados prev: \n',memoria_prev,'\n')
        memoria_prev = [str(x) for x in memoria_prev]
        json.dump(memoria_prev, file)

    # Lógica Corretivas
    with open('Manutencao_Apontamentos/Manutencao_Apontamentos_Corretivas.json', 'r') as file:
        frotas_manut_corr = json.loads(file.read())
    frotas_desconsierar = [frota for frota in frotas_manut_corr if frota not in list(dfn["Número do Equipamento"].unique())]
    df_corr = dfn[dfn.Atividade.isin(apontamentos_manutencao_corretiva)]
    frotas_corr = list(df_corr['Número do Equipamento'].unique())
    df_corr['Tempo em atividade'] = df_corr['Tempo em atividade'].apply(calcular_tempo)
    memoria_prev = frotas_corr+frotas_desconsierar
    for id,row in df_corr[~df_corr["Número do Equipamento"].isin(frotas_manut_corr)
                        & (df_corr["Frente associada"].isin(contatos_manutencao.keys()))].iterrows():
        mensagem_envio = gerar_manutencao_corr(row)
        contato_envio = contatos_manutencao[row["Frente associada"]]
        contato, tipo_contato = verificar_tipo_de_contato(contato_envio)
        gravar_em_banco_para_envio([('MANUT_Apto_Manutencao_SGPA3_Main',datetime.now(),contato, tipo_contato,mensagem_envio,'')])
        # Duplicata para time Jatai
        if 'JAT-' in row["Frente associada"]:
            contato, tipo_contato = verificar_tipo_de_contato('Apontamentos de manutenção ')
            gravar_em_banco_para_envio([('MANUT_Apto_Manutencao_SGPA3_JATAI',datetime.now(),contato, tipo_contato,mensagem_envio,'')])
            pass
        # Direcionando para TO
        if row['Atividade'] in to_apts:
            sigla_und = row["Frente associada"][:3]
            grupo_envio_TO = to_grupo[sigla_und]
            mensagem_TO = gerar_mensagem_TO(row)
            contato, tipo_contato = verificar_tipo_de_contato(grupo_envio_TO)
            gravar_em_banco_para_envio([('MANUT_Apto_Manutencao_SGPA3_TO',datetime.now(),contato, tipo_contato,mensagem_TO,'')])
    with open('Manutencao_Apontamentos/Manutencao_Apontamentos_Corretivas.json', 'w') as file:
        memoria_prev = [str(x) for x in memoria_prev]
        json.dump(memoria_prev, file)

########### Report Colheista

def ciclo_report_moagem():

    def sub_formatar_base(caminho_downloads,tipo_arquivo):
        global arquivo_mais_recente
        folder_path = caminho_downloads
        file_type =  tipo_arquivo
        files = glob.glob(folder_path + file_type)
        arquivo_mais_recente = max(files, key=os.path.getctime)

    def atualizar_ref_2CCT():
        global dict_ref_und_cct, inv_dict_ref_und_cct, lista_caminhoes, lista_num_frentes_proprias
        try:
            caminho_arquiv_suport_cct_2 = r'\\CSCLSFSR01\Agricola$\Logistica Agroindustrial\CIA 22.23\11. Analytics\BOT CIA\Contatos\REF segunda func CCT.xlsx'
            # Lista Unidades
            df_referencia_cct = pd.read_excel(caminho_arquiv_suport_cct_2, sheet_name='unidade')
            dict_ref_und_cct = dict(zip(df_referencia_cct.INSTANCIA, df_referencia_cct.REF1))
            inv_dict_ref_und_cct = dict(zip(df_referencia_cct.REF1, df_referencia_cct.INSTANCIA))
            for key in list(inv_dict_ref_und_cct.keys()): inv_dict_ref_und_cct[key.upper()] = inv_dict_ref_und_cct[key]
            # Lista Canavieiros
            lista_caminhoes = pd.read_excel(caminho_arquiv_suport_cct_2, sheet_name='canavieiro')
            lista_caminhoes = list(lista_caminhoes.CANAVIEIROS)
            # Lista Mix
            df_mix_frentes = pd.read_excel(caminho_arquiv_suport_cct_2, sheet_name='mix')
            df_mix_frentes = df_mix_frentes[['Frente','Instancia','Tipo Frente','Frente SGPA']]
            df_mix_frentes['Frente SGPA'] = df_mix_frentes['Frente SGPA'].str[4:]
            df_mix_frentes = df_mix_frentes[df_mix_frentes['Tipo Frente'] == 'Próprio']
            lista_num_frentes_proprias = list(df_mix_frentes['Frente'])
            #print(lista_num_frentes_proprias)
        except IndentationError:
            print('\n=======================\nNão foi possível atualizar referências!!!')
            pass

    while True:
        try:
            cd = pd.read_excel(verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\CIA Analytics - BOT CIA\Extrator\CCT\CD_Hora.xlsx'))
            break
        except:
            print('Erro em carregar base CD Bordo.')
            sleep(3)

    cd = cd[cd.DT_LOCAL>datetime.now()-timedelta(hours=datetime.now().hour+1)]

    def calculo_ns_caminhao(row):
        numerador = row[row.CD_OPERACAO==237].VL_HR_OPERACIONAIS.sum()
        denominador = row[row.CD_OPERACAO.isin([237,117,832])].VL_HR_OPERACIONAIS.sum()
        return round((1-(numerador/denominador))*100,1)

    ns_caminhao = cd.groupby(['DESC_UNIDADE']).apply(calculo_ns_caminhao)
    ns_caminhao['VALER'] = ns_caminhao['VALE DO ROSARIO'] if "VALE DO ROSARIO" in ns_caminhao.keys() else '*'
    ns_caminhao['SCAND'] = ns_caminhao['SANTA CÂNDIDA'] if "SANTA CÂNDIDA" in ns_caminhao.keys() else '*'
    ns_caminhao['SELIS'] = ns_caminhao['SANTA ELISA'] if "SANTA ELISA" in ns_caminhao.keys() else '*'
    ns_caminhao['JUN'] = ns_caminhao['JUNQUEIRA'] if "JUNQUEIRA" in ns_caminhao.keys() else '*'
    ns_caminhao['MUND'] = ns_caminhao['MUNDIAL'] if "MUNDIAL" in ns_caminhao.keys() else '*'
    ns_caminhao['RBRIL'] = ns_caminhao['RIO BRILHANTE'] if "RIO BRILHANTE" in ns_caminhao.keys() else '*'
    ns_caminhao['LPRAT'] = ns_caminhao['LAGOA DA PRATA'] if "LAGOA DA PRATA" in ns_caminhao.keys() else '*'
    ns_caminhao['CONTI'] = ns_caminhao['CONTINENTAL'] if "CONTINENTAL" in ns_caminhao.keys() else '*'
    ns_caminhao['CAAR'] = ns_caminhao['CAARAPÓ'] if "CAARAPÓ" in ns_caminhao.keys() else '*'
    ns_caminhao['BONF'] = ns_caminhao['BONFIM'] if "BONFIM" in ns_caminhao.keys() else '*'
    ns_caminhao['MORRO'] = ns_caminhao['UMB'] if "UMB" in ns_caminhao.keys() else '*'
    ns_caminhao['UMB'] = ns_caminhao['MORRO'] if "UMB" in ns_caminhao.keys() else '*'
    ns_caminhao['BENA'] = ns_caminhao['BENALCOOL'] if "BENALCOOL" in ns_caminhao.keys() else '*'
    ns_caminhao['COPI'] = ns_caminhao['COSTA PINTO'] if "COSTA PINTO" in ns_caminhao.keys() else '*'
    ns_caminhao['DEST'] = ns_caminhao['DESTIVALE'] if "DESTIVALE" in ns_caminhao.keys() else '*'
    ns_caminhao['DIA'] = ns_caminhao['DIAMANTE'] if "DIAMANTE" in ns_caminhao.keys() else '*'
    ns_caminhao['PASSA'] = ns_caminhao['PASSATEMPO'] if "PASSATEMPO" in ns_caminhao.keys() else '*'
    ns_caminhao['RAF'] = ns_caminhao['RAFARD'] if "RAFARD" in ns_caminhao.keys() else '*'
    ns_caminhao['UNI'] = ns_caminhao['UNIVALEM'] if "UNIVALEM" in ns_caminhao.keys() else '*'
    ns_caminhao['JATAI'] = ns_caminhao['JATAÍ'] if "JATAÍ" in ns_caminhao.keys() else '*'

    def bases_segunda_func_cct():
        atualizar_ref_2CCT()
        global meta_moagem, med_entrega_3h, df_entrada_3h, df_ton_cana_dia, df_ton_cana_mix, ap_cam_t1, ap_cam_t2, ap_cam_t3, ap_cam_t4, df_entrada_saida, df_MG, df_ton_cana_ontem, df_apontamento_atual, df_patio_int, df_patio_ext, df_patio_ext_ff, df_ton_cana, hora_atual_ref, hora_atual_ref_MG, ton_moagem_hora_ref,data_hora_moagem_ref
        und_hora_dif = 'PASSA|RBRIL|CAAR'

        # META MOAGEM
        meta_moagem = pd.read_excel(verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\Metas_Gov_Indicadores - Documentos\CCT\Parâmetros Relatórios CCT (Metas).xlsx'), sheet_name='Metas')
        meta_moagem = dict(zip(list(meta_moagem['UNIDADE']),list(meta_moagem['Meta Moagem TCD'])))
        meta_moagem['MORRO'] = meta_moagem['UMB']
        meta_moagem['DDC'] = meta_moagem['DIA']
        meta_moagem['COP'] = meta_moagem['COPI']
        meta_moagem['PASSA'] = meta_moagem['PTP']
        meta_moagem['LEME'] = meta_moagem['LEM']
        meta_moagem['SELIS'] = meta_moagem['SEL']
        meta_moagem['VALER'] = meta_moagem['VRO']
        meta_moagem['LPRAT'] = meta_moagem['LPT']
        meta_moagem['CONTI'] = meta_moagem['CNT']
        meta_moagem['RBRIL'] = meta_moagem['RBR']

        # AGREGADOS:
        ap_cam_t1 = ['895 - Deslocamento Vazio']
        ap_cam_t2 = ['779 - Carregamento de cana p/ moagem', '779 - Carregamento', '891 - Troca Carretas - BV Campo', '886 - Aguardando carregamento', '886 - Aguardando Transbordo']
        ap_cam_t3 = ['881 - Deslocamento Carregado', '1068 - Enlonamento']
        ap_cam_t4 = ['833 - Deslocamento', '890 - Troca Carretas - BV Pát.Ext', '206 - Aguard. Seq. de Trabalho (Pátio Externo)', '212 - Falta de Motorista/Operad.', '213 - Patio / Reserva', '227 - Limitação Indústria',
                    '1400 - Manutenção Telemetria (Veltec)', '399 - Lubrificação', '844 - Manutenção TO', '1119 - Manut por oportunidade', '201 - Abastecimento', '882 - Pátio Externo - Carregado', '884 - Desloc. Após-Descar. Hilo',
                    '885 - Após balança de saída até CT', '888 - Balança - Saída', '889 - Balança - Entrada', '892 - Sonda', '893 - Fila Balança / Pátio interno', '894 - Manobra Carretas-Pátio', '896 - Descarregamento']
        
        # TON CANA:
        df_ton_cana = pd.read_excel(verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\CIA Analytics - BOT CIA\Extrator\CCT\Moagem\Ton_Cana.xlsx'))
        def somar_moagem(row):
            return row.QT_LIQUIDO.sum()/1000
        def densidade_carga_24h(row):
            return (row.QT_LIQUIDO.sum()/1000) / row.CONTAGEM_CARGAS.sum()
        dens_carga_24h = df_ton_cana[((df_ton_cana.instancia.str.contains(und_hora_dif)) 
                                & (df_ton_cana.data_hora > datetime.now()-timedelta(hours=25)))
                                | ((~df_ton_cana.instancia.str.contains(und_hora_dif)) 
                                & (df_ton_cana.data_hora > datetime.now()-timedelta(hours=24)))].groupby('instancia').apply(densidade_carga_24h)
        df_ton_cana_dia = df_ton_cana[((df_ton_cana.instancia.str.contains(und_hora_dif)) 
                                & (df_ton_cana.data_hora > datetime.now()-timedelta(hours=datetime.now().hour+1, minutes=datetime.now().minute+1)))
                                | ((~df_ton_cana.instancia.str.contains(und_hora_dif)) 
                                & (df_ton_cana.data_hora > datetime.now()-timedelta(hours=datetime.now().hour, minutes=datetime.now().minute)))].groupby('instancia').apply(somar_moagem)
        df_ton_cana_mix = df_ton_cana[df_ton_cana.data_hora > datetime.now()-timedelta(hours=datetime.now().hour, minutes=datetime.now().minute+.001)]
        df_ton_cana = df_ton_cana[((df_ton_cana.instancia.str.contains(und_hora_dif)) 
                                & (df_ton_cana.data_hora > datetime.now()-timedelta(hours=4, minutes=datetime.now().minute+.001)) & (df_ton_cana.data_hora < datetime.now()-timedelta(hours=1, minutes=datetime.now().minute+.0001)))
                                | ((~df_ton_cana.instancia.str.contains(und_hora_dif)) 
                                & (df_ton_cana.data_hora > datetime.now()-timedelta(hours=3, minutes=datetime.now().minute+.001)) & (df_ton_cana.data_hora < datetime.now()-timedelta(minutes=datetime.now().minute+.0001)))]

        # APONTAMENTOS: Traga as comunicações, filtes os canavieiros e filtre somente os mais recentes.
        sub_formatar_base(r'\\CSCLSFSR03\SoftsPRD\Extrator\PRD\CCT\Apontamentos Atual','\*csv')
        check_x = 0
        while check_x < 1:
            try:
                df_apontamento_atual = pd.read_csv(arquivo_mais_recente, encoding="ISO-8859-1", sep=';', on_bad_lines='skip')
                check_x = 1
            except IndexError:
                sleep(0.5)
                print(f'{datetime.now()} --> Erro em base APONTAMENTO ATUAL\n')
        
        df_apontamento_atual.DESC_UNIDADE = df_apontamento_atual.DESC_UNIDADE.replace(inv_dict_ref_und_cct)
        df_apontamento_atual = df_apontamento_atual[df_apontamento_atual['DESC_GRUPO_EQUIPAMENTO'].str.contains('-LN-|-BV-')]
        df_apontamento_atual.ULTIMA_COMUNICACAO = pd.to_datetime(df_apontamento_atual.ULTIMA_COMUNICACAO, dayfirst=True, errors='ignore')
        df_apontamento_atual = df_apontamento_atual[((df_apontamento_atual.DESC_UNIDADE.str.contains(und_hora_dif))
                                                    & (df_apontamento_atual.ULTIMA_COMUNICACAO > datetime.now()-timedelta(hours=2, minutes=datetime.now().minute+.001)))
                                                    |
                                                    ((~df_apontamento_atual.DESC_UNIDADE.str.contains(und_hora_dif))
                                                    & (df_apontamento_atual.ULTIMA_COMUNICACAO > datetime.now()-timedelta(hours=1, minutes=datetime.now().minute+.001)))]
    
        # ENTRADA E SAÍDA: Trazer somente os ultimos check-in e out's de cada unidades (Dia e momento), para assim somente as duas ultimas colunas.
        df_entrada_saida = pd.read_excel(verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\CIA Analytics - BOT CIA\Extrator\CCT\Check_In_Out.xlsx'))
        df_entrada_saida.DE_HORA = pd.to_timedelta(df_entrada_saida.DE_HORA)
        df_entrada_saida.UNIDADE = df_entrada_saida.UNIDADE.astype(str)
        df_entrada_saida['MOMENTO'] = df_entrada_saida.DE_DATA + df_entrada_saida.DE_HORA
        def med_entrega(row):
            try: densidade_caminhao = dens_carga_24h[row.UNIDADE.values[0]]*2
            except: densidade_caminhao = 30*2
            return (row.QT_CHECKIN.sum()/3)*densidade_caminhao
        med_entrega_3h = df_entrada_saida[((df_entrada_saida.UNIDADE.str.contains(und_hora_dif))
                                            & (df_entrada_saida.MOMENTO > datetime.now()-timedelta(hours=4, minutes=datetime.now().minute+.001))) 
                                            | (~df_entrada_saida.UNIDADE.str.contains(und_hora_dif)) 
                                            & (df_entrada_saida.MOMENTO > datetime.now()-timedelta(hours=3, minutes=datetime.now().minute+.001))].groupby('UNIDADE').apply(med_entrega)
        df_entrada_saida = df_entrada_saida[((df_entrada_saida.UNIDADE.str.contains(und_hora_dif))
                                            & (df_entrada_saida.MOMENTO > datetime.now()-timedelta(hours=2, minutes=datetime.now().minute+.001))) 
                                            | (~df_entrada_saida.UNIDADE.str.contains(und_hora_dif)) 
                                            & (df_entrada_saida.MOMENTO > datetime.now()-timedelta(hours=1, minutes=datetime.now().minute+.001))]
    
        # PÁTIO INTERNO:
        df_patio = pd.read_excel(verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\CIA Analytics - BOT CIA\Extrator\CCT\Cargas\Cargas_1.xlsx'))
        df_patio_int = df_patio[df_patio.Status == 'Interno']

        # PÁTIO EXTERNO:
        df_patio_ext = df_patio[df_patio.Status == 'Externo']

        # PÁTIO EXTERNO FF:
        df_patio_ext_ff = pd.read_excel(verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\CIA Analytics - BOT CIA\Extrator\CCT\Cargas\Cargas_2.xlsx'))
        hora_recente = datetime.now()-timedelta(minutes=datetime.now().minute+1)
        df_patio_ext_ff = df_patio_ext_ff[((df_patio_ext_ff.HR_BALANCA >= hora_recente) 
                            & (hora_recente >= df_patio_ext_ff.HR_CHECKIN) 
                            & (~df_patio_ext_ff.INSTANCIA.str.contains('CAAR|RBRIL|PASSA')))
                            & (df_patio_ext_ff.PROPRIEDADE_EQPTO == 'Foca') & (df_patio_ext_ff.TIPO_CANA == 'Mechanized')
                            | ((df_patio_ext_ff.HR_BALANCA >= hora_recente-timedelta(hours=1)) 
                            & (df_patio_ext_ff.HR_CHECKIN <= hora_recente-timedelta(hours=1)) 
                            & (df_patio_ext_ff.PROPRIEDADE_EQPTO == 'Foca') & (df_patio_ext_ff.TIPO_CANA == 'Mechanized')
                            & (df_patio_ext_ff.INSTANCIA.str.contains('CAAR|RBRIL|PASSA')))]


    def gerar_informativo_segunda_func_CCT(unidade): 
        mensagem_unidade = []
        unidades_do_fuso_diferente = ['CAAR','PASSA','RBRIL']
        if unidade in unidades_do_fuso_diferente: 
            moagem_soma_ultima_hora =  df_ton_cana[(df_ton_cana.instancia == unidade) & (df_ton_cana.data_hora > datetime.now()-timedelta(hours=2, minutes=datetime.now().minute+.001))]
            moagem_media_3horas =  df_ton_cana[(df_ton_cana.instancia == unidade) & (df_ton_cana.data_hora > datetime.now()-timedelta(hours=4, minutes=datetime.now().minute+.001))]
        else: 
            moagem_soma_ultima_hora =  df_ton_cana[(df_ton_cana.instancia == unidade) & (df_ton_cana.data_hora > datetime.now()-timedelta(hours=1, minutes=datetime.now().minute+.001))]
            moagem_media_3horas =  df_ton_cana[(df_ton_cana.instancia == unidade) & (df_ton_cana.data_hora > datetime.now()-timedelta(hours=3, minutes=datetime.now().minute+.001))]
        moagem_ultima_hora = moagem_soma_ultima_hora
        moagem_soma_ultima_hora = moagem_soma_ultima_hora.QT_LIQUIDO.sum()/1000
        moagem_media_3horas = moagem_media_3horas.QT_LIQUIDO.sum()/1000
        df_ton_cana_mix_prop = df_ton_cana_mix[df_ton_cana_mix.instancia == unidade]
        moagem_total_dia = df_ton_cana_mix_prop.QT_LIQUIDO.sum()
        df_ton_cana_mix_prop = df_ton_cana_mix_prop[df_ton_cana_mix_prop['CD_FREN_TRAN'].isin(lista_num_frentes_proprias)]
        moagem_prop_dia = (df_ton_cana_mix_prop.QT_LIQUIDO.sum()) / moagem_total_dia
        moagem_foca_dia = 1 - moagem_prop_dia
        if unidade in unidades_do_fuso_diferente: #Unidade MG
            hora_atual_ref0 = str(datetime.now()-timedelta(hours=1))[11:13]
        else:
            hora_atual_ref0 = str(datetime.now()-timedelta(hours=0))[11:13]
        escrita_momento_report = int(hora_atual_ref0[:2])-1

        if escrita_momento_report == -1:
            escrita_momento_report = 23
        
        # MOAGEM
        mensagem_unidade.append(f'🏭 *Report Aux. - {unidade} {int(hora_atual_ref0[:2])}h*')
        moagem_cm = df_ton_cana[df_ton_cana.instancia==unidade]
        moagem_do_dia = df_ton_cana_dia[unidade] if unidade in list(df_ton_cana_dia.index) else 0
        media_entrega_3h = round(med_entrega_3h[unidade],2) if unidade in list(med_entrega_3h.index) else 0
        media_moagem_3h = round(moagem_media_3horas/3)
        projecao = round(moagem_do_dia + (media_moagem_3h * (24 - datetime.now().hour)),2)
        icon_proj = '✅' if projecao>meta_moagem[unidade] else '🔻'
        #mensagem_unidade.append(f'PROJEÇÃO: {projecao} ton {icon_proj}')
        #mensagem_unidade.append(f'Moagem Atual [Dia]: {moagem_do_dia} ton')
        mensagem_unidade.append(f'Média Entrega [3h]: {media_entrega_3h} ton')
        try:
            media_carga = round(moagem_cm['QT_LIQUIDO'].mean()/1000)
        except:
            media_carga = 31
        #mensagem_unidade.append(f"Moagem {escrita_momento_report}h: {round(moagem_soma_ultima_hora)} ton")     
        #mensagem_unidade.append(f"Moagem med. 3h : {media_moagem_3h} ton")
        try:
            mensagem_unidade.append(f"MIX :  PP {round((moagem_prop_dia*100),2)}% /  FF {round((moagem_foca_dia*100),2)}% ")
        except:
            mensagem_unidade.append(f"MIX :  PP 0% /  FF 0% -")
        # CARGAS
        #mensagem_unidade.append('\n📌 *Cargas em Pátio*')
        cargas_PP = df_patio_ext[df_patio_ext.instancia==unidade].CARGAS.sum()
        cargas_FF = df_patio_ext_ff[df_patio_ext_ff.INSTANCIA==unidade].CARGAS.sum()
       # mensagem_unidade.append(f'Cargas Externo: PP: {cargas_PP} / FF: {cargas_FF}')
        cargas_int = df_patio_int[df_patio_int.instancia==unidade]
        cargas_int = cargas_int.CARGAS.sum()
        #mensagem_unidade.append(f'Cargas Interno: {cargas_int}\n')
        mensagem_unidade.append("\n🚚 *Caminhões*")
        ns_cm_und = ns_caminhao[unidade] if unidade in ns_caminhao.keys() else '#'
        mensagem_unidade.append(f'NS CM {ns_cm_und}%')

        entrada_saida = df_entrada_saida[df_entrada_saida['UNIDADE']==unidade]
        entrada_check = entrada_saida['QT_CHECKIN'].sum()
        saida_check = entrada_saida['QT_CHECKOUT'].sum()
        mensagem_unidade.append(f"Entrada: {entrada_check} / Saída: {saida_check}")
        #mensagem_unidade.append(f"Moagem CM {escrita_momento_report}h: {round(moagem_ultima_hora.CONTAGEM_CARGAS.sum()/2)}")
        if unidade in unidades_do_fuso_diferente: mensagem_unidade.append(f"\n_Dados até {(datetime.now()-timedelta(hours=2)).hour}h59._")
        else: mensagem_unidade.append(f"\n_Dados até {(datetime.now()-timedelta(hours=1)).hour}h59._")
        return '\n'.join(mensagem_unidade)


    def atualizar_contatos_envio_report_colheita():
        sucesso_cont = 0
        while sucesso_cont < 1:
            try:
                contatos_envio_rep_c = pd.read_excel(r'\\CSCLSFSR01\Agricola$\Logistica Agroindustrial\CIA 22.23\11. Analytics\BOT CIA\Contatos\REF segunda func CCT.xlsx', sheet_name='contatos_n')
                contatos_envio_rep_c = contatos_envio_rep_c[['INSTANCIA','CONTATO_n']]
                contatos_envio_rep_c.dropna(inplace=True)
                contatos_envio_rep_c = dict(zip(contatos_envio_rep_c.INSTANCIA,contatos_envio_rep_c.CONTATO_n))
                return contatos_envio_rep_c
            except:
                sleep(3)
                pass

    try:
        bases_segunda_func_cct()
        dict_contatos_rep_c = atualizar_contatos_envio_report_colheita()
        for unidade in [x for x in list(dict_contatos_rep_c.keys()) if 'cont' not in x.lower()]:
            contato_destino = dict_contatos_rep_c[unidade]
            if len(str(contato_destino).split('/')) > 1:
                for duplicata in str(contato_destino).split('/'):
                    try: 
                        contato, tipo_contato = verificar_tipo_de_contato(duplicata)
                        gravar_em_banco_para_envio([('CCT_Report_Colheita',datetime.now(),contato, tipo_contato, gerar_informativo_segunda_func_CCT(unidade), '')])
                        #print(gerar_informativo_segunda_func_CCT(unidade))
                    except IndexError as error_rp_col: print(f'\n* Erro em geração report Moagem de {duplicata}\n{error_rp_col}\n')
            else:
                try: 
                    contato, tipo_contato = verificar_tipo_de_contato(contato_destino)
                    gravar_em_banco_para_envio([('CCT_Report_Colheita',datetime.now(),contato, tipo_contato, gerar_informativo_segunda_func_CCT(unidade), '')])    
                    #print(gerar_informativo_segunda_func_CCT(unidade))
                except IndexError as error_rp_col: print(f'\n* Erro em geração report Moagem de {contato_destino}\n{error_rp_col}\n') 
    except IndexError as error_rp_col: print(f'\n*** ERRO em geração report Moagem!!!')

############ Velocidade PLANTADORAS

def velocidade_pl_horario():
    def dash_gov_velocidade_plantadoras():
        try:
            #excel = win32.gencache.EnsureDispatch('Excel.Application')
            excel = win32com.client.Dispatch('Excel.Application')
            t, pid__ = win32process.GetWindowThreadProcessId(excel.application.Hwnd)
            print('O pid é Excel é: ',pid__)
            Caminho_arquivo = verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\CIA Analytics - BOT CIA\Extrator\Integracoes Gov Op\[PLANTIO] - Velocidade Plantadoras PBI.xlsx')
            excel.Visible = True
            wb = excel.Workbooks.Open(Caminho_arquivo) 
            pvtTable = wb.Sheets('Vel Hora').Range("A3").PivotTable
            pvtTable.PivotFields("[TAB PLANTADORAS].[FILTER_1_HORAS].[FILTER_1_HORAS]").ClearAllFilters()
            for n in range(5):
                try:
                    pvtTable.PivotCache().Refresh()
                    sleep(1)
                    break
                except: pass
            pvtTable.PivotFields("[TAB PLANTADORAS].[FILTER_1_HORAS].[FILTER_1_HORAS]").CurrentPageName = '[TAB PLANTADORAS].[FILTER_1_HORAS].&[OK]'
            table_data = []
            for i in pvtTable.TableRange1:
                table_data.append(str(i))
            velocidade_pl = pd.DataFrame(np.array(table_data).reshape(round(len(table_data)/5),5), columns=['und','Frente','Frota','Vel','Meta']).iloc[2:, [1,2,3,4]]
            try:
                wb.Save()
                wb.Close()
                excel.Quit()
                print('Geração OK - Encerrado OK.')
            except:
                print('Geração OK - Arquivo excel encerrado de forma forçada.')
                try: psutil.Process(pid__).terminate()
                except: print('Não conseguimos encerrar EXCEL.exe')
            return velocidade_pl
        except:
            print('Geração NOK - Arquivo excel encerrado de forma forçada.')
            try: psutil.Process(pid__).terminate()
            except: print('Não conseguimos encerrar EXCEL.exe')
            return pd.DataFrame([], columns=['und','Frente','Frota','Vel','Meta'])


    velocidades_pl = dash_gov_velocidade_plantadoras()
    def atualizar_df_vel_pl_e_contatos():
        # Atualizar contatos
        cont_id_vl_pl = pd.read_excel(r'\\CSCLSFSR01\Agricola$\Logistica Agroindustrial\CIA 22.23\11. Analytics\BOT CIA\Contatos\Velocidades_PL.xlsx', sheet_name='Contatos')
        cont_id_vl_pl[['Unidade','Destino_envio']]
        cont_id_vl_pl.dropna(subset='Destino_envio', inplace=True)
        dict_ = dict(zip(cont_id_vl_pl.Unidade, cont_id_vl_pl.Destino_envio))
        # Atualizar dataframe velocidade pl
        df = pd.read_excel(verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\CIA Analytics - BOT CIA\Extrator\PLANTIO\Plantio_Hora.xlsx'))
        df.DT_LOCAL = pd.to_datetime(df.DT_LOCAL, format='%Y-%m-%d')
        df.HR_LOCAL = df.HR_LOCAL.astype(int)
        df.DT_LOCAL += pd.to_timedelta(df.HR_LOCAL, unit='h')
        df = df[(df.CD_OPERACAO == 789)]
        df = df.replace(',','.', regex=True)
        df = df[df.DESC_GRUPO_EQUIPAMENTO.str.contains(f'-PL-')]
        return dict_, df
    dict_cont_velpl, df_vp = atualizar_df_vel_pl_e_contatos()
    for und in dict_cont_velpl.keys():
        try:
            msg_vel_pl = []
            if und in ['CAARAPÓ','RIO BRILHANTE','PASSATEMPO']: df_slice = df_vp[(df_vp.DESC_UNIDADE == und) & (df_vp.DT_LOCAL > datetime.now()-timedelta(hours=2.95))]
            else: df_slice = df_vp[(df_vp.DESC_UNIDADE == und) & (df_vp.DT_LOCAL > datetime.now()-timedelta(hours=1.95))]
            if len(df_slice) < 1: pass
            else:
                vel_frentes = velocidades_pl[velocidades_pl.Frente.str.contains('|'.join(df_slice.DESC_GRUPO_EQUIPAMENTO.unique()))]
                msg_vel_pl.append(f'🌱🎋 *Velocidade Plantadoras {und}*\n')
                dict_frotas_max_min = {}
                print(f'A unidade {und} ficou com as frentes: {"|".join(df_slice.DESC_GRUPO_EQUIPAMENTO.unique())}')
                for frotas in vel_frentes.Frota.unique():
                    try: dict_frotas_max_min[frotas] = round(float(vel_frentes[(vel_frentes.Frota == frotas)].Vel.values[0]),1)
                    except: pass
                try:
                    msg_vel_pl.append(f'📈 Plantadora com maior desempenho: {(list(dict_frotas_max_min.keys())[list(dict_frotas_max_min.values()).index(max(dict_frotas_max_min.values()))])} - *{round(max(dict_frotas_max_min.values()),1)} km/h*')
                    msg_vel_pl.append(f'📉 Plantadora com menor desempenho: {(list(dict_frotas_max_min.keys())[list(dict_frotas_max_min.values()).index(min(dict_frotas_max_min.values()))])} - *{round(min(dict_frotas_max_min.values()),1)} km/h*')
                    for frente in vel_frentes.Frente.unique(): # loop da frente
                        msg_vel_pl.append(f'\n🎋 *Frente {frente[-3:]}*')
                        for frota in vel_frentes[(vel_frentes.Frente == frente)].Frota.unique():
                            try: vl_atual = round(float(vel_frentes[(vel_frentes.Frente == frente) & (vel_frentes.Frota == frota)].Vel.values[0]),1)
                            except: vl_atual = '-'
                            try:
                                mt_atual = round(float(vel_frentes[(vel_frentes.Frente == frente) & (vel_frentes.Frota == frota)].Meta.values[0]),1)
                                emoji_meta = '✅' if vl_atual >= mt_atual else '⚠️'
                                msg_vel_pl.append(f"PL {frota} - {emoji_meta} Vel {vl_atual} (PPC {mt_atual})")
                            except:
                                msg_vel_pl.append(f"PL {frota} - ❔ Vel {vl_atual} (PPC -)")
                    if len(msg_vel_pl) > 4: 
                        if und in ['CAARAPÓ','RIO BRILHANTE','PASSATEMPO']: msg_vel_pl.append(f"\n_Dados de {(datetime.now()-timedelta(hours=2)).hour}h00 até {(datetime.now()-timedelta(hours=2)).hour}h59._")
                        else: msg_vel_pl.append(f"\n_Dados de {(datetime.now()-timedelta(hours=1)).hour}h00 até {(datetime.now()-timedelta(hours=1)).hour}h59._")
                        mensagem_a_ser_enviada = '\n'.join(msg_vel_pl)
                        contato, tipo_contato = verificar_tipo_de_contato(dict_cont_velpl[und])
                        gravar_em_banco_para_envio([('CCT_Velocidade',datetime.now(),contato, tipo_contato, mensagem_a_ser_enviada, '')])
                except: print(f'erro em unidade: {und}')
        except IndexError as err: print(f'Pulado UND {und},\n{err}')

##################### Dados Pane Seca
def comparar_segunda_func_pane_seca():
    def lista_unidades_monitoradas_comboio():
        monit_comb = pd.read_excel(r'\\CSCLSFSR01\Agricola$\Logistica Agroindustrial\CIA 22.23\11. Analytics\BOT CIA\Contatos\Contatos_BOT_CIA_Manutencao.xlsx', sheet_name='1f_comb')
        monit_comb = monit_comb.iloc[:, [0,3]].dropna(subset='Envio')
        return '|'.join(list([f'{u}-' for u in monit_comb.Frente]))
    def carregar_historicos_PANE_SECA():
        data = pd.read_sql("""SELECT 
                                gerada_em,
                                mensagem
                            FROM 
                                envio_msg
                            WHERE
                                (gerada_em >= date('now', '-6 hours')
                                and
                                mensagem LIKE '%*Apontamento de PANE SECA!*%') 
                                OR
                                (gerada_em >= date('now', '-8 hours')
                                and
                                mensagem LIKE '%*Dados SGPA3 - PANE SECA!*%')""", con_temp)
        data.gerada_em = pd.to_datetime(data.gerada_em)
        con_temp.close()
        for id, row in data.iterrows(): data.loc[id, 'Frota'] = int(row.mensagem.split('*Frota:* ')[1].split('\n*Tipo:* ')[0])
        try: data.Frota = data.Frota.astype(int)
        except: data.loc[0, 'Frota'] = 0
        return data
    def carregar_dados_last_sgpa3():
        conn_sgpa3 = sqlite3.Connection(verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\CIA Analytics - BOT CIA\Extrator\SGPA3\Exportacao Monit.db'))
        data_ = pd.read_sql("SELECT * FROM 'Exportacao Monit'", conn_sgpa3)
        #data_ = data_.iloc[:,[0,1,2,8,9,11,10,15,16,22]]
        data_ = data_[data_.Grupo.str.contains(lista_unidades_monitoradas_comboio())]
        return data_
    def gerar_mensagem_pane_dados(Frota,Frente,Tipo,Inicio):
        mensagem_pane_dados = []
        mensagem_pane_dados.append(f"❌⛽📡 *Dados SGPA3 - PANE SECA!*")
        mensagem_pane_dados.append(f"*Frente:* {Frente}")
        mensagem_pane_dados.append(f"*Frota:* {Frota}")
        mensagem_pane_dados.append(f"*Tipo:* {Tipo}")
        mensagem_pane_dados.append(f"⏱️ _Inicio do apontamento: {Inicio}_")
        return '\n'.join(mensagem_pane_dados)
    con_temp = sqlite3.connect(r"C:\CIAANALYTICS\1 - Producao\1 4 - Banco\envio_msg.db")
    #con_temp = sqlite3.connect(r'\\CSCLSFSR01\Agricola$\Logistica Agroindustrial\CIA 22.23\11. Analytics\1 4 - Banco\envio_msg.db')
    df_hist = carregar_historicos_PANE_SECA()
    df_monit = carregar_dados_last_sgpa3()
    df_monit.dropna(subset='Operacao', inplace=True)
    # Falta de Combustivel | Falta de Caminhão
    apontamento_alvo = 'Falta de Combustivel'
    for frota_pane in df_monit[df_monit.Operacao.str.contains(apontamento_alvo)].Equipamento.unique():
        slice = df_monit[(df_monit.Equipamento == frota_pane) & (df_monit.Operacao.str.contains(apontamento_alvo))].copy() #Grupo
        for id, sl in slice.iterrows():
                agora = sl['Data/Hora']
                if id == slice.index[0]: pass
                else: slice.loc[id, 'Duracao'] = np.datetime64(agora) - np.datetime64(passado)
                passado = sl['Data/Hora']
        if len(slice[slice.Operacao == apontamento_alvo]) == 1 or len(slice[slice.Operacao.str.contains(apontamento_alvo)]) == 1:
            if frota_pane not in list(df_hist.Frota):
                Inicio_Pane = (np.datetime64(slice["Data/Hora"].values[0]).astype(datetime)).strftime('%d/%m/%Y %H:%M:%S')
                # print(f'Pane em {frota_pane} de {slice.Grupo.values[0]} ás: {Inicio_Pane}')
                mensagem_a_ser_enviada = gerar_mensagem_pane_dados(Frota=slice[slice.Operacao.str.contains(apontamento_alvo)]['Equipamento'].values[0],
                                                Frente=slice[slice.Operacao.str.contains(apontamento_alvo)]['Grupo'].values[0],
                                                Tipo=slice[slice.Operacao.str.contains(apontamento_alvo)]['Tipo de Equipamento'].values[0],
                                                Inicio=Inicio_Pane,)
                contato, tipo_contato = verificar_tipo_de_contato('Report Pane-Seca')
                gravar_em_banco_para_envio([('COMBOIO_Pane_Seca_Dados',datetime.now(),contato, tipo_contato, mensagem_a_ser_enviada, '')])
        else:
            if frota_pane not in list(df_hist.Frota):
                #Duracao_ = slice[slice.Operacao.str.contains(apontamento_alvo)].Duracao.sum()
                #if (Duracao_.total_seconds() / 60) > 60: Duracao_ = f'{round((Duracao_.total_seconds() / 60) / 60,1)} horas'
                #else: Duracao_ = f'{round(Duracao_.total_seconds() / 60)} minutos'
                Inicio_Pane = (np.datetime64(slice[slice.Operacao.str.contains(apontamento_alvo)]["Data/Hora"].values[0]).astype(datetime)).strftime('%d/%m/%Y %H:%M:%S')
                # print(f'Pane para {frota_pane} de {slice.Grupo.values[0]}, durante: {Duracao_} (Inicio: {Inicio_Pane})')
                mensagem_a_ser_enviada = gerar_mensagem_pane_dados(Frota=slice[slice.Operacao.str.contains(apontamento_alvo)]['Equipamento'].values[0],
                                                Frente=slice[slice.Operacao.str.contains(apontamento_alvo)]['Grupo'].values[0],
                                                Tipo=slice[slice.Operacao.str.contains(apontamento_alvo)]['Tipo de Equipamento'].values[0],
                                                Inicio=Inicio_Pane,)
                contato, tipo_contato = verificar_tipo_de_contato('Report Pane-Seca')
                gravar_em_banco_para_envio([('COMBOIO_Pane_Seca_Dados',datetime.now(),contato, tipo_contato, mensagem_a_ser_enviada, '')])

def Verificar_Panes_Secas_old():
    acompanhamento = pd.read_excel(verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\MinhaTI\CIA Analytics - Comboio\Acompanhamento_Smart_route.xlsx'))
    acompanhamento[["equipmentExternalSystemId","plannedTime"]]
    acompanhamento = dict(zip(acompanhamento.equipmentExternalSystemId,acompanhamento.plannedTime))

    # Se não existem dados de últimas panes criar.
    if not os.path.exists('Comboio_Memoria'):
        os.mkdir('Comboio_Memoria')
        with open('Comboio_Memoria/Comboio_Memoria.json', 'w') as file:
            json.dump(['Lista_criada_agora'], file)

    def mensagem_comboio_programada(data):
        frota = int(data["Número do Equipamento"])
        if frota in rotas.keys() and type(rotas[frota]) != type(pd.NaT):
            status_abastecimento = f"*Último Abastecimento:* {rotas[frota].strftime('%d/%m/%Y ás %H:%M:%S')}"
        else: 
            status_abastecimento = '*Último Abastecimento:* Não encontrada'
        if frota in acompanhamento.keys() and type(acompanhamento[frota]) != type(pd.NaT):
            acompanhamento_abastec = f"✅ Programada para {acompanhamento[frota].strftime('%d/%m/%Y ás %H:%M:%S')}"
        else: 
            acompanhamento_abastec = "⭕ Não existe no roteiro deste turno"
        lista_1f_comb = f"""❌⛽ *Apontamento de PANE SECA! [SGPA3]*
*Frente:* {data["Frente associada"]}
*Frota:* {data["Número do Equipamento"]}
*Tipo:* {data["Tipo do equipamento"].split(' ')[0]}
*Apontamento:* {data["Atividade"]}
⏱️ _Ultima comunicação: {data["Registro mais recente"]}_
{status_abastecimento}
{acompanhamento_abastec}"""
        return lista_1f_comb

    # Coletando rotas do Smart Route
    rotas = pd.read_excel(verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\MinhaTI\CIA Analytics - Comboio\Rotas_Smart_route.xlsx'))
    rotas['lastRefuel'] = [last if str(acomp) == str('NaT') else acomp for last,acomp in zip(rotas['lastRefuel'],rotas['accomplishedTime'])]
    rotas = rotas[['equipmentExternalSystemId','lastRefuel']]
    rotas = rotas.sort_values(by=['equipmentExternalSystemId','lastRefuel'], ascending=False)
    rotas = rotas.drop_duplicates(keep='first', subset='equipmentExternalSystemId') 
    rotas = dict(zip(rotas.equipmentExternalSystemId,rotas.lastRefuel))

    # Coletando frentes que podem ser monitoradas
    frentes = pd.read_excel(r'\\CSCLSFSR01\Agricola$\Logistica Agroindustrial\CIA 22.23\11. Analytics\BOT CIA\Contatos\Contatos_BOT_CIA_Manutencao.xlsx', sheet_name="1f_comb")
    frentes = list(frentes.dropna(subset='Envio').Frente)
    fretes_monitoradas = pd.read_excel(r'\\CSCLSFSR01\Agricola$\Logistica Agroindustrial\CIA 22.23\11. Analytics\BOT CIA\Contatos\Contatos_BOT_CIA_Manutencao.xlsx', sheet_name='1f_comb')
    fretes_monitoradas = list(fretes_monitoradas.dropna(subset='Envio').Frente.unique())
    fretes_monitoradas = '|'.join(fretes_monitoradas)

    # Conxeção com banco
    dfn = carregar_df_monitoramento_SGPA3()
    #dfn = dfn.drop(columns="_id")
    dfn = dfn[dfn['Frente associada'].str.contains(fretes_monitoradas)]
    with open('Comboio_Memoria/Comboio_Memoria.json', 'r') as file:
            memoria_panes = json.load(file)
    frotas_desconsierar = [frota for frota in memoria_panes if frota not in list(dfn["Número do Equipamento"].unique())]
    dfn = dfn[(dfn['Atividade'].str.contains('211 - Falta de Combustivel')) 
        & (~dfn['Tempo em atividade'].str[-6:].str.contains("|".join([f"0{i}:" for i in range(10)])))]
    #estao_em_pane = list(dfn["Número do Equipamento"].unique())
    # Trava JSON
    if len(dfn) > 0:    
        frotas_211_memoria = list(dfn['Número do Equipamento'].unique())

        dfn = dfn[~dfn['Número do Equipamento'].isin(memoria_panes)]
        dfn = dfn[~dfn['Frente associada'].str.contains('-LN-')]
        memoria_panes = frotas_211_memoria+frotas_desconsierar

        for id, row in dfn.iterrows():
            mensagem_pane_seca_v2 = mensagem_comboio_programada(row)
            contato, tipo_contato = verificar_tipo_de_contato('Report Pane-Seca')
            gravar_em_banco_para_envio([('COMBOIO_Pane_Seca_V2',datetime.now(),contato, tipo_contato, mensagem_pane_seca_v2,'')])


        with open('Comboio_Memoria/Comboio_Memoria.json', 'w') as file:
                json.dump(memoria_panes, file)

        print('Monitoramento Comboio V2 - OK')
    print('Base veio em branco!')

def Verificar_Panes_Secas():
    arquivo_registro_json = f'Comboio_Memoria\Comboio_Memoria_SGPA3.json'
    if not os.path.exists('Comboio_Memoria'):
        os.mkdir('Comboio_Memoria')

    def salvar_acionamento(meu_dict_alvo):
        def serializar_datetime(obj):
            if isinstance(obj, datetime):
                return obj.isoformat()
            raise TypeError(f"Tipo '{type(obj)}' não é serializável.")
        with open(arquivo_registro_json, "w") as arquivo:
            json.dump(meu_dict_alvo, arquivo, default=serializar_datetime)

    def carregar_acionamento():
        if not os.path.exists(arquivo_registro_json):
            salvar_acionamento({'teste':datetime.now()})
            print('Criado arquivo')
        with open(arquivo_registro_json, "r") as arquivo:
            meu_dict = json.load(arquivo)
        for chave in meu_dict.keys():
            meu_dict[chave] = pd.to_datetime(meu_dict[chave], dayfirst=False)
        return meu_dict

    acompanhamento = pd.read_excel(verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\MinhaTI\CIA Analytics - Comboio\Acompanhamento_Smart_route.xlsx'))
    acompanhamento[["equipmentExternalSystemId","plannedTime"]]
    acompanhamento = dict(zip(acompanhamento.equipmentExternalSystemId,acompanhamento.plannedTime))

    # Se não existem dados de últimas panes criar.
    if not os.path.exists('Comboio_Memoria'):
        os.mkdir('Comboio_Memoria')
        if os.path.exists('Comboio_Memoria\Comboio_Memoria_SGPA3.json'):
            salvar_acionamento({'111111':datetime.now()})

    def mensagem_comboio_programada(data):
        frota = int(data["Número do Equipamento"])
        if frota in rotas.keys() and type(rotas[frota]) != type(pd.NaT):
            status_abastecimento = f"*Último Abastecimento:* {rotas[frota].strftime('%d/%m/%Y ás %H:%M:%S')}"
        else: 
            status_abastecimento = '*Último Abastecimento:* Não encontrada'
        if frota in acompanhamento.keys() and type(acompanhamento[frota]) != type(pd.NaT):
            acompanhamento_abastec = f"✅ Programada para {acompanhamento[frota].strftime('%d/%m/%Y ás %H:%M:%S')}"
        else: 
            acompanhamento_abastec = "⭕ Não existe no roteiro deste turno"
        lista_1f_comb = f"""❌⛽ *Apontamento de PANE SECA! [SGPA3]*
*Frente:* {data["Frente associada"]}
*Frota:* {data["Número do Equipamento"]}
*Tipo:* {data["Tipo do equipamento"].split(' ')[0]}
*Apontamento:* {data["Atividade"]}
⏱️ _Ultima comunicação: {data["Registro mais recente"]}_
{status_abastecimento}
{acompanhamento_abastec}"""
        return lista_1f_comb

    # Coletando rotas do Smart Route
    rotas = pd.read_excel(verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\MinhaTI\CIA Analytics - Comboio\Rotas_Smart_route.xlsx'))
    rotas['lastRefuel'] = [last if str(acomp) == str('NaT') else acomp for last,acomp in zip(rotas['lastRefuel'],rotas['accomplishedTime'])]
    rotas = rotas[['equipmentExternalSystemId','lastRefuel']]
    rotas = rotas.sort_values(by=['equipmentExternalSystemId','lastRefuel'], ascending=False)
    rotas = rotas.drop_duplicates(keep='first', subset='equipmentExternalSystemId')
    rotas = dict(zip(rotas.equipmentExternalSystemId,rotas.lastRefuel))

    # Coletando frentes que podem ser monitoradas
    fretes_monitoradas = pd.read_excel(r'\\CSCLSFSR01\Agricola$\Logistica Agroindustrial\CIA 22.23\11. Analytics\BOT CIA\Contatos\Contatos_BOT_CIA_Manutencao.xlsx', sheet_name='1f_comb')
    fretes_monitoradas = list(fretes_monitoradas.dropna(subset='Envio').Frente.unique())
    fretes_monitoradas = '|'.join(fretes_monitoradas)

    # Conxeção com banco
    dfn = carregar_df_monitoramento_SGPA3()
    dfn["Número do Equipamento"] = dfn["Número do Equipamento"].astype(str)
    dfn = dfn[~dfn['Frente associada'].str.contains("-LN-")]
    #dfn = dfn.drop(columns="_id")
    acionamentos = carregar_acionamento()
    #dfn = pd.read_excel(r'C:\Users\ciaanalytics\MinhaTI\CIA Analytics - BOT CIA\Extrator\SGPA3\monitoramento_sgpa3-D-FR4K1M3-8.xlsx')
    dfn = dfn[dfn['Frente associada'].str.contains(fretes_monitoradas)]
    dfn = dfn[(dfn['Atividade'].str.contains('211 - Falta de Combustivel')) 
        & (~dfn['Tempo em atividade'].str[-6:].str.contains("|".join([f"0{i}:" for i in range(10)])))]
    dfn['Registro mais recente'] = pd.to_datetime(dfn['Registro mais recente'], dayfirst=True)
    for id, row in dfn.iterrows():
        frota_ref = row["Número do Equipamento"]
        registro_ref = row["Registro mais recente"]
        if frota_ref not in acionamentos.keys() or row["Registro mais recente"] > acionamentos[frota_ref]:
            acionamentos[frota_ref] = registro_ref
            contato, tipo_contato = verificar_tipo_de_contato('Report Pane-Seca')
            gravar_em_banco_para_envio([('COMBOIO_Pane_Seca_V2',datetime.now(),contato, tipo_contato, mensagem_comboio_programada(row),'')])
            salvar_acionamento(acionamentos)
    print('Pane Seca Verificado com sucesso!')

#### Análise de Cenário CCT:
def analise_cenario_cct():
    try:
        print(f'Iniciando Verificação de Cenários CCT, a carregar bases [ás {datetime.now()}].')
        # Verificando quais unidades para envio:
        def quais_unidades_enviar_analise_cen_cct(lista_de_unidades):
            CAMINHO_db_analise_cenario_CCT = r'C:\CIAANALYTICS\1 - Producao\1 4 - Banco\analise_cenario_cct.db'
            CAMINHO_db_envio_msg = r'C:\CIAANALYTICS\1 - Producao\1 4 - Banco\envio_msg.db'
            #CAMINHO_db_analise_cenario_CCT = r'\\CSCLSFSR01\Agricola$\Logistica Agroindustrial\CIA 22.23\11. Analytics\1 4 - Banco\analise_cenario_cct.db'
            #CAMINHO_db_envio_msg = r'\\CSCLSFSR01\Agricola$\Logistica Agroindustrial\CIA 22.23\11. Analytics\1 4 - Banco\envio_msg.db'

            PADRAO_MSG_CHUVA = 'Usina em cenário de chuva' # 'Usina parada por chuva' 'Mesmo ofensor' 'Divergente da projeção' 'Usina parada por limitação'

            def CHECK_carregar_base_envio_mesagens_gatilho(CAMINHO_db_envio_msg):
                # Base de Analises de Cenario do CCT
                conn = sqlite3.connect(CAMINHO_db_envio_msg)
                gat_cct = pd.read_sql(f'''
                        SELECT * 
                        FROM envio_msg
                        WHERE gerada_por = "CCT_Analise_Cenario"
                        AND destino = "Contato"
                        AND gerada_em > '{datetime.now()-timedelta(hours=7)}'
                                ''', conn)
                conn.close()
                gat_cct.gerada_em = pd.to_datetime(gat_cct.gerada_em)
                gat_cct = gat_cct[gat_cct.gerada_em >= datetime(2023,7,1)]
                gat_cct.reset_index(drop=True, inplace=True)
                gat_cct['DATA'] = [r.date() for r in gat_cct.gerada_em]
                gat_cct['HORA'] = [r.hour for r in gat_cct.gerada_em]
                gat_cct['TURNO'] = ['A' if(r.hour in [7,8,9,10,11,12,13,14]) else 'B' if(r.hour in [15,16,17,18,19,20,21,22]) else 'C' for r in gat_cct.gerada_em]
                gat_cct['Turno'] = [f"{(r-timedelta(hours=7)).day}/{(r-timedelta(hours=7)).month}" for r in gat_cct.gerada_em]
                gat_cct['Turno'] = 'Turno ' + gat_cct.TURNO.astype(str) + ' ' + gat_cct.Turno.astype(str)
                gat_cct['Unidade'] = [r.split('\n')[0].replace('📍 *GATILHO - Analise Cenário de ','')[:-1] for r in gat_cct.mensagem]
                polo_centro_sul = ['VALER','CAAR','MORRO','SELIS', 'RBRIL', 'PASSA','LPRAT','CONTI','LEME','JUN','BONF']
                gat_cct['Polo'] = ['Centro Sul' if r in polo_centro_sul else 'Leste & Oeste' for r in gat_cct.Unidade]
                gat_cct.drop(columns=['gerada_por','destino','anexo','envio_status'], inplace=True)
                return gat_cct

            def CHECK_carregar_base_respostas_analise_cenario_CCT(CAMINHO_db_analise_cenario_CCT):
                # Base de Analises de Cenario do CCT
                conn = sqlite3.connect(CAMINHO_db_analise_cenario_CCT)
                last_6h = (datetime.now()-timedelta(hours=5.5)).timestamp()*1000
                res_cct = pd.read_sql(f"SELECT * FROM analise_cenario_cct where recebida_h > {last_6h}", conn)
                conn.close()
                res_cct.gerada_em = [datetime.fromtimestamp(f) for f in res_cct.gerada_em]
                res_cct = res_cct[(res_cct.gerada_em >= datetime(2023,7,1))]
                res_cct = res_cct[res_cct.de_.str.contains('@c.us')]
                return res_cct

            resp_cct = CHECK_carregar_base_respostas_analise_cenario_CCT(CAMINHO_db_analise_cenario_CCT)
            gat_cct = CHECK_carregar_base_envio_mesagens_gatilho(CAMINHO_db_envio_msg)

            resposta = []
            for id,gat in gat_cct.iterrows(): # Base Gatilho
                    inicio_gatilho = gat.gerada_em
                    fim_gatilho = inicio_gatilho+timedelta(hours=1, minutes=25)
                    contato_gatilho = str(gat.para_)
                    resp_db = resp_cct[(resp_cct.gerada_em > inicio_gatilho) & (resp_cct.gerada_em < fim_gatilho) & (resp_cct.de_.str.contains(contato_gatilho))]
                    #resposta.append(resp_db.mensagem.values)
                    resposta.append([resp_db.mensagem.values,resp_db.mensagem.values])
            gat_cct['resp'] = resposta
            gat_cct['resp_OK'] = [True if len(r) > 0 else False for r in gat_cct.resp]
            gat_cct['CHUVA'] = [True if sum([True if PADRAO_MSG_CHUVA.lower() in str(y.tolist())[:49].lower() else False for y in x])> 0 else False for x in gat_cct.resp]
            # Filtrando respostas que tivemos: "Usina parada por chuva" pelo Analista CIA.
            unidade_parada_chuva = list(set(gat_cct[gat_cct.CHUVA==True].Unidade.values))
            lista_envio = [f for f in lista_de_unidades if f not in unidade_parada_chuva]
            return lista_envio

        #>>> Contatos:
        CAMINHO_contatos_CCT = r'\\CSCLSFSR01\Agricola$\Logistica Agroindustrial\CIA 22.23\11. Analytics\BOT CIA\Contatos\CCT_Contatos.xlsx'
        def carregar_contatos_cct_torre(caminho):
            torre_cct = pd.read_excel(caminho)
            torre_cct = torre_cct[torre_cct.CONTROLE_Analise_Cenario=='SIM']
            torre_cct = torre_cct[['Torre_Numero','Unidade']]
            torre_cct = dict(zip(torre_cct.Unidade,torre_cct.Torre_Numero))
            return torre_cct
        torre_cct = carregar_contatos_cct_torre(CAMINHO_contatos_CCT)
        #>>>> Parametros:
        hora_recente = datetime.now()-timedelta(minutes=datetime.now().minute)
        hora_menos_3h = datetime.now()-timedelta(minutes=datetime.now().minute+0.2, hours=3)
        meta_moagem = pd.read_excel(verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\Metas_Gov_Indicadores - Documentos\CCT\Parâmetros Relatórios CCT (Metas).xlsx'), sheet_name='Metas')
        meta_moagem = dict(zip(list(meta_moagem['UNIDADE']),list(meta_moagem['Meta Moagem TCD'])))
        meta_moagem['MORRO'] = meta_moagem['UMB']
        meta_moagem['DDC'] = meta_moagem['DIA']
        meta_moagem['COP'] = meta_moagem['COPI']
        meta_moagem['PASSA'] = meta_moagem['PTP']
        meta_moagem['LEME'] = meta_moagem['LEM']
        meta_moagem['SELIS'] = meta_moagem['SEL']
        meta_moagem['VALER'] = meta_moagem['VRO']
        meta_moagem['LPRAT'] = meta_moagem['LPT']
        meta_moagem['CONTI'] = meta_moagem['CNT']
        meta_moagem['RBRIL'] = meta_moagem['RBR']

        #>>>> Contatos Torre:
        #---> INSERIR BASE DE CONTATOS

        #>>>> Base de Cargas 1: Somatórias de cargas interno [FF e PP] + Externo Própria [PP]
        estoque_p = pd.read_excel(verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\CIA Analytics - BOT CIA\Extrator\CCT\Cargas\Cargas_1.xlsx'))
        estoque_p = estoque_p[(((hora_recente > estoque_p.HR_ENTRADAA) 
                                & (hora_recente < estoque_p.HR_SAIDAA)) 
                                & (~estoque_p.instancia.str.contains('CAAR|RBRIL|PASSA')) 
                                | ((estoque_p.HR_GUARITAA < hora_recente) & (estoque_p.HR_ENTRADAA > hora_recente) 
                                & (~estoque_p.instancia.str.contains('CAAR|RBRIL|PASSA')))) 
                                | (((hora_recente-timedelta(hours=1) > estoque_p.HR_ENTRADAA) 
                                    & (hora_recente-timedelta(hours=1) < estoque_p.HR_SAIDAA)) 
                                    & (estoque_p.instancia.str.contains('CAAR|RBRIL|PASSA')) 
                                    | ((estoque_p.HR_GUARITAA < hora_recente-timedelta(hours=1)) 
                                    & (estoque_p.HR_ENTRADAA > hora_recente-timedelta(hours=1)) 
                                    & (estoque_p.instancia.str.contains('CAAR|RBRIL|PASSA'))))]

        #>>>> Estoque de Cargas 2: 
        estoque_f = pd.read_excel(verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\CIA Analytics - BOT CIA\Extrator\CCT\Cargas\Cargas_2.xlsx'))
        estoque_f = estoque_f[((estoque_f.HR_BALANCA >= hora_recente) 
                            & (hora_recente >= estoque_f.HR_CHECKIN) 
                            & (~estoque_f.INSTANCIA.str.contains('CAAR|RBRIL|PASSA')))
                            & (estoque_f.PROPRIEDADE_EQPTO == 'Foca') & (estoque_f.TIPO_CANA == 'Mechanized')
                            | ((estoque_f.HR_BALANCA >= hora_recente-timedelta(hours=1)) 
                            & (estoque_f.HR_CHECKIN <= hora_recente-timedelta(hours=1)) 
                            & (estoque_f.PROPRIEDADE_EQPTO == 'Foca') & (estoque_f.TIPO_CANA == 'Mechanized')
                            & (estoque_f.INSTANCIA.str.contains('CAAR|RBRIL|PASSA')))]

        #>>>> Estoque Parametros Rotina: Troca de cana
        #---> INSERIR TROCA DE CANA

        #>>>> Base Moagem: Média entrega ultimas 3h e densidade média por cargas
        moagem = pd.read_excel(verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\CIA Analytics - BOT CIA\Extrator\CCT\Moagem\Ton_cana.xlsx'))
        frente_pp = pd.read_excel(verificar_base_atualizada(r'\\CSCLSFSR01\Agricola$\Logistica Agroindustrial\CIA 22.23\11. Analytics\BOT CIA\Contatos\REF segunda func CCT.xlsx'), sheet_name='mix')
        # Densidade Geral de Cargas
        dens_geral = moagem[(moagem.data_hora >= datetime.now()-timedelta(hours=24, minutes=datetime.now().minute+1)) 
                        & (moagem.data_hora <= hora_recente) 
                        & (~moagem.instancia.str.contains('CAAR|RBRIL|PASSA'))
                        | (moagem.data_hora >= datetime.now()-timedelta(hours=25, minutes=datetime.now().minute+1)) 
                        & (moagem.data_hora <= hora_recente-timedelta(hours=1))
                        & (moagem.instancia.str.contains('CAAR|RBRIL|PASSA'))]
        dens_geral['DONO'] = dens_geral['CD_FREN_TRAN'].apply(lambda x: "PP" if x in list(frente_pp.Frente) else "FF")
        def densidade_und(df):
            moagem = df['QT_LIQUIDO']
            cargas = df['CONTAGEM_CARGAS']
            return round(moagem.sum() / cargas.sum()/1000,2)
        densidade_geral = dens_geral.groupby(['instancia','DONO']).apply(densidade_und)
        # Frentes Foca
        moagem_cm_ff = moagem[(moagem.data_hora >= hora_menos_3h) 
                            & (moagem.data_hora <= hora_recente) 
                            & (~moagem.CD_FREN_TRAN.isin(list(frente_pp.Frente)))
                            & (~moagem.instancia.str.contains('CAAR|RBRIL|PASSA'))
                            | (moagem.data_hora >= hora_menos_3h-timedelta(hours=1)) 
                            & (moagem.data_hora <= hora_recente-timedelta(hours=1)) 
                            & (~moagem.CD_FREN_TRAN.isin(list(frente_pp.Frente)))
                            & (moagem.instancia.str.contains('CAAR|RBRIL|PASSA'))]
        def soma_entrega_ff(df):
            entrada = df['QT_LIQUIDO']
            return entrada.sum()/3
        moagem_cm_ff = moagem_cm_ff.groupby(['instancia']).apply(soma_entrega_ff)
        # Frentes próprias
        def media_carga_por_frente(df):
            qt_liquido = df['QT_LIQUIDO']
            cargas = df['CONTAGEM_CARGAS']
            return round(qt_liquido.sum() / cargas.sum())
        # Densidade PP
        densidade_cm_pp = moagem[(moagem.data_hora >= datetime.now()-timedelta(hours=3, minutes=datetime.now().minute+.05)) 
                            & (moagem.CD_FREN_TRAN.isin(list(frente_pp.Frente)))
                            & (~moagem.instancia.str.contains('CAAR|RBRIL|PASSA'))
                            | (moagem.data_hora >= datetime.now()-timedelta(hours=3+1, minutes=datetime.now().minute+.05)) 
                            & (moagem.CD_FREN_TRAN.isin(list(frente_pp.Frente)))
                            & (moagem.instancia.str.contains('CAAR|RBRIL|PASSA'))]
        densidade_pp = densidade_cm_pp.groupby(['instancia']).apply(media_carga_por_frente)
        # Densidade FF
        densidade_cm_ff = moagem[(moagem.data_hora >= datetime.now()-timedelta(hours=datetime.now().hour, minutes=datetime.now().minute+.5)) 
                            & (~moagem.CD_FREN_TRAN.isin(list(frente_pp.Frente)))
                            & (~moagem.instancia.str.contains('CAAR|RBRIL|PASSA'))
                            | (moagem.data_hora >= datetime.now()-timedelta(hours=datetime.now().hour+1, minutes=datetime.now().minute+.5)) 
                            & (~moagem.CD_FREN_TRAN.isin(list(frente_pp.Frente)))
                            & (moagem.instancia.str.contains('CAAR|RBRIL|PASSA'))]
        densidade_ff = densidade_cm_ff.groupby(['instancia']).apply(media_carga_por_frente)
        # Moagem ultimas 3h
        moagem_3h = moagem[(moagem.data_hora >= hora_menos_3h) 
                        & (moagem.data_hora <= hora_recente) 
                        & (~moagem.instancia.str.contains('CAAR|RBRIL|PASSA'))
                        | (moagem.data_hora >= hora_menos_3h-timedelta(hours=1)) 
                        & (moagem.data_hora <= hora_recente-timedelta(hours=1)) 
                        & (moagem.instancia.str.contains('CAAR|RBRIL|PASSA'))]
        unidades_elegiveis = list(moagem.instancia.unique())
        # Média da entrada
        def media_entrega_(df):
            qt_liquido = df['QT_LIQUIDO']
            return round(qt_liquido.sum()/1000)
        def moagem_media_3h(df):
            qt_liquido = df['QT_LIQUIDO']
            return round(qt_liquido.sum()/1000/3)
        media_entrega = moagem_3h.groupby(['instancia','Início da Hora']).apply(media_entrega_)
        moagem_med = moagem_3h.groupby(['instancia']).apply(moagem_media_3h)
        #>>>> Check_in: Quantidade média de entrega das últimas 3h:
        ck_in = pd.read_excel(verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\CIA Analytics - BOT CIA\Extrator\CCT\Check_In_Out.xlsx'))
        ck_in['Momento'] = pd.to_datetime(ck_in.DE_DATA) + pd.to_timedelta(ck_in.DE_HORA)
        if ck_in.Momento.max() < datetime.now()-timedelta(hours=1): 
            print(f'*****************\n[NOK] Analise de cenário não foi atualizada devido a base de check-in e out estar desatualizada!!! {ck_in.Momento.max()}')
        ck_in = ck_in[(ck_in.Momento <= hora_recente) 
                    & (ck_in.Momento >= hora_menos_3h) 
                    & (~ck_in.UNIDADE.str.contains('CAAR|RBRIL|PASSA'))
                    | (ck_in.Momento <= hora_recente-timedelta(hours=1)) 
                    & (ck_in.Momento >= hora_menos_3h-timedelta(hours=1)) 
                    & (ck_in.UNIDADE.str.contains('CAAR|RBRIL|PASSA'))]
        def soma_entrega(df):
            entrada = df['QT_CHECKIN']
            return round(entrada.sum(),2)
        soma_entrega_3h = ck_in.groupby('UNIDADE').apply(soma_entrega)
        ##### Iniciando lógica de processamento.
        def pegar_cargas_ext_jat_jun(unidade_alvo):
            if unidade_alvo in ['JATAI', 'JAT', 'JATAÍ', 'JUN', 'JUNQ', 'JUNQUEIRA']:
                if unidade_alvo in ['JATAI', 'JAT', 'JATAÍ']:
                    try:
                        while True:
                            try: 
                                dados_rotina_cct = pd.read_excel(verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\Metas_Gov_Indicadores - Documentos\CCT\Parâmetros Relatórios CCT (Rotina).xlsx'), sheet_name='Pátio Externo FF')
                                break
                            except: print('Erro em carregar Parâmetros Relatórios CCT (Rotina).xlsx - JAT')
                        cargas_FF = dados_rotina_cct.iloc[0][2] + dados_rotina_cct.iloc[0][1]
                    except: 
                        cargas_FF = 0
                    return cargas_FF
                if unidade_alvo in ['JUN', 'JUNQ', 'JUNQUEIRA']:
                    try:
                        while True:
                            try: 
                                dados_rotina_cct = pd.read_excel(verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\Metas_Gov_Indicadores - Documentos\CCT\Parâmetros Relatórios CCT (Rotina).xlsx'), sheet_name='Pátio Externo FF')
                                break
                            except: print('Erro em carregar Parâmetros Relatórios CCT (Rotina).xlsx - JUN')
                        cargas_FF = dados_rotina_cct.iloc[1][1] + dados_rotina_cct.iloc[1][2]
                        cargas_PP = int(dados_rotina_cct.iloc[0][4])
                    except:
                        cargas_FF = 0
                        cargas_PP = 0
                    return cargas_PP+cargas_FF
            else: return 0
        print(f'Bases carregadas, a verificar unidades [ás {datetime.now()}].')
        moagem_1h = moagem[(moagem.data_hora >= datetime.now()-timedelta(hours=1, minutes=datetime.now().minute)) 
                        & (~moagem.instancia.str.contains('CAAR|RBRIL|PASSA'))
                        | (moagem.data_hora >= datetime.now()-timedelta(hours=2, minutes=datetime.now().minute)) 
                        & (moagem.instancia.str.contains('CAAR|RBRIL|PASSA'))]
        for unidade in quais_unidades_enviar_analise_cen_cct(torre_cct.keys()): # torre_cct.keys()
            print(f'Moagem {unidade} ultima hora de: {round(moagem_1h[moagem_1h.instancia == unidade].QT_LIQUIDO.sum())} ton') #ck_in.Momento.max() < datetime.now()-timedelta(hours=1)
            if round(moagem_1h[moagem_1h.instancia == unidade].QT_LIQUIDO.sum()) > 1 and ck_in.Momento.max() > datetime.now()-timedelta(hours=1):
                try:
                    def emoji(autonomia_alvo):
                        try:
                            if autonomia_alvo > timedelta(3/24): return '🟣'
                            if autonomia_alvo > timedelta(1.5/24): return '🟢'
                            if autonomia_alvo <= timedelta(0): return '🔴'
                            if autonomia_alvo > timedelta(1/24): return '🟡'
                            if autonomia_alvo <= timedelta(1/24): return '🟠'
                        except: return ''
                    cargas = estoque_p[estoque_p.instancia == unidade].CARGAS.sum()
                    cargas += estoque_f[estoque_f.INSTANCIA == unidade].CARGAS.sum()
                    cargas += pegar_cargas_ext_jat_jun(unidade)
                    carga_med = densidade_geral[unidade].values.mean()
                    meta_dia = meta_moagem[unidade]/24 if unidade in meta_moagem.keys() else 0
                    dens_pp = densidade_pp[unidade] if unidade in densidade_pp.keys() else carga_med
                    entrega_pp_med = round((soma_entrega_3h[unidade]/3)*dens_pp*2/1000) if unidade in soma_entrega_3h.keys() else 0
                    entrega_ff_med = round(moagem_cm_ff[unidade]/1000) if unidade in moagem_cm_ff.keys() else 0
                    if unidade == 'JATAI':
                        entrega_med = round((soma_entrega_3h[unidade]/3)*carga_med*2)
                        entrega_ff_med = 0
                    else:
                        entrega_med = entrega_pp_med + entrega_ff_med
                    estoque = round(cargas*(dens_pp/1000)) #carga_med
                    gap_estoque = round(entrega_med-moagem_med[unidade])
                    autonomia_atual = timedelta((estoque/meta_dia)/24) if meta_dia > 0 else timedelta(0)
                    autonomia_1h = timedelta(((estoque+gap_estoque*1)/meta_dia)/24) if meta_dia > 0 else timedelta(0)
                    autonomia_2h = timedelta(((estoque+gap_estoque*2)/meta_dia)/24) if meta_dia > 0 else timedelta(0)
                    autonomia_3h = timedelta(((estoque+gap_estoque*3)/meta_dia)/24) if meta_dia > 0 else timedelta(0)
                    autonomia_4h = timedelta(((estoque+gap_estoque*4)/meta_dia)/24) if meta_dia > 0 else timedelta(0)
                    autonomia_5h = timedelta(((estoque+gap_estoque*5)/meta_dia)/24) if meta_dia > 0 else timedelta(0)
                    #autonomia_6h = timedelta(((estoque+gap_estoque*6)/meta_dia)/24) if meta_dia > 0 else timedelta(0)
                    emoji_atual = emoji(autonomia_atual)
                    emoji_1h = emoji(autonomia_1h)
                    emoji_2h = emoji(autonomia_2h)
                    emoji_3h = emoji(autonomia_3h)
                    emoji_4h = emoji(autonomia_4h)
                    emoji_5h = emoji(autonomia_5h)
                    #emoji_6h = emoji(autonomia_6h)
                    print(f'[ON] Analise Cen. CCT - Unidade {unidade} com autonomia de: {autonomia_5h}.')
                    if autonomia_5h < timedelta(1/24):
                        mensagem_gatilho = []
                        mensagem_gatilho.append(f'📍 *GATILHO - Analise Cenário de {unidade}*')
                        mensagem_gatilho.append(f'Estoque {estoque} ton (Cargas {cargas} | Dens. {round(carga_med,2)})')
                        mensagem_gatilho.append(f'Entrega [3h]: {entrega_med} ton (PP {entrega_pp_med} | FF {entrega_ff_med})\nMoagem META dia {round(meta_dia*24)} ton (hora {round(meta_dia)})')
                        mensagem_gatilho.append(f'Check-In [3h]: {round(soma_entrega_3h[unidade]/3)} | Dens.PP {round(dens_pp/1000,1)} ton')
                        mensagem_gatilho.append(f'Moagem [3h]: {round(moagem_med[unidade])} ton/h')
                        mensagem_gatilho.append(f'GAP de {gap_estoque} ton')
                        mensagem_gatilho.append(f'{emoji_atual} Autonomia [0h]: {str(autonomia_atual)[:7] if autonomia_atual > timedelta(0) else "*Falta de cana!*"}\n{emoji_1h} Auto. [1h]: {str(autonomia_1h)[:7] if autonomia_1h > timedelta(0) else "*Falta de cana!*"}\n{emoji_2h} Auto. [2h]: {str(autonomia_2h)[:7] if autonomia_2h > timedelta(0) else "*Falta de cana!*"}\n{emoji_3h} Auto. [3h]: {str(autonomia_3h)[:7] if autonomia_3h > timedelta(0) else "*Falta de cana!*"}\n{emoji_4h} Auto. [4h]: {str(autonomia_4h)[:7] if autonomia_4h > timedelta(0) else "*Falta de cana!*"}\n{emoji_5h} Auto. [5h]: {str(autonomia_5h)[:7] if autonomia_5h > timedelta(0) else "*Falta de cana!*"}')#\n{emoji_6h} Auto. [6h]: {str(autonomia_6h)[:7] if autonomia_6h > timedelta(0) else "*Falta de cana!*"}')
                        if datetime.now().hour in [11,12,13,18,19,20,2,3,4]:
                            mensagem_gatilho.append(f'\n_Por favor responder nesse contato com a análise de cenário da {unidade} dentro de 1 hora e 40 minutos [até as {(datetime.now()+timedelta(hours=1, minutes=42)).strftime("%d/%m %H:%M")}]_\nNOTA: Acrescentamos 40 minutos devido período de refeição.')
                        else:
                            mensagem_gatilho.append(f'\n_Por favor responder nesse contato com a análise de cenário da {unidade} dentro de 1 hora [até as {(datetime.now()+timedelta(hours=1, minutes=2)).strftime("%d/%m %H:%M")}]._')
                        mensagem_gatilho.append(f'\n*NOTA!* No cabeçalho da mensagem deve conter exatamente: *ANÁLISE DE CENÁRIO*')
                        mensagem_gatilho = '\n'.join(mensagem_gatilho)
                        mensagem_resumo_gatilho = []
                        mensagem_resumo_gatilho = f'📍 *Gatilho {unidade}* | Autonomia [5]: {emoji_5h} {str(autonomia_5h)[:7] if autonomia_5h > timedelta(0) else "*Falta de cana!*"}'
                        # Envio para Focal e Gestão
                        turno_atual = 'C' if datetime.now().hour < 7 else 'A' if datetime.now().hour < 15 else 'B' if datetime.now().hour < 23 else 'C'
                        e_centro_sul = True if unidade in ['BONF','LEME','PASSA','CAAR','RBRIL','CONTI','MORRO','JUN','VALER','LPRAT','SELIS'] else False
                        gat_foc = {
                            'A':'19 99667-9285',
                            'B':'16 99634-0907',
                            'C':'19 97126-1795'}
                        gat_gest = {
                            'A':{
                                True:'14 99858-3973', # Centro Sul
                                False:'14 99858-3973', # Polo Leste
                                },
                            'B':{
                                True:'19 99744-5293',
                                False:'16 99643-1910',
                                },
                            'C':{
                                True:'19 99682-3186',
                                False:'19 99682-3186',
                                }}
                        contato, tipo_contato = verificar_tipo_de_contato(gat_foc[turno_atual]) #FOCAL
                        gravar_em_banco_para_envio([('DEBUG_CCT_Analise_Cenario', datetime.now(), contato, tipo_contato, mensagem_resumo_gatilho, '')])
                        contato, tipo_contato = verificar_tipo_de_contato(gat_gest[turno_atual][e_centro_sul]) #GESTOR
                        gravar_em_banco_para_envio([('DEBUG_CCT_Analise_Cenario', datetime.now(), contato, tipo_contato, mensagem_resumo_gatilho, '')])
                        # Envio para grupo CCT
                        contato, tipo_contato = verificar_tipo_de_contato('11 96320-8908') #Beck
                        gravar_em_banco_para_envio([('DEBUG_CCT_Analise_Cenario', datetime.now(), contato, tipo_contato, mensagem_resumo_gatilho, '')])
                        contato, tipo_contato = verificar_tipo_de_contato('19 99744-1803') # Jeverson
                        gravar_em_banco_para_envio([('DEBUG_CCT_Analise_Cenario', datetime.now(), contato, tipo_contato, mensagem_resumo_gatilho, '')])
                        contato, tipo_contato = verificar_tipo_de_contato('19 97120-7715') #Gisele
                        gravar_em_banco_para_envio([('DEBUG_CCT_Analise_Cenario', datetime.now(), contato, tipo_contato, mensagem_resumo_gatilho, '')])
                        # Envio para a torre CCT
                        for contato_torre_cct in torre_cct[unidade].split(';'):
                            contato, tipo_contato = verificar_tipo_de_contato(contato_torre_cct)
                            gravar_em_banco_para_envio([('CCT_Analise_Cenario', datetime.now(), contato, tipo_contato, mensagem_gatilho, '')])
                    else: 
                        #print(f'Autonomia de unidade: {unidade} OK [{autonomia_6h}]')
                        mensagem_a = f'Autonomia {unidade} OK! [{emoji_5h} {str(autonomia_5h)[:7]}] - Estoque {estoque} ton (Cargas {cargas} | Dens. {round(carga_med,2)}) - Entrega [3h]: {entrega_med} ton (PP {entrega_pp_med} | FF {entrega_ff_med})\nMoagem META dia {round(meta_dia*24)} ton (hora {round(meta_dia)}) - Check-In [3h]: {round(soma_entrega_3h[unidade]/3)} | Dens.PP {round(dens_pp/1000,1)} ton - Moagem [3h]: {round(moagem_med[unidade])} ton/h'
                        print(mensagem_a)
                        #contato, tipo_contato = verificar_tipo_de_contato('BOT CIA - CCT')
                        #gravar_em_banco_para_envio([('CCT_Analise_Cenario',datetime.now(),contato, tipo_contato, mensagem_a, '')])
                except Exception as exectp_und: 
                    print(f'\n\nErro em unidade: {unidade}, exceção:\n{exectp_und}\n')
            else: print(f'[OFF] Analise Cen. CCT - Unidade {unidade} não teve moagem na última hora.')
    except: print('Erro em analise cenário CCT')

##### Previsibilidade DF
def verificar_previsibilidade_df():
    # >>>>> Parametros
    unidade_fuso = ['CAARAPÓ','RIO BRILHANTE','PASSATEMPO']
    RETIRAR_FRENTES = '|'.join(['489', '492', '493', '494', '797','139']) #DESC_GRUPO_EQUIPAMENTO
    frentes_PMA = {
            'BARRA':'BAR',
            'BENALCOOL':'BEN',
            'BONFIM':'BON',
            'CAARAPÓ':'CAA',
            'CAARAPO':'CAA',
            'CONTINENTAL':'CNT',
            'COSTA PINTO':'COP',
            'DESTIVALE':'DES',
            'DIAMANTE':'DIA',
            'GASA':'GAS',
            'IPAUSSU':'IPA',
            'JATAÍ':'JAT',
            'JATAI':'JAT',
            'JUNQUEIRA':'JUN',
            'LAGOA DA PRATA':'LPT',
            'LEME':'LEM',
            'MUNDIAL':'MUN',
            'PARAISO':'UPA',
            'PARAÍSO':'UPA',
            'PASSATEMPO':'PTP',
            'RAFARD':'RAF',
            'SERRA':'SER',
            'RIO BRILHANTE':'RBR',
            'SANTA CÂNDIDA':'USC',
            'SANTA CANDIDA':'USC',
            'SANTA ELISA':'SEL',
            'SÃO FRANCISCO':'USF',
            'UMB':'UMB',
            'UNIVALEM':'UNI',
            'VALE DO ROSARIO':'VRO',
            'VALE DO ROSÁRIO':'VRO',
            'ZANIN':'ZAN'}

    # Apontamentos não contabilizados na DF
    nao_conta_df = pd.read_excel(verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\CIA Analytics - Manutenção\Operações Não Conta DF.xlsx'))
    lista_nao_conta_df = list(nao_conta_df['Códigos'])

    # >>>>> Grupo operativo dos apontamentos
    grupo_apt = pd.read_excel(verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\CIA Analytics - BOT CIA\Extrator\Suporte\Grupo_Apontamento.xlsx'))
    def sobrescrever_descricao(row):
        if row['CD_OPERACAO'] in lista_nao_conta_df:
            return 'PERDIDA*'
        return row['DESC_GRUPO_OPERAC']
    grupo_apt['DESC_GRUPO_OPERAC'] = grupo_apt.apply(sobrescrever_descricao, axis=1)

    # >>>>> Colhedoras CCT
    cd_cct = pd.read_excel(verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\CIA Analytics - BOT CIA\Extrator\CCT\CD_Hora.xlsx'))
    cd_cct = cd_cct[((cd_cct.DT_LOCAL >= datetime.now()-timedelta(hours=datetime.now().hour, minutes=datetime.now().minute+1)) 
            & (~cd_cct.DESC_UNIDADE.isin(unidade_fuso)))
            | ((cd_cct.DT_LOCAL >= datetime.now()-timedelta(hours=datetime.now().hour+1, minutes=datetime.now().minute+1))
            & (cd_cct.DESC_UNIDADE.isin(unidade_fuso)))]
    cd_cct = cd_cct[~cd_cct.DESC_GRUPO_EQUIPAMENTO.str.contains(RETIRAR_FRENTES)]
    def calculo_df_cct(row):
        manutencao = row[(row.DESC_GRUPO_OPERAC == 'MANUTENCAO')].VL_HR_OPERACIONAIS.sum()
        operacao = row[(row.CD_OPERACAO != 213)].VL_HR_OPERACIONAIS.sum()
        return round(((operacao - manutencao) / operacao)*100,2)
    cd_cct = cd_cct.merge(grupo_apt[['CD_OPERACAO', 'DESC_GRUPO_OPERAC']], on='CD_OPERACAO', how='left')
    DF_CD_CCT = cd_cct.groupby(['DESC_UNIDADE']).apply(calculo_df_cct)

    # >>>>> Base do PMA
    base_pma = pd.read_excel(verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\CIA Analytics - BOT CIA\Extrator\Manutencao\PMA\PMA.xlsx'))
    base_pma.sort_values(by='DS_DESCRICAO_TRABALHO', inplace=True)
    unidade_fuso_frente = ['RBR','PTP','CAA']
    base_pma = base_pma[(base_pma.DS_STATUS != "Recolhido") & (base_pma.DS_STATUS != "Concluído")]
    base_pma['DH_PREVISAO_LIBERACAO'] = base_pma['DH_PREVISAO_LIBERACAO'].apply(lambda x: x if x > datetime.now() - timedelta(hours=1) else "Sem Previsão")
    base_pma.DH_PREVISAO_LIBERACAO = base_pma.DH_PREVISAO_LIBERACAO.fillna('Sem Previsão')
    base_pma = base_pma.drop(base_pma[(base_pma.CD_FRENTE.str.contains('-MU-')) & (base_pma.NM_MODELO_EQUIPAMENTO=='TRANSBORDO')].index)

    def pegar_prazos(row):
        if row['DH_PREVISAO_LIBERACAO'] == 'Sem Previsão':
            return pd.Series([timedelta(1/24), timedelta(1/24), timedelta(1/24), timedelta(1/24), timedelta(1/24)])
        def motor_prazo(prazo_total):
            prazo = prazo_total
            prazo1, prazo2, prazo3, prazo4, prazo5 = timedelta(0), timedelta(0), timedelta(0), timedelta(0), timedelta(0)
            if prazo > timedelta(1/24): prazo1,prazo = timedelta(1/24), prazo-timedelta(hours=1)
            elif prazo > timedelta(0) and prazo < timedelta(1/24): prazo1,prazo = prazo, timedelta(0)
            if prazo > timedelta(1/24): prazo2,prazo = timedelta(1/24),prazo-timedelta(hours=1)
            elif prazo > timedelta(0) and prazo < timedelta(1/24): prazo2, prazo = prazo, timedelta(0)
            if prazo > timedelta(1/24): prazo3,prazo = timedelta(1/24),prazo-timedelta(hours=1)
            elif prazo > timedelta(0) and prazo < timedelta(1/24): prazo3,prazo = prazo,timedelta(0)
            if prazo > timedelta(1/24): prazo4,prazo = timedelta(1/24),prazo-timedelta(hours=1)
            elif prazo > timedelta(0) and prazo < timedelta(1/24): prazo4,prazo = prazo,timedelta(0)
            if prazo > timedelta(1/24): prazo5,prazo = timedelta(1/24),prazo-timedelta(hours=1)
            elif prazo > timedelta(0) and prazo < timedelta(1/24): prazo5,prazo = prazo,timedelta(0)
            return pd.Series([prazo1,prazo2,prazo3,prazo4,prazo5])
        # CASO FUSO DIFERENTE ['RBR','PTP','CAA']
        if row['CD_FRENTE'][:3] in unidade_fuso_frente and (row['DH_PREVISAO_LIBERACAO'] - (prazo_h-timedelta(hours=1))) > timedelta(0):
            return motor_prazo((row['DH_PREVISAO_LIBERACAO'] - (prazo_h-timedelta(hours=1))))
        # CASO NORMAL
        elif (row['DH_PREVISAO_LIBERACAO'] - prazo_h) > timedelta(0):
            return motor_prazo((row['DH_PREVISAO_LIBERACAO'] - (prazo_h)))
        # Prazo informado já vencido
        else: return pd.Series([timedelta(0), timedelta(0), timedelta(0), timedelta(0), timedelta(0)])
    # Pega os prazos das manutenções se ainda não forem atingidos
    prazo_h = datetime.now() #-timedelta(minutes=datetime.now().minute, seconds=datetime.now().second+.001)
    base_pma[['PRAZO_1h','PRAZO_2h','PRAZO_3h','PRAZO_4h','PRAZO_5h']] = base_pma.apply(pegar_prazos, axis=1)
    base_pma['CD_APT'] = base_pma.DS_OPERACAO.str.split(' - ').str.get(0)
    base_pma = base_pma[base_pma.CD_APT.isin(list(str(f) for f in grupo_apt[grupo_apt.DESC_GRUPO_OPERAC=='MANUTENCAO'].CD_OPERACAO.values))]

    # LÓGICA FUTURA COLHEDORAS CCT:
    def calculo_df_cct_futuro_cd(row):
        # Primeira hora
        manutencao_pma = base_pma[(base_pma['CD_FRENTE'].str[-3:]==str(row.name[-3:][1][-3:])) & (base_pma.NM_MODELO_EQUIPAMENTO.str.contains('COLHE|Colhe|CD'))].PRAZO_1h.sum().total_seconds()/3600
        manutencao = row[(row.DESC_GRUPO_OPERAC == 'MANUTENCAO')].VL_HR_OPERACIONAIS.sum()/3600 + manutencao_pma
        operacao = (row[(row.CD_OPERACAO != 213)].VL_HR_OPERACIONAIS.sum()/3600) + manutencao_pma + len(set(row.CD_EQUIPAMENTO)) * 1
        prev1 = round(((operacao - manutencao) / operacao)*100,2)
        # Segunda hora
        manutencao_pma += base_pma[(base_pma['CD_FRENTE'].str[-3:]==str(row.name[-3:][1][-3:])) & (base_pma.NM_MODELO_EQUIPAMENTO.str.contains('COLHE|Colhe|CD'))].PRAZO_2h.sum().total_seconds()/3600
        manutencao = row[(row.DESC_GRUPO_OPERAC == 'MANUTENCAO')].VL_HR_OPERACIONAIS.sum()/3600 + manutencao_pma
        operacao = (row[(row.CD_OPERACAO != 213)].VL_HR_OPERACIONAIS.sum()/3600) + manutencao_pma + len(set(row.CD_EQUIPAMENTO)) * 2
        prev2 = round(((operacao - manutencao) / operacao)*100,2)
        # Terceira hora
        manutencao_pma += base_pma[(base_pma['CD_FRENTE'].str[-3:]==str(row.name[-3:][1][-3:])) & (base_pma.NM_MODELO_EQUIPAMENTO.str.contains('COLHE|Colhe|CD'))].PRAZO_3h.sum().total_seconds()/3600
        manutencao = row[(row.DESC_GRUPO_OPERAC == 'MANUTENCAO')].VL_HR_OPERACIONAIS.sum()/3600 + manutencao_pma
        operacao = (row[(row.CD_OPERACAO != 213)].VL_HR_OPERACIONAIS.sum()/3600) + manutencao_pma + len(set(row.CD_EQUIPAMENTO)) * 3
        prev3 = round(((operacao - manutencao) / operacao)*100,2)
        # Quarta hora
        manutencao_pma += base_pma[(base_pma['CD_FRENTE'].str[-3:]==str(row.name[-3:][1][-3:])) & (base_pma.NM_MODELO_EQUIPAMENTO.str.contains('COLHE|Colhe|CD'))].PRAZO_4h.sum().total_seconds()/3600
        manutencao = row[(row.DESC_GRUPO_OPERAC == 'MANUTENCAO')].VL_HR_OPERACIONAIS.sum()/3600 + manutencao_pma
        operacao = (row[(row.CD_OPERACAO != 213)].VL_HR_OPERACIONAIS.sum()/3600) + manutencao_pma + len(set(row.CD_EQUIPAMENTO)) * 4
        prev4 = round(((operacao - manutencao) / operacao)*100,2)
        # Quinta hora
        manutencao_pma += base_pma[(base_pma['CD_FRENTE'].str[-3:]==str(row.name[-3:][1][-3:])) & (base_pma.NM_MODELO_EQUIPAMENTO.str.contains('COLHE|Colhe|CD'))].PRAZO_5h.sum().total_seconds()/3600
        manutencao = row[(row.DESC_GRUPO_OPERAC == 'MANUTENCAO')].VL_HR_OPERACIONAIS.sum()/3600 + manutencao_pma
        operacao = (row[(row.CD_OPERACAO != 213)].VL_HR_OPERACIONAIS.sum()/3600) + manutencao_pma + len(set(row.CD_EQUIPAMENTO)) * 5
        prev5 = round(((operacao - manutencao) / operacao)*100,2)
        return pd.Series([prev1,prev2,prev3,prev4,prev5])
    DF_CD_CCT_FUT = cd_cct.groupby(['DESC_UNIDADE']).apply(calculo_df_cct_futuro_cd)
    DF_CD_CCT_FUT.rename(columns={0:'CD_MO_1h',1:'CD_MO_2h',2:'CD_MO_3h',3:'CD_MO_4h',4:'CD_MO_5h'}, inplace=True)
    # >>>>> Transbordos CCT
    while True:
        try:
            tb_gr_cct = pd.read_excel(verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\CIA Analytics - BOT CIA\Extrator\CCT\Transbordo.xlsx'))
            tb_gr_cct = tb_gr_cct[~tb_gr_cct.DESC_GRUPO_EQUIPAMENTO.str.contains(RETIRAR_FRENTES)]
            break
        except: 
            print('Erro em base Transbordo.xlsx')
            sleep(1)
            pass
    # Filtrar grunner
    is_grunner = pd.read_excel(verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\CIA Analytics - BOT CIA\Extrator\Suporte\Cadastro_Grunners_CCT.xlsx'))
    is_grunner = list(is_grunner.cd_equipto)
    #tb_gr_cct = tb_gr_cct.merge(grupo_apt[['CD_OPERACAO', 'DESC_GRUPO_OPERAC']], on='CD_OPERACAO', how='left')
    tb_cct = tb_gr_cct[~tb_gr_cct.CD_EQUIPAMENTO.isin(is_grunner)]
    tb_cct.DT_LOCAL = pd.to_datetime(tb_cct.DT_LOCAL) + pd.to_timedelta(tb_cct.HR_LOCAL, unit='h')
    tb_cct = tb_cct[((tb_cct.DT_LOCAL >= datetime.now()-timedelta(hours=datetime.now().hour, minutes=datetime.now().minute+1)) 
            & (~tb_cct.DESC_UNIDADE.isin(unidade_fuso)))
            | ((tb_cct.DT_LOCAL >= datetime.now()-timedelta(hours=datetime.now().hour+1, minutes=datetime.now().minute+1))
            & (tb_cct.DESC_UNIDADE.isin(unidade_fuso)))]
    tb_cct = tb_cct.merge(grupo_apt[['CD_OPERACAO', 'DESC_GRUPO_OPERAC']], on='CD_OPERACAO', how='left')
    DF_TB_CCT = tb_cct.groupby(['DESC_UNIDADE']).apply(calculo_df_cct)

    # LÓGICA FUTURA TRANSBORDOS CCT:
    def calculo_df_cct_futuro_tb(row):
        # Primeira hora
        manutencao_pma = base_pma[(base_pma['CD_FRENTE'].str[-3:]==str(row.name[-3:][1][-3:])) & (base_pma.NM_MODELO_EQUIPAMENTO.str.contains('TRANS|4x4|TRATOR|TT'))].PRAZO_1h.sum().total_seconds()/3600
        manutencao = row[(row.DESC_GRUPO_OPERAC == 'MANUTENCAO')].VL_HR_OPERACIONAIS.sum()/3600 + manutencao_pma
        operacao = (row[(row.CD_OPERACAO != 213)].VL_HR_OPERACIONAIS.sum()/3600) + manutencao_pma + len(set(row.CD_EQUIPAMENTO)) * 1
        prev1 = round(((operacao - manutencao) / operacao)*100,2)
        # Segunda hora
        manutencao_pma += base_pma[(base_pma['CD_FRENTE'].str[-3:]==str(row.name[-3:][1][-3:])) & (base_pma.NM_MODELO_EQUIPAMENTO.str.contains('TRANS|4x4|TRATOR|TT'))].PRAZO_2h.sum().total_seconds()/3600
        manutencao = row[(row.DESC_GRUPO_OPERAC == 'MANUTENCAO')].VL_HR_OPERACIONAIS.sum()/3600 + manutencao_pma
        operacao = (row[(row.CD_OPERACAO != 213)].VL_HR_OPERACIONAIS.sum()/3600) + manutencao_pma + len(set(row.CD_EQUIPAMENTO)) * 2
        prev2 = round(((operacao - manutencao) / operacao)*100,2)
        # Terceira hora
        manutencao_pma += base_pma[(base_pma['CD_FRENTE'].str[-3:]==str(row.name[-3:][1][-3:])) & (base_pma.NM_MODELO_EQUIPAMENTO.str.contains('TRANS|4x4|TRATOR|TT'))].PRAZO_3h.sum().total_seconds()/3600
        manutencao = row[(row.DESC_GRUPO_OPERAC == 'MANUTENCAO')].VL_HR_OPERACIONAIS.sum()/3600 + manutencao_pma
        operacao = (row[(row.CD_OPERACAO != 213)].VL_HR_OPERACIONAIS.sum()/3600) + manutencao_pma + len(set(row.CD_EQUIPAMENTO)) * 3
        prev3 = round(((operacao - manutencao) / operacao)*100,2)
        # Quarta hora
        manutencao_pma += base_pma[(base_pma['CD_FRENTE'].str[-3:]==str(row.name[-3:][1][-3:])) & (base_pma.NM_MODELO_EQUIPAMENTO.str.contains('TRANS|4x4|TRATOR|TT'))].PRAZO_4h.sum().total_seconds()/3600
        manutencao = row[(row.DESC_GRUPO_OPERAC == 'MANUTENCAO')].VL_HR_OPERACIONAIS.sum()/3600 + manutencao_pma
        operacao = (row[(row.CD_OPERACAO != 213)].VL_HR_OPERACIONAIS.sum()/3600) + manutencao_pma + len(set(row.CD_EQUIPAMENTO)) * 4
        prev4 = round(((operacao - manutencao) / operacao)*100,2)
        # Quinta hora
        manutencao_pma += base_pma[(base_pma['CD_FRENTE'].str[-3:]==str(row.name[-3:][1][-3:])) & (base_pma.NM_MODELO_EQUIPAMENTO.str.contains('TRANS|4x4|TRATOR|TT'))].PRAZO_5h.sum().total_seconds()/3600
        manutencao = row[(row.DESC_GRUPO_OPERAC == 'MANUTENCAO')].VL_HR_OPERACIONAIS.sum()/3600 + manutencao_pma
        operacao = (row[(row.CD_OPERACAO != 213)].VL_HR_OPERACIONAIS.sum()/3600) + manutencao_pma + len(set(row.CD_EQUIPAMENTO)) * 5
        prev5 = round(((operacao - manutencao) / operacao)*100,2)
        return pd.Series([prev1,prev2,prev3,prev4,prev5])
    DF_TB_CCT_FUT = tb_cct.groupby(['DESC_UNIDADE']).apply(calculo_df_cct_futuro_tb)
    DF_TB_CCT_FUT.rename(columns={0:'TB_MO_1h',1:'TB_MO_2h',2:'TB_MO_3h',3:'TB_MO_4h',4:'TB_MO_5h'}, inplace=True)

    # >>>>> Base da Produção
    base_plantio = pd.read_excel(verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\CIA Analytics - BOT CIA\Extrator\PLANTIO\Plantio_Hora.xlsx'))
    base_plantio = base_plantio[~base_plantio.DESC_GRUPO_EQUIPAMENTO.str.contains(RETIRAR_FRENTES)]
    base_plantio['DT_LOCAL'] = pd.to_datetime(base_plantio.DT_LOCAL) + pd.to_timedelta(base_plantio.HR_LOCAL, unit='h')
    base_plantio = base_plantio[((base_plantio.DT_LOCAL >= datetime.now()-timedelta(hours=datetime.now().hour, minutes=datetime.now().minute+1)) 
            & (~base_plantio.DESC_UNIDADE.isin(unidade_fuso)))
            | ((base_plantio.DT_LOCAL >= datetime.now()-timedelta(hours=datetime.now().hour+1, minutes=datetime.now().minute+1))
            & (base_plantio.DESC_UNIDADE.isin(unidade_fuso)))]
    # -->> Base com dados Colhedoras da MUDA
    cd_mu = base_plantio[(base_plantio.DESC_GRUPO_EQUIPAMENTO.str.contains('-MU-')) & (base_plantio.FG_TP_EQUIPAMENTO == 1)]
    cd_mu = cd_mu.merge(grupo_apt[['CD_OPERACAO', 'DESC_GRUPO_OPERAC']], on='CD_OPERACAO', how='left')
    def calculo_df_prod(row):
        manutencao = row[(row.DESC_GRUPO_OPERAC=='MANUTENCAO')].VL_HR_OPERACIONAIS.sum()/3600
        operacao = row[(row.DESC_GRUPO_OPERAC.isin(['PRODUTIVA','MANUTENCAO','AUXILIAR']))].VL_HR_OPERACIONAIS.sum()/3600
        return round(((operacao - manutencao) / operacao)*100,2)
    DF_CD_MU = cd_mu.groupby(['DESC_UNIDADE']).apply(calculo_df_prod)

    # -->> Base com dados Plantadoras PL
    tt_pl = base_plantio[(base_plantio.DESC_GRUPO_EQUIPAMENTO.str.contains('-PL-')) & (base_plantio.FG_TP_EQUIPAMENTO == 40)]
    tt_pl = tt_pl.merge(grupo_apt[['CD_OPERACAO', 'DESC_GRUPO_OPERAC']], on='CD_OPERACAO', how='left')
    DF_TT_PL = tt_pl.groupby(['DESC_UNIDADE']).apply(calculo_df_prod)

    # Lógica para calcular DF futura na Produção
    def calculo_df_prod_futuro(row):
        if row.name[-3:][1][:-3] in unidade_fuso_frente: # unidade_fuso_frente = ['RBR','PTP','CAA']
            corte_3h = datetime.now()-timedelta(hours=4.8)
        else: corte_3h = datetime.now()-timedelta(hours=3.8)
        # Primeira hora
        horas_futuro = 1
        manutencao_pma = base_pma[base_pma['CD_FRENTE'].str[-3:]==str(row.name[-3:][1][-3:])].PRAZO_1h.sum().total_seconds()/3600
        manutencao = (row[(row.DESC_GRUPO_OPERAC == 'MANUTENCAO')].VL_HR_OPERACIONAIS.sum()/3600) + manutencao_pma
        operacao = (((row[(row.DT_LOCAL >= corte_3h) & (row.DESC_GRUPO_OPERAC.isin(['PRODUTIVA','MANUTENCAO','AUXILIAR']))].VL_HR_OPERACIONAIS.sum()/3600)/3) * horas_futuro) + (row[(row.DESC_GRUPO_OPERAC.isin(['PRODUTIVA','MANUTENCAO','AUXILIAR']))].VL_HR_OPERACIONAIS.sum()/3600) + manutencao_pma
        prev1 = round(((operacao - manutencao) / operacao)*100,2) if (operacao+manutencao) > 0 else np.nan
        # Segunda hora
        horas_futuro = 2
        manutencao_pma = (base_pma[base_pma['CD_FRENTE'].str[-3:]==str(row.name[-3:][1][-3:])].PRAZO_1h.sum().total_seconds()/3600) + (base_pma[base_pma['CD_FRENTE'].str[-3:]==str(row.name[-3:][1][-3:])].PRAZO_2h.sum().total_seconds()/3600)
        manutencao = (row[(row.DESC_GRUPO_OPERAC == 'MANUTENCAO')].VL_HR_OPERACIONAIS.sum()/3600) + manutencao_pma
        operacao = (((row[(row.DT_LOCAL >= corte_3h) & (row.DESC_GRUPO_OPERAC.isin(['PRODUTIVA','MANUTENCAO','AUXILIAR']))].VL_HR_OPERACIONAIS.sum()/3600)/3) * horas_futuro) + (row[(row.DESC_GRUPO_OPERAC.isin(['PRODUTIVA','MANUTENCAO','AUXILIAR']))].VL_HR_OPERACIONAIS.sum()/3600) + manutencao_pma
        prev2 = round(((operacao - manutencao) / operacao)*100,2) if (operacao+manutencao) > 0 else np.nan
        # Terceira hora
        horas_futuro = 3
        manutencao_pma = (base_pma[base_pma['CD_FRENTE'].str[-3:]==str(row.name[-3:][1][-3:])].PRAZO_1h.sum().total_seconds()/3600) + (base_pma[base_pma['CD_FRENTE'].str[-3:]==str(row.name[-3:][1][-3:])].PRAZO_2h.sum().total_seconds()/3600) + (base_pma[base_pma['CD_FRENTE'].str[-3:]==str(row.name[-3:][1][-3:])].PRAZO_3h.sum().total_seconds()/3600)
        manutencao = (row[(row.DESC_GRUPO_OPERAC == 'MANUTENCAO')].VL_HR_OPERACIONAIS.sum()/3600) + manutencao_pma
        operacao = (((row[(row.DT_LOCAL >= corte_3h) & (row.DESC_GRUPO_OPERAC.isin(['PRODUTIVA','MANUTENCAO','AUXILIAR']))].VL_HR_OPERACIONAIS.sum()/3600)/3) * horas_futuro) + (row[(row.DESC_GRUPO_OPERAC.isin(['PRODUTIVA','MANUTENCAO','AUXILIAR']))].VL_HR_OPERACIONAIS.sum()/3600) + manutencao_pma
        prev3 = round(((operacao - manutencao) / operacao)*100,2) if (operacao+manutencao) > 0 else np.nan
        # Quarta hora
        horas_futuro = 4
        manutencao_pma = (base_pma[base_pma['CD_FRENTE'].str[-3:]==str(row.name[-3:][1][-3:])].PRAZO_1h.sum().total_seconds()/3600) + (base_pma[base_pma['CD_FRENTE'].str[-3:]==str(row.name[-3:][1][-3:])].PRAZO_2h.sum().total_seconds()/3600) + (base_pma[base_pma['CD_FRENTE'].str[-3:]==str(row.name[-3:][1][-3:])].PRAZO_3h.sum().total_seconds()/3600) + (base_pma[base_pma['CD_FRENTE'].str[-3:]==str(row.name[-3:][1][-3:])].PRAZO_4h.sum().total_seconds()/3600)
        manutencao = (row[(row.DESC_GRUPO_OPERAC == 'MANUTENCAO')].VL_HR_OPERACIONAIS.sum()/3600) + manutencao_pma
        operacao = (((row[(row.DT_LOCAL >= corte_3h) & (row.DESC_GRUPO_OPERAC.isin(['PRODUTIVA','MANUTENCAO','AUXILIAR']))].VL_HR_OPERACIONAIS.sum()/3600)/3) * horas_futuro) + (row[(row.DESC_GRUPO_OPERAC.isin(['PRODUTIVA','MANUTENCAO','AUXILIAR']))].VL_HR_OPERACIONAIS.sum()/3600) + manutencao_pma
        prev4 = round(((operacao - manutencao) / operacao)*100,2) if (operacao+manutencao) > 0 else np.nan
        # Quinta hora
        horas_futuro = 5
        manutencao_pma = (base_pma[base_pma['CD_FRENTE'].str[-3:]==str(row.name[-3:][1][-3:])].PRAZO_1h.sum().total_seconds()/3600) + (base_pma[base_pma['CD_FRENTE'].str[-3:]==str(row.name[-3:][1][-3:])].PRAZO_2h.sum().total_seconds()/3600) + (base_pma[base_pma['CD_FRENTE'].str[-3:]==str(row.name[-3:][1][-3:])].PRAZO_3h.sum().total_seconds()/3600) + (base_pma[base_pma['CD_FRENTE'].str[-3:]==str(row.name[-3:][1][-3:])].PRAZO_4h.sum().total_seconds()/3600) + (base_pma[base_pma['CD_FRENTE'].str[-3:]==str(row.name[-3:][1][-3:])].PRAZO_5h.sum().total_seconds()/3600)
        manutencao = (row[(row.DESC_GRUPO_OPERAC == 'MANUTENCAO')].VL_HR_OPERACIONAIS.sum()/3600) + manutencao_pma
        operacao = (((row[(row.DT_LOCAL >= corte_3h) & (row.DESC_GRUPO_OPERAC.isin(['PRODUTIVA','MANUTENCAO','AUXILIAR']))].VL_HR_OPERACIONAIS.sum()/3600)/3) * horas_futuro) + (row[(row.DESC_GRUPO_OPERAC.isin(['PRODUTIVA','MANUTENCAO','AUXILIAR']))].VL_HR_OPERACIONAIS.sum()/3600) + manutencao_pma
        prev5 = round(((operacao - manutencao) / operacao)*100,2) if (operacao+manutencao) > 0 else np.nan
        return pd.Series([prev1,prev2,prev3,prev4,prev5])
    # Previsão DF para Colhedoras Muda
    DF_CD_MU_FUT = cd_mu.groupby(['DESC_UNIDADE']).apply(calculo_df_prod_futuro)
    DF_CD_MU_FUT.rename(columns={0:'CD_MU_1h',1:'CD_MU_2h',2:'CD_MU_3h',3:'CD_MU_4h',4:'CD_MU_5h'}, inplace=True)
    # Previsão DF para Tratores Platio
    DF_TT_PL_FUT = tt_pl.groupby(['DESC_UNIDADE']).apply(calculo_df_prod_futuro)
    DF_TT_PL_FUT.rename(columns={0:'TT_PL_1h',1:'TT_PL_2h',2:'TT_PL_3h',3:'TT_PL_4h',4:'TT_PL_5h'}, inplace=True)

    ### DF's da hora atual + Futura de cada Operação
    if len(DF_CD_MU) > 0:
        DF_CD_MU = pd.merge(DF_CD_MU.to_frame('CD_MU'), DF_CD_MU_FUT, 
                left_index=True, right_index=True, how='outer')
    if len(DF_TT_PL) > 0:
        DF_TT_PL = pd.merge(DF_TT_PL.to_frame('TT_PL'), DF_TT_PL_FUT, 
                left_index=True, right_index=True, how='outer')
    if len(DF_CD_CCT) > 0:
        DF_CD_CCT = pd.merge(DF_CD_CCT.to_frame('CD_MO'), DF_CD_CCT_FUT, 
                left_index=True, right_index=True, how='outer')
    if len(DF_TB_CCT) > 0:
        DF_TB_CCT = pd.merge(DF_TB_CCT.to_frame('TB_MO'), DF_TB_CCT_FUT, 
                left_index=True, right_index=True, how='outer')
    # >>>> Relação de Abreviação Frente para Código Unidade
    FrenteUnidade = pd.read_excel(verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\CIA Analytics - Manutenção\Frentes.xlsx'))

    # Detalhamento do Grupo Operativo
    def somar_operacao(row):
        return round(row.VL_HR_OPERACIONAIS.sum()/3600,2)
    # Detalhamento Grupo Operativo
    DF_CD_MU_DET = cd_mu.groupby(['DESC_UNIDADE','DESC_GRUPO_EQUIPAMENTO']).apply(calculo_df_prod)
    DF_TT_PL_DET = tt_pl.groupby(['DESC_UNIDADE','DESC_GRUPO_EQUIPAMENTO']).apply(calculo_df_prod)
    DF_CD_CCT_DET = cd_cct.groupby(['DESC_UNIDADE','DESC_GRUPO_EQUIPAMENTO']).apply(calculo_df_cct)
    DF_TB_CCT_DET = tb_cct.groupby(['DESC_UNIDADE','DESC_GRUPO_EQUIPAMENTO']).apply(calculo_df_cct)

    # Histórico de ocorrência PMA
    hist_pma = pd.read_excel(verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\CIA Analytics - BOT CIA\Extrator\Manutencao\PMA\HIST_PMA.xlsx'))
    hist_pma['Duração'] = hist_pma.DH_FIM_OS - hist_pma.DH_INICIO_OPERACAO
    hist_pma.rename(columns={'DH_INICIO_OPERACAO':'Início Manut.','CD_EQUIPAMENTO':'Frota','DS_DESCRICAO_TRABALHO':'Descrição'}, inplace=True)
    if 'NM_MODELO_EQUIPAMENTO' not in hist_pma.columns:
        hist_pma['NM_MODELO_EQUIPAMENTO'] = ''
    hist_pma = hist_pma[['NM_MODELO_EQUIPAMENTO','CD_FRENTE','Frota','Início Manut.','Descrição']]
    hist_pma = hist_pma.sort_values(by=['Início Manut.'])
    hist_pma['Descrição'] = hist_pma['Descrição'].str[:140]

    ### Lógica dos Grunners:
    # LÓGICA GRUNNERS CCT:
    gr_cct = tb_gr_cct[tb_gr_cct.CD_EQUIPAMENTO.isin(is_grunner)]
    # Fim Filtrar grunner
    gr_cct.DT_LOCAL = pd.to_datetime(gr_cct.DT_LOCAL) + pd.to_timedelta(gr_cct.HR_LOCAL, unit='h')
    gr_cct = gr_cct[((gr_cct.DT_LOCAL >= datetime.now()-timedelta(hours=datetime.now().hour, minutes=datetime.now().minute+1)) 
            & (~gr_cct.DESC_UNIDADE.isin(unidade_fuso)))
            | ((gr_cct.DT_LOCAL >= datetime.now()-timedelta(hours=datetime.now().hour+1, minutes=datetime.now().minute+1))
            & (gr_cct.DESC_UNIDADE.isin(unidade_fuso)))]
    gr_cct = gr_cct.merge(grupo_apt[['CD_OPERACAO', 'DESC_GRUPO_OPERAC']], on='CD_OPERACAO', how='left')
    DF_GR_CCT = gr_cct.groupby(['DESC_UNIDADE']).apply(calculo_df_cct)
    # Futuro:
    DF_GR_CCT_FUT = gr_cct.groupby(['DESC_UNIDADE']).apply(calculo_df_cct_futuro_tb)
    DF_GR_CCT_FUT.rename(columns={0:'GR_MO_1h',1:'GR_MO_2h',2:'GR_MO_3h',3:'GR_MO_4h',4:'GR_MO_5h'}, inplace=True)
    DF_GR_CCT_DET = gr_cct.groupby(['DESC_UNIDADE','DESC_GRUPO_EQUIPAMENTO']).apply(calculo_df_cct)
    if len(DF_GR_CCT) > 0:
        DF_GR_CCT = pd.merge(DF_GR_CCT.to_frame('GR_MO'), DF_GR_CCT_FUT, 
                left_index=True, right_index=True, how='outer')
    # Meta GR CCT
    meta_gr_cct = pd.read_excel(verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\CIA Analytics - Manutenção\Metas DF.xlsx'), sheet_name='GR_CCT')
    meta_gr_cct = meta_gr_cct[['Unidade',datetime.now().month]]
    # fim LÓGICA GRUNNERS CCT

    # >>>>> Carregaento de Metas
    meta_cd_cct = pd.read_excel(verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\CIA Analytics - Manutenção\Metas DF.xlsx'), sheet_name='CD_CCT')
    meta_cd_cct = meta_cd_cct[['Unidade',datetime.now().month]]

    meta_tt_cct = pd.read_excel(verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\CIA Analytics - Manutenção\Metas DF.xlsx'), sheet_name='TT_CCT')
    meta_tt_cct = meta_tt_cct[['Unidade',datetime.now().month]]

    meta_tt_pl = pd.read_excel(verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\CIA Analytics - Manutenção\Metas DF.xlsx'), sheet_name='TT_PL')
    meta_tt_pl = meta_tt_pl[['Unidade',datetime.now().month]]

    meta_cd_mu = pd.read_excel(verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\CIA Analytics - Manutenção\Metas DF.xlsx'), sheet_name='CD_MU')
    meta_cd_mu = meta_cd_mu[['Unidade',datetime.now().month]]

    # Lògicas:
    def frente_para_horas_por_frota(DF_dataframe_DET, frente_alvo): # Retorna dict[frota] = [produtivaF,auxiliarF,manutencaoF]
        frotas_frente = list(set([f[0] for f in list(DF_dataframe_DET.loc[frente_alvo].keys())]))
        frota_por_horas = {}
        for frota in frotas_frente:
            manutencaoF = DF_dataframe_DET.loc[frente_alvo,frota]['MANUTENCAO'] if 'MANUTENCAO' in DF_dataframe_DET.loc[frente_alvo,frota].keys() else np.float64(0.0)
            produtivaF = DF_dataframe_DET.loc[frente_alvo,frota]['PRODUTIVA'] if 'PRODUTIVA' in DF_dataframe_DET.loc[frente_alvo,frota].keys() else np.float64(0.0)
            auxiliarF = DF_dataframe_DET.loc[frente_alvo,frota]['AUXILIAR'] if 'AUXILIAR' in DF_dataframe_DET.loc[frente_alvo,frota].keys() else np.float64(0.0)
            if '-MO-' in frente_alvo: 
                perdidaF = (DF_dataframe_DET.loc[frente_alvo,frota]['PERDIDA'] if 'PERDIDA' in DF_dataframe_DET.loc[frente_alvo,frota].keys() else np.float64(0.0)) \
                    + (DF_dataframe_DET.loc[frente_alvo,frota]['PERDIDA*'] if 'PERDIDA*' in DF_dataframe_DET.loc[frente_alvo,frota].keys() else np.float64(0.0))
            else: perdidaF = (DF_dataframe_DET.loc[frente_alvo,frota]['PERDIDA'] if 'PERDIDA' in DF_dataframe_DET.loc[frente_alvo,frota].keys() else np.float64(0.0))
            frota_por_horas[str(frota)] = [produtivaF,auxiliarF,manutencaoF,perdidaF]
        return frota_por_horas

    def carregar_meta_frente(operacao_alvo):
        if operacao_alvo == 'TT_PL':
            df_meta_atual = meta_tt_pl
        elif operacao_alvo == 'CD_MU':
            df_meta_atual = meta_cd_mu
        elif operacao_alvo == 'CD_MO':
            df_meta_atual = meta_cd_cct
        elif operacao_alvo == 'TB_MO':
            df_meta_atual = meta_tt_cct
        elif operacao_alvo == 'GR_MO':
            df_meta_atual = meta_gr_cct
        meta = FrenteUnidade.merge(df_meta_atual, left_on='Unidades', right_on='Unidade', how='inner')
        meta = meta.iloc[:, [0,-1]]
        try:
            registro = pd.DataFrame([['USF',meta[meta.Frentes=='RAF'].values[0][1]]], columns=meta.columns)
            meta = pd.concat([registro,meta],axis=0,ignore_index=True)
        except: pass
        return meta

    def descritivo_df(meta_df,atual_df,prejecao_df,dataframe_alvo,unidade,nivel_mesagem):
        operacao_alvo = dataframe_alvo.columns[0]
        if operacao_alvo == 'TT_PL':
            mensagem_operacao = 'Tratores do Plantio'
        elif operacao_alvo == 'CD_MO':
            mensagem_operacao = 'Colhedoras do CCT'
        elif operacao_alvo == 'TB_MO':
            mensagem_operacao = 'Transbordos do CCT'
        elif operacao_alvo == 'CD_MU':
            mensagem_operacao = 'Colhedoras do Muda'
        elif operacao_alvo == 'GR_MO':
            mensagem_operacao = 'Grunners do CCT'
        if meta_df > atual_df and meta_df > prejecao_df:
            return f'⚠️ *{unidade} {nivel_mesagem}*: Atualmente a DF (Disponibilidade Física) *{mensagem_operacao}* está ABAIXO da meta.\nNa projeção de 5 horas futuras ainda demonstra cenário ABAIXO do indicador.\n- DF atual {atual_df}%\n- Projeção DF +5h {prejecao_df}%\n- Meta DF {meta_df}%'
        elif meta_df > atual_df and meta_df < prejecao_df:
            return f'⚠️ *{unidade} {nivel_mesagem}*: Atualmente a DF (Disponibilidade Física) *{mensagem_operacao}* está ABAIXO da meta.\nNa projeção de 5 horas futuras temos indícios de RETOMADA do indicador.\n- DF atual {atual_df}%\n- Projeção DF +5h {prejecao_df}%\n- Meta DF {meta_df}%'
        elif meta_df < atual_df and meta_df > prejecao_df:
            return f'⚠️ *{unidade} {nivel_mesagem}*: Atualmente a DF (Disponibilidade Física) *{mensagem_operacao}* está ACIMA da meta.\nPorém na projeção de 5 horas futuras temos indícios de QUEDA do indicador.\n- DF atual {atual_df}%\n- Projeção DF +5h {prejecao_df}%\n- Meta DF {meta_df}%'
        else: 'Analise Previsibilidade DF'

        #-->> 1º Gráfico Previsibilidade DF
    def criar_grafico_df(DF_dataframe, unidade_alvo):
        def custom_formatter(x, pos):
            if x == 0:
                return '0%'
            elif x == 100:
                return '100%'
            elif 0 < x < 1:
                return '{:.2f}%'.format(x * 100)
            else:
                return '{:.0f}%'.format(x)
        operacao_alvo = DF_dataframe.columns[0] # Pega o nome da operação
        if operacao_alvo == 'TT_PL':
            meta_ = meta_tt_pl
        elif operacao_alvo == 'CD_MO':
            meta_ = meta_cd_cct
        elif operacao_alvo == 'TB_MO':
            meta_ = meta_tt_cct
        elif operacao_alvo == 'CD_MU':
            meta_ = meta_cd_mu
        elif operacao_alvo == 'GR_MO':
            meta_ = meta_gr_cct
        unidade = frentes_PMA[unidade_alvo]
        # meta_unidade = round(meta_[meta_.Unidade==unidade].values[0][1]*100,1)
        meta_unidade = round(meta[meta['Frentes']==unidade].values[0][1]*100,2)
        fig, axs = plt.subplots(figsize=(18, 4))
        prev_df_x = ['Atual','+1h','+2h','+3h','+4h','+5h']
        prev_df_y = DF_dataframe[DF_dataframe.index.get_level_values(0) == unidade_alvo].values[0]
        axs.axhline(y=meta_unidade, color='red', linestyle='-')
        axs.annotate(f'META\n {meta_unidade}%', xy=(prev_df_x[-1], meta_unidade), xytext=(prev_df_x[-1], meta_unidade - 14), color='red', fontsize=15)

        axs.plot(prev_df_x, prev_df_y, color='#781E77', linewidth=4)
        
        for i, v in enumerate(DF_dataframe[DF_dataframe.index.get_level_values(0) == unidade_alvo].values[0]):
            if v == 100:
                axs.text(i, v, str(v)[:3]+'%', ha='center', va='bottom', color='w', fontsize='24',
                    bbox={'facecolor': '#781E77', 'edgecolor': '#781E77', 'pad': 1})
            else:
                axs.text(i, v, str(v)[:4]+'%', ha='center', va='bottom', color='w', fontsize='24',
                        bbox={'facecolor': '#781E77', 'edgecolor': '#781E77', 'pad': 1})
        axs.spines['top'].set_visible(False)
        axs.spines['right'].set_visible(False)
        axs.spines['bottom'].set_visible(False)
        axs.spines['left'].set_visible(False)
        axs.tick_params(axis='y', length=0)
        axs.set_yticks([0, 25, 50, 75, 100])
        axs.set_yticklabels(axs.get_yticks(), fontweight='bold')
        axs.yaxis.set_major_formatter(ticker.FuncFormatter(custom_formatter))
        axs.yaxis.set_tick_params(labelsize='26')
        axs.xaxis.set_tick_params(labelsize='26')
        plt.tight_layout()
        plt.savefig(f'analise_df\\grafico1_{unidade_alvo}.png')
        return os.path.abspath(f'analise_df\\grafico1_{unidade_alvo}.png')
    # USO: grafico_df_path = criar_grafico_df(DF_CD_CCT,'COP-MO-001')

    #-->> 2º Gráfico Compilado manutenção / Frota
    def criar_grafico_df_frente(DF_dataframe, unidade_alvo):
        def custom_formatter(x, pos):
            if x == 0:
                return '0%'
            elif x == 100:
                return '100%'
            elif 0 < x < 1:
                return '{:.2f}%'.format(x * 100)
            else:
                return '{:.0f}%'.format(x)
        operacao_alvo = DF_dataframe.columns[0] # Pega o nome da operação
        if operacao_alvo == 'TT_PL':
            frentes_df = DF_TT_PL_DET
            meta_ = meta_tt_pl
        elif operacao_alvo == 'CD_MO':
            frentes_df = DF_CD_CCT_DET
            meta_ = meta_cd_cct
        elif operacao_alvo == 'TB_MO':
            frentes_df = DF_TB_CCT_DET
            meta_ = meta_tt_cct
        elif operacao_alvo == 'CD_MU':
            frentes_df = DF_CD_MU_DET
            meta_ = meta_cd_mu
        elif operacao_alvo == 'GR_MO':
            frentes_df = DF_GR_CCT_DET
            meta_ = meta_gr_cct
        fig, axs = plt.subplots(figsize=(18, 4))
        unidade = frentes_PMA[unidade_alvo]
        # meta_unidade = round(meta_[meta_.Unidade==unidade].values[0][1]*100,1)
        meta_unidade = round(meta[meta['Frentes']==unidade].values[0][1]*100,2)
        frentes = [frent[-3:] for frent in frentes_df.loc[unidade_alvo].index]
        df_s = [df_ for df_ in frentes_df.loc[unidade_alvo].values]
        axs.axhline(y=meta_unidade, color='red', linestyle='-', zorder=1)
        #axs.annotate(f'META\n {meta_unidade}%', xy=(1, axs.get_xticks()[-1]), xytext=(1, axs.get_xticks()[-1] - 14), color='red', fontsize=15)
        axs.bar(frentes, df_s, color='#781E77', zorder=2)
        for i, v in enumerate(df_s):
            if v == 100: axs.text(i, v, '100%', ha='center', va='top', color='white', fontsize='22')
            elif v > 0: axs.text(i, v, f'{str(v)[:4]}%', ha='center', va='top', color='white', fontsize='22')
            else: pass
        axs.spines['top'].set_visible(False)
        axs.spines['right'].set_visible(False)
        axs.spines['bottom'].set_visible(False)
        axs.spines['left'].set_visible(False)
         # Formatação
        axs.tick_params(axis='y', length=0)
        axs.set_yticks([0, 25, 50, 75, 100])
        axs.set_yticklabels(axs.get_yticks(), fontweight='bold')
        axs.yaxis.set_major_formatter(ticker.FuncFormatter(custom_formatter))
        axs.yaxis.set_tick_params(labelsize='24')
        axs.xaxis.set_tick_params(labelsize='24')
        plt.tight_layout()
        plt.savefig(f'analise_df\\grafico2_{unidade_alvo}.png')
        return os.path.abspath(f'analise_df\\grafico2_{unidade_alvo}.png')
    # USO: criar_graficos_operacao(DF_CD_CCT, 'BARRA')

    #-->> 3º Gráfico Linha temporal por grupo operativo da frota
    def gerar_linha_do_tempo(dataframe_alvo,unidade_alvo):
        def soma_tempo(row):
            return row['VL_HR_OPERACIONAIS'].sum()
        dict_grupo = {'PRODUTIVA':"#2FBD50",
                    'PERDIDA':"#BD2431",'PERDIDA*':"#932431",
                    'MANUTENCAO':"#BF960F",
                    'AUXILIAR':"#4D5CBD"}
        operacao_alvo = dataframe_alvo.columns[0] # Pega o nome da operação
        if operacao_alvo == 'TT_PL':
            df_alvo = tt_pl
        elif operacao_alvo == 'CD_MO':
            df_alvo = cd_cct
        elif operacao_alvo == 'TB_MO':
            df_alvo = tb_cct
        elif operacao_alvo == 'CD_MU':
            df_alvo = cd_mu
        elif operacao_alvo == 'GR_MO':
            df_alvo = gr_cct
        else: print('Qual operação você se refere??? Opções: TT_PL, CD_MO, TB_MO, GR_MO e CD_MU')
        compilado_manut_frota = df_alvo[(df_alvo.DESC_UNIDADE==unidade_alvo) & (df_alvo.DESC_GRUPO_OPERAC=='MANUTENCAO')].groupby('CD_EQUIPAMENTO')['VL_HR_OPERACIONAIS'].sum()
        compilado_manut_frota = compilado_manut_frota.sort_values(ascending=False)[:7]
        fig, axs = plt.subplots(figsize=(30, 17))
        lista_frota_frente = []
        escala = df_alvo[(df_alvo.DESC_UNIDADE==unidade_alvo)].groupby('CD_EQUIPAMENTO').apply(soma_tempo).sort_values().values[-1]
        escala = int(timedelta(round((escala+3600)/3600/24,1)).total_seconds() // 3600)
        for id, frota in enumerate(compilado_manut_frota.sort_values(ascending=False)[:7].index):
            n=0
            lista_frota_frente.append(f'{frota}\n{df_alvo[df_alvo.CD_EQUIPAMENTO==frota].DESC_GRUPO_EQUIPAMENTO.values[0]}')
            for index, row in df_alvo[df_alvo.CD_EQUIPAMENTO==frota].sort_values(by='DT_LOCAL').iterrows():
                axs.barh(str(frota), row.VL_HR_OPERACIONAIS, left=n, color=dict_grupo[row.DESC_GRUPO_OPERAC])
                n += row.VL_HR_OPERACIONAIS
        axs.spines['top'].set_visible(False)
        axs.spines['right'].set_visible(False)
        axs.spines['bottom'].set_visible(False)
        axs.spines['left'].set_visible(False)
        axs.set_xticks(axs.get_xticks())
        axs.yaxis.set_tick_params(labelsize='28')
        axs.xaxis.set_tick_params(labelsize='28')
        axs.set_xticks([f*3600 for f in np.arange(escala)])
        axs.set_xticklabels([f'{int((timedelta(hours=int(f)).total_seconds() // 3600))}H' for f in np.arange(escala)])
        axs.set_yticklabels(lista_frota_frente)
        plt.tight_layout()
        plt.savefig(f'analise_df\\grafico3_{unidade_alvo}.png')
        return os.path.abspath(f'analise_df\\grafico3_{unidade_alvo}.png')
    # USO: gerar_linha_do_tempo(DF_CD_CCT,'BARRA')

    #-->> 4º Gráfico[TABELA] Previsão frotas
    def gerar_tabela_previsoes(dataframe_alvo,unidade_alvo):
        def gerar_imagem(dataframe_alvo, nome_a_ser_salvo):
            def destacar_sem_previsao(valor):
                if valor == "Sem Previsão":
                    return 'background-color: #FFB6C1'  # Vermelho claro
                return ''
            estilo_centralizado = {'selector': 'th, td',
                                'props': [('text-align', 'center'), ('font-family', 'sans-serif')]}
            estilo_cabecalho = {'selector': 'th',
                                'props': [('font-weight', 'bold'), ('font-family', 'sans-serif'),
                                        ('background-color', '#781E77'), ('color', 'white')]}     
            estilo_largura_dupla = {
                                    'selector': '.col2',
                                    'props': [('width', '600px')],
                                    'table': [
                                        {
                                            'selector': '.col2',
                                            'props': [('color', 'black')]  # Define a cor do texto para preto
                                        }
                                    ],
                                    'data': [
                                        {
                                            'selector': '.col2',
                                            'props': [('background-color', '#FFFFFF')]  # Define a cor de fundo padrão
                                        }
                                    ],
                                    'highlight': [
                                        {
                                            'selector': '.col2',
                                            'props': [('background-color', '#FFB6C1')]  # Define a cor de destaque para Sem Previsão
                                        }
                                    ]
                                }
            df_style = dataframe_alvo.style \
                .hide(axis="index") \
                .set_table_styles([estilo_centralizado, estilo_cabecalho,estilo_largura_dupla]) \
                .applymap(destacar_sem_previsao, subset=pd.IndexSlice[:, 'Previsão Liberação'])
            html = df_style.to_html()
            # display(HTML(html)) from IPython.display import display, HTML
            options = {
                'format': 'png',
                'quiet': ''}
                #'width': 900}
            img_path = r'C:\CIAANALYTICS\Python 3\imgkit\wkhtmltopdf\bin\wkhtmltoimage.exe'
            imgkit.from_string(html, nome_a_ser_salvo, options=options, config=imgkit.config(wkhtmltoimage=img_path))
        operacao_alvo = dataframe_alvo.columns[0] # Pega o nome da operação
        cod_und_f = frentes_PMA[unidade_alvo]
        if operacao_alvo == 'TT_PL':
            df_alvo = base_pma[(base_pma.NM_MODELO_EQUIPAMENTO.str.contains('TRANS|4x4|TRATOR|TT')) & (base_pma.CD_FRENTE.str[0:6] == f'{cod_und_f}-PL')]
        elif operacao_alvo == 'CD_MO':
            df_alvo = base_pma[(base_pma.NM_MODELO_EQUIPAMENTO.str.contains('COLHE|Colhe|CD')) & (base_pma.CD_FRENTE.str[0:6] == f'{cod_und_f}-MO')]
        elif operacao_alvo == 'TB_MO':
            df_alvo = base_pma[(base_pma.NM_MODELO_EQUIPAMENTO.str.contains('TRANS|4x4|TRATOR|TT')) & (base_pma.CD_FRENTE.str[0:6] == f'{cod_und_f}-MO') & (~base_pma.CD_EQUIPAMENTO.isin(is_grunner))]
        elif operacao_alvo == 'GR_MO':
            df_alvo = base_pma[(base_pma.NM_MODELO_EQUIPAMENTO.str.contains('TRANS|4x4|TRATOR|TT')) & (base_pma.CD_FRENTE.str[0:6] == f'{cod_und_f}-MO') & (base_pma.CD_EQUIPAMENTO.isin(is_grunner))]
        elif operacao_alvo == 'CD_MU':
            df_alvo = base_pma[(base_pma.NM_MODELO_EQUIPAMENTO.str.contains('COLHE|Colhe|CD')) & (base_pma.CD_FRENTE.str[0:6] == f'{cod_und_f}-MU')]
        # gerando tabela da unidade, operação e frente
        df_alvo = df_alvo[['CD_EQUIPAMENTO','CD_APT','DS_DESCRICAO_TRABALHO','DH_INICIO_OPERACAO','DH_PREVISAO_LIBERACAO']]
        df_alvo['DH_INICIO_OPERACAO'] = df_alvo['DH_INICIO_OPERACAO'].dt.strftime('%d/%m %H:%M')
        df_alvo['DH_PREVISAO_LIBERACAO'] = df_alvo['DH_PREVISAO_LIBERACAO'].apply(lambda x: x.strftime('%d/%m %H:%M') if isinstance(x, datetime) else 'Sem Previsão')
        df_alvo = df_alvo.rename(columns={'NM_MODELO_EQUIPAMENTO':'Modelo','CD_EQUIPAMENTO':'Frota','CD_APT':'Apontamento','DS_DESCRICAO_TRABALHO':'Descrição','DH_PREVISAO_LIBERACAO':'Previsão Liberação','DH_INICIO_OPERACAO':'Início Operação'})
        gerar_imagem(df_alvo, f'analise_df\\tabela1_{unidade_alvo}.png')
        return os.path.abspath(f'analise_df\\tabela1_{unidade_alvo}.png')
    # USO: gerar_tabela_previsoes(DF_CD_CCT, 'BARRA')

    #-->> 5º Gráfico[TABELA] Histórico Corretiva Frotas
    def gerar_tabela_hist_pma(dataframe_alvo,unidade_alvo):
        def gerar_imagem(dataframe_alvo, nome_a_ser_salvo):
            estilo_centralizado = {'selector': 'th, td',
                                'props': [('text-align', 'center'), ('font-family', 'sans-serif')]}
            estilo_cabecalho = {'selector': 'th',
                                'props': [('font-weight', 'bold'), ('font-family', 'sans-serif'),
                                        ('background-color', '#781E77'), ('color', 'white')]}     
            estilo_largura_dupla = {
                                    'selector': '.col2',
                                    'props': [('width', '800px')],
                                    'table': [
                                        {
                                            'selector': '.col2',
                                            'props': [('color', 'black')]  # Define a cor do texto para preto
                                        }
                                    ],
                                    'data': [
                                        {
                                            'selector': '.col2',
                                            'props': [('background-color', '#FFFFFF')]  # Define a cor de fundo padrão
                                        }
                                    ],
                                    'highlight': [
                                        {
                                            'selector': '.col2',
                                            'props': [('background-color', '#FFB6C1')]  # Define a cor de destaque para Sem Previsão
                                        }
                                    ]
                                }
            estilo_borda_preta = {
                'selector': 'td',
                'props': [('border', '1px solid black')]}
            estilo_largura_0 = {'selector': '.col0',
                                    'props': [('width', '130px')]}
            estilo_largura_1 = {'selector': '.col1',
                            'props': [('width', '130px')]}
            '''estilo_largura_3 = {'selector': '.col3',
                            'props': [('width', '100px')]}'''
            df_style = dataframe_alvo.style \
                .hide(axis="index") \
                .set_table_styles([estilo_centralizado, estilo_cabecalho,estilo_largura_dupla,estilo_borda_preta,estilo_largura_0,estilo_largura_1])
            html = df_style.to_html()
            # display(HTML(html)) from IPython.display import display, HTML
            options = {
                'format': 'png',
                'quiet': ''}
                #'width': 900}
            img_path = r'C:\CIAANALYTICS\Python 3\imgkit\wkhtmltopdf\bin\wkhtmltoimage.exe'
            imgkit.from_string(html, nome_a_ser_salvo, options=options, config=imgkit.config(wkhtmltoimage=img_path))
        operacao_alvo = dataframe_alvo.columns[0] # Pega o nome da operação
        cod_und_f = frentes_PMA[unidade_alvo]
        if operacao_alvo == 'TT_PL':
            df_alvo = hist_pma[(hist_pma.NM_MODELO_EQUIPAMENTO.str.contains('TRANS|4x4|TRATOR|TT')) & (hist_pma['CD_FRENTE'].str.contains(f'{cod_und_f}-PL'))]
        elif operacao_alvo == 'CD_MO':
            df_alvo = hist_pma[(hist_pma.NM_MODELO_EQUIPAMENTO.str.contains('COLHE|Colhe|CD')) & (hist_pma['CD_FRENTE'].str.contains(f'{cod_und_f}-MO'))]
        elif operacao_alvo == 'TB_MO':
            df_alvo = hist_pma[(hist_pma.NM_MODELO_EQUIPAMENTO.str.contains('TRANS|4x4|TRATOR|TT')) & (hist_pma['CD_FRENTE'].str.contains(f'{cod_und_f}-MO')) & (~hist_pma.Frota.isin(is_grunner))]
        elif operacao_alvo == 'GR_MO':
            df_alvo = hist_pma[(hist_pma.NM_MODELO_EQUIPAMENTO.str.contains('TRANS|4x4|TRATOR|TT')) & (hist_pma['CD_FRENTE'].str.contains(f'{cod_und_f}-MO')) & (hist_pma.Frota.isin(is_grunner))]
        elif operacao_alvo == 'CD_MU':
            df_alvo = hist_pma[(hist_pma.NM_MODELO_EQUIPAMENTO.str.contains('COLHE|Colhe|CD')) & (hist_pma['CD_FRENTE'].str.contains(f'{cod_und_f}-MU'))]
        # gerando tabela da unidade, operação e frente
        df_alvo.sort_values(by='Início Manut.', ascending=True, inplace=True)
        df_alvo['Início Manut.'] = df_alvo['Início Manut.'].dt.strftime('%d/%m %H:%M')
        #df_alvo['Duração'] = df_alvo['Duração'].dt.strftime('%H:%M')
        #df_alvo['Duração'] = df_alvo['Duração'].apply(lambda x: '{:02d}:{:02d}'.format(int(x.total_seconds() // 3600), int((x.total_seconds() % 3600) // 60)))
        gerar_imagem(df_alvo.iloc[:,2:], f'analise_df\\tabela2_{unidade_alvo}.png')
        return os.path.abspath(f'analise_df\\tabela2_{unidade_alvo}.png')
    # USO: gerar_tabela_hist_pma(DF_CD_CCT, 'BARRA')

    def montar_slides(dataframe_alvo, unidade_alvo):
        # Carregando moldes:
        operacao_alvo = dataframe_alvo.columns[0]
        if operacao_alvo in ['TT_PL','CD_MU']:
            slide1 = Image.open(r'\\CSCLSFSR01\Agricola$\Logistica Agroindustrial\CIA 22.23\11. Analytics\BOT CIA\0 - Logica Codigos\23 - Analise Previsibilidade DF\Molde\PROD1.png')
            slide2 = Image.open(r'\\CSCLSFSR01\Agricola$\Logistica Agroindustrial\CIA 22.23\11. Analytics\BOT CIA\0 - Logica Codigos\23 - Analise Previsibilidade DF\Molde\PROD2.png')
            slide3 = Image.open(r'\\CSCLSFSR01\Agricola$\Logistica Agroindustrial\CIA 22.23\11. Analytics\BOT CIA\0 - Logica Codigos\23 - Analise Previsibilidade DF\Molde\PROD3.png')
        elif operacao_alvo in ['CD_MO','TB_MO','GR_MO']:
            slide1 = Image.open(r'\\CSCLSFSR01\Agricola$\Logistica Agroindustrial\CIA 22.23\11. Analytics\BOT CIA\0 - Logica Codigos\23 - Analise Previsibilidade DF\Molde\CCT1.PNG')
            slide2 = Image.open(r'\\CSCLSFSR01\Agricola$\Logistica Agroindustrial\CIA 22.23\11. Analytics\BOT CIA\0 - Logica Codigos\23 - Analise Previsibilidade DF\Molde\CCT2.PNG')
            slide3 = Image.open(r'\\CSCLSFSR01\Agricola$\Logistica Agroindustrial\CIA 22.23\11. Analytics\BOT CIA\0 - Logica Codigos\23 - Analise Previsibilidade DF\Molde\CCT3.PNG')
        if operacao_alvo == 'TT_PL':
            texto_ = f'Plantadoras {frentes_PMA[unidade_alvo]}'
        elif operacao_alvo == 'CD_MU':
            texto_ = f'Colhedoras {frentes_PMA[unidade_alvo]}'
        elif operacao_alvo == 'CD_MO':
            texto_ = f'Colhedoras {frentes_PMA[unidade_alvo]}'
        elif operacao_alvo == 'TB_MO':
            texto_ = f'Transbordos {frentes_PMA[unidade_alvo]}'
        elif operacao_alvo == 'GR_MO':
            texto_ = f'Grunners {frentes_PMA[unidade_alvo]}'
        # Gerando gráficos:
        prev_df = Image.open(criar_grafico_df(dataframe_alvo, unidade_alvo))
        frentes_df = Image.open(criar_grafico_df_frente(dataframe_alvo, unidade_alvo))
        linha_tempo = Image.open(gerar_linha_do_tempo(dataframe_alvo, unidade_alvo))
        previsoes_pma = Image.open(gerar_tabela_previsoes(dataframe_alvo, unidade_alvo))
        hist_pma = Image.open(gerar_tabela_hist_pma(dataframe_alvo, unidade_alvo))
        # Transformando gráficos
        prev_df = prev_df.resize((687, 283))
        linha_tempo = linha_tempo.resize((1313, 729))
        frentes_df = frentes_df.resize((689, 284))
        previsoes_pma = previsoes_pma.resize((1400, previsoes_pma.size[1]))
        hist_pma = hist_pma.resize((1400, round((hist_pma.size[1]*1.1)) if round((hist_pma.size[1]*1.1)) < 735 else 735))
        slide3 = slide3.convert(hist_pma.mode)
        # Colando Gráficos em Moldes
        slide1.paste(prev_df, (28, 140))
        slide1.paste(frentes_df, (740, 137))
        slide1.paste(previsoes_pma, (28, 487))
        slide2.paste(linha_tempo, (31, 130))
        slide3.paste(hist_pma, (30, 132))
        # Inserindo dados na imagem
        ImageDraw.Draw(slide1).text((1105, 17), texto_, font=ImageFont.truetype("arial.ttf", 40), fill=(255, 255, 255))
        ImageDraw.Draw(slide2).text((1105, 17), texto_, font=ImageFont.truetype("arial.ttf", 40), fill=(255, 255, 255))
        ImageDraw.Draw(slide3).text((1105, 17), texto_, font=ImageFont.truetype("arial.ttf", 40), fill=(255, 255, 255))
        # Salvar Slides
        slide1.save(f'analise_df\\report\\slide1_{frentes_PMA[unidade_alvo]}{operacao_alvo}.png')
        slide2.save(f'analise_df\\report\\slide2_{frentes_PMA[unidade_alvo]}{operacao_alvo}.png')
        slide3.save(f'analise_df\\report\\slide3_{frentes_PMA[unidade_alvo]}{operacao_alvo}.png')
        return [os.path.abspath(f'analise_df\\report\\slide1_{frentes_PMA[unidade_alvo]}{operacao_alvo}.png'),os.path.abspath(f'analise_df\\report\\slide2_{frentes_PMA[unidade_alvo]}{operacao_alvo}.png'),os.path.abspath(f'analise_df\\report\\slide3_{frentes_PMA[unidade_alvo]}{operacao_alvo}.png')]

    def carregar_contatos_prev_df():
        while True:
            try:
                contatos_prev_df = pd.read_excel(r'\\CSCLSFSR01\Agricola$\Logistica Agroindustrial\CIA 22.23\11. Analytics\BOT CIA\Contatos\MANUT_Previsibilidade_DF.xlsx')
                contatos_prev_df = contatos_prev_df[contatos_prev_df.Ativado=='SIM']
                return contatos_prev_df
            except:
                sleep(1)
                print('Não conseguimos carregar contatos prev. DF')
                pass

    def carregar_controle_prev_df():
        # Chave: Unidade_TipoFrota_Operacao | Sequencia: GP, Sup e Ger
        try:
            caminho_arquivo = r"C:\CIAANALYTICS\1 - Producao\1 2 - Geracao_Mensagens\analise_df\report\controle_prev_df.json"
            with open(caminho_arquivo, "r") as arquivo:
                controle_prev_df = json.load(arquivo)
            # [(datetime.fromisoformat(f)) for f in controle_prev_df['LPT_TT_PL']]
            return controle_prev_df
        except:
            print('\n------>>>>> NÃO CONSEGUIMOS ATUALIZAR CHAVE DE CONTROLE!!!')
            return dict()

    def salvar_controle_prev_df(meu_dict_alvo):
        #controle_prev_df = carregar_controle_prev_df()
        # Chave: Unidade_TipoFrota_Operacao | Sequencia: GP, Sup e Ger
        def serializar_datetime(obj):
            if isinstance(obj, datetime):
                return obj.isoformat()
            raise TypeError(f"Tipo '{type(obj)}' não é serializável.")
        caminho_arquivo = r"C:\CIAANALYTICS\1 - Producao\1 2 - Geracao_Mensagens\analise_df\report\controle_prev_df.json"
        with open(caminho_arquivo, "w") as arquivo:
            json.dump(meu_dict_alvo, arquivo, default=serializar_datetime)

    def realizar_envio_previsibilidade_df(unidade,nivel): #Nível: (1 = Torre e GPM) (2 = 1 + Supervisor) (3 = 2 + Gerente)
        if unidade in frentes_PMA.keys() and frentes_PMA[unidade] in list(contatos_prev_df.Unidade):
            tipo_operacao = 'CCT' if 'MO' in base_operacao.columns[0] else 'PROD'
            # Mensagem texto e anexos
            escrita_Nivel = 'nível GPM' if nivel == 1 else 'nível SUP.' if nivel == 2 else 'nível Ger.'
            mensagem_intro = descritivo_df(meta_unidade,df_atual,df_5h,base_operacao,frentes_PMA[unidade],escrita_Nivel)
            imagens = montar_slides(base_operacao, unidade)
            # Destinos
            destinos_prev_df = contatos_prev_df[contatos_prev_df.Unidade==frentes_PMA[unidade]]
            lista_envio_prev_df = []
            lista_envio_prev_df.append(str(destinos_prev_df['Torre_Manut'].values[0]))
            for level in range(nivel):
                if str(destinos_prev_df[f'{tipo_operacao}_{level+1}'].values[0]) != 'nan':
                    lista_envio_prev_df.append(str(destinos_prev_df[f'{tipo_operacao}_{level+1}'].values[0]))
            lista_envio_prev_df = ';'.join(lista_envio_prev_df)
            # Mensagem para grupo de teste
            for destino in lista_envio_prev_df.split(';'):
                if len(str(destino)) > 3:
                    contato, tipo_contato = verificar_tipo_de_contato(str(destino))
                    gravar_em_banco_para_envio([('MANUT_Previsibilidade_DF',datetime.now(),contato, tipo_contato, mensagem_intro, '')])
                    for imagem in imagens:
                        gravar_em_banco_para_envio([('MANUT_Previsibilidade_DF',datetime.now(),contato, tipo_contato, '', imagem)])
                        pass

            # Output para console:
            escrita_nivel = 'Torre e GPM' if nivel == 1 else 'Torre, GPM e Sup.' if nivel == 2 else 'Torre, GPM, Sup. e Ger.'
            print(f'[MANUT_Prev_DF] -> Unidade {unidade} envio nível: {nivel} = {escrita_nivel}')
        else: print(f'[MANUT_Prev_DF] -> Unidade {unidade} não teve envio realizado por estar desativa') # Output para console

    contatos_prev_df = carregar_contatos_prev_df()

    lista_verificacao_prev_df = [f for f in [DF_CD_CCT,DF_TB_CCT,DF_CD_MU,DF_TT_PL, DF_GR_CCT] if len(f) > 0]
    if datetime.now().hour > 5 and datetime.now().hour < 21:
        for base_operacao in lista_verificacao_prev_df:
            controle_prev_df = carregar_controle_prev_df()
            for chave in controle_prev_df.keys():
                for id, momento in enumerate(controle_prev_df[chave]):
                    momento_acionamento = datetime.fromisoformat(momento)
                    if momento_acionamento < (datetime.now()-timedelta(hours=5)) and momento_acionamento.date() == datetime.now().date():
                        controle_prev_df[chave][id] = (datetime.now()-timedelta(days=2)).isoformat()
                        print(f'Estamos resetando nível {id} para {chave}')
        #print(base_operacao)
        #print(f'Iniciando análise Prev. DF de : {base_operacao.columns[0]}')
        meta = carregar_meta_frente(base_operacao.columns[0])
        for unidade in base_operacao.groupby(level=0):
            und_eq_op = f'{frentes_PMA[unidade[0]]}_{base_operacao.columns[0]}'
            # Código UND da unidade: unidade[0][0:3]
            # Código unidade: unidade[0]
            # DF Atual: unidade[1].values[0][0]
            # DF +5h: unidade[1].values[0][5]
            try:
                meta_unidade = round(meta[meta['Frentes']==frentes_PMA[unidade[0]]].values[0][1]*100,2)
            except:
                print(f'ATENÇÃO! Meta da unidade: {unidade} [{frentes_PMA[unidade[0]]}] operação: {base_operacao.columns[0][0]} - COM ERRO!!!')
                meta_unidade = 1
            df_atual = unidade[1].values[0][0]
            df_5h = unidade[1].values[0][5]
            #print(f'unidade: {unidade[0]} | DF: {df_atual}% | META: {meta_unidade}% | PROJ.[5H]: {df_5h}%')
            if meta_unidade > df_atual or meta_unidade > unidade[1].values[0][1]:
                print(f'--> Gatilho nível Gerência Manutenção para unidade {frentes_PMA[unidade[0]]}')
                if und_eq_op not in controle_prev_df.keys():
                    print(f'Unidade {frentes_PMA[unidade[0]]} sem registros passados de scalation.')
                    controle_prev_df[und_eq_op] = [(datetime.now()-timedelta(days=10)).isoformat(),(datetime.now()-timedelta(days=10)).isoformat(),(datetime.now()-timedelta(days=10)).isoformat()]
                else:
                    print(f'Unidade {frentes_PMA[unidade[0]]} já tem registrode scalation: {datetime.fromisoformat(controle_prev_df[und_eq_op][2])}')
            
                if datetime.fromisoformat(controle_prev_df[und_eq_op][2]).date() == datetime.now().date():
                    print(f'Scalation para a unidade {frentes_PMA[unidade[0]]} já realizada na data: {datetime.now().date()}')
                else:
                    print(f'Realizando scalation para a unidade {frentes_PMA[unidade[0]]}, momento: {datetime.now().date()}')
                    controle_prev_df[und_eq_op] = [datetime.now(),datetime.now(),datetime.now()]
                    # Realizar envio da mensagem
                    realizar_envio_previsibilidade_df(unidade[0],3)
                #
            elif meta_unidade*1.02 > unidade[1].values[0][2]:
                print(f'Gatilho nível Supervisão Manutenção para unidade {frentes_PMA[unidade[0]]}')
                #if frentes_PMA[unidade[0]] == 'LPT': 
                if und_eq_op not in controle_prev_df.keys():
                    print(f'Unidade {frentes_PMA[unidade[0]]} sem registros passados de scalation.')
                    controle_prev_df[und_eq_op] = [(datetime.now()-timedelta(days=10)).isoformat(),(datetime.now()-timedelta(days=10)).isoformat(),(datetime.now()-timedelta(days=10)).isoformat()]
                else:
                    print(f'Unidade {frentes_PMA[unidade[0]]} já tem registrode scalation: {datetime.fromisoformat(controle_prev_df[und_eq_op][1])}')
                    if datetime.fromisoformat(controle_prev_df[und_eq_op][1]).date() == datetime.now().date():
                        print(f'Scalation para a unidade {frentes_PMA[unidade[0]]} já realizada na data: {datetime.now().date()}')
                    else:
                        print(f'Realizando scalation para a unidade {frentes_PMA[unidade[0]]}, momento: {datetime.now().date()}')
                        controle_prev_df[und_eq_op] = [datetime.now(),datetime.now(),controle_prev_df[und_eq_op][2]]
                        # Realizar envio da mensagem
                        realizar_envio_previsibilidade_df(frentes_PMA[unidade[0]],2)
                #
            elif meta_unidade*1.02 > unidade[1].values[0][3]:
                print(f'Gatilho nível GPM para unidade {frentes_PMA[unidade[0]]}')
                #if frentes_PMA[unidade[0]] == 'LPT': 
                if und_eq_op not in controle_prev_df.keys():
                    print(f'Unidade {frentes_PMA[unidade[0]]} sem registros passados de scalation.')
                    controle_prev_df[und_eq_op] = [(datetime.now()-timedelta(days=10)).isoformat(),(datetime.now()-timedelta(days=10)).isoformat(),(datetime.now()-timedelta(days=10)).isoformat()]
                else:
                    print(f'Unidade {frentes_PMA[unidade[0]]} já tem registrode scalation: {datetime.fromisoformat(controle_prev_df[und_eq_op][0])}')
                    if datetime.fromisoformat(controle_prev_df[und_eq_op][0]).date() == datetime.now().date():
                        print(f'Scalation para a unidade {frentes_PMA[unidade[0]]} já realizada na data: {datetime.now().date()}')
                    else:
                        print(f'Realizando scalation para a unidade {frentes_PMA[unidade[0]]}, momento: {datetime.now().date()}')
                        controle_prev_df[und_eq_op] = [datetime.now(),controle_prev_df[und_eq_op][1],controle_prev_df[und_eq_op][2]]
                        # Realizar envio da mensagem
                        realizar_envio_previsibilidade_df(frentes_PMA[unidade[0]],1)
                #
            salvar_controle_prev_df(controle_prev_df)

#### PREPARO APOTNAMENTO
def gerar_mensagens_preparo():
    print('Inicio geração de apontamentos do Preparo.')
    def carregar_grupos_preparo():
        contatoPR = pd.read_excel(r'\\CSCLSFSR01\Agricola$\Logistica Agroindustrial\CIA 22.23\11. Analytics\BOT CIA\Contatos\PROD_Apontamento.xlsx', sheet_name='PR')
        contatoPR = contatoPR[contatoPR.Ativo=='SIM'].drop_duplicates().dropna()
        contatoPR = dict(zip(contatoPR.Frente, contatoPR.Grupo))
        return contatoPR
    def gerar_imagem_preparo(dataframe_alvo, nome_a_ser_salvo):
        estilo_colunas = [
                {'selector': '.col0', 'props': [('width', '150px')]},
                {'selector': '.col1', 'props': [('width', '140px')]},
                {'selector': '.col3', 'props': [('width', '210px')]},
                {'selector': '.col4', 'props': [('width', '210px')]}
            ]
        estilo_centralizado = {'selector': 'th, td',
                            'props': [('text-align', 'center'), ('font-family', 'sans-serif')]}
        estilo_cabecalho = {'selector': 'th',
                            'props': [('font-weight', 'bold'), ('font-family', 'sans-serif'),
                                    ('background-color', '#781E77'), ('color', 'white')]}     
        df_style = dataframe_alvo.style \
            .hide(axis="index") \
            .set_table_styles(estilo_colunas + [estilo_centralizado, estilo_cabecalho])
        html = df_style.to_html()
        #display(HTML(html)) 
        options = {'format': 'png','quiet': ''}
        caminho_robo = r'C:\CIAANALYTICS\Python 3\imgkit\wkhtmltopdf\bin\wkhtmltoimage.exe'
        img_path = caminho_robo #r'C:\Users\ciaanalytics\Downloads\Python3\imgkit\wkhtmltopdf\bin\wkhtmltoimage.exe'
        imgkit.from_string(html, nome_a_ser_salvo, options=options, config=imgkit.config(wkhtmltoimage=img_path))
        return os.path.abspath(nome_a_ser_salvo)
    # Base de dados
    com = pd.read_excel(verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\CIA Analytics - BOT CIA\Extrator\AGRON\agron_comunicacao.xlsx'))

    com = com[com['Frente associada'].str.contains('-PR-')]

    com = com[['Número do Equipamento','Frente associada','Atividade','Registro mais recente']]
    com = com.rename(columns={
            'Número do Equipamento':'Frota',
            'Frente associada':'Frente',
            'Atividade':'Apontamento',
            'Registro mais recente':'Última Comunicação'})

    com['Tempo sem comunicar'] = [datetime.now()-r for r in com['Última Comunicação']]

    def duracao_tempo(row):
        valor = row['Tempo sem comunicar']
        dias = valor.days
        hh,mm,ss = str(valor).split(' ')[-1].split('.')[0].split(':')
        return str((dias * 24) + int(hh)) + ":" + ':'.join([mm,ss])

    com['Tempo sem comunicar'] = com.apply(duracao_tempo, axis=1)
    com = com.sort_values(by=['Frente','Última Comunicação'], ascending=False)
    com['Última Comunicação'] = com['Última Comunicação'].dt.strftime('%d/%m/%Y %H:%M:%S')
    # Grupos Preparo
    GruposPR = carregar_grupos_preparo()
    # Geraçcao de imagens
    if not os.path.exists('Apontamento_Preparo'): os.mkdir('Apontamento_Preparo')

    for frente in com.Frente.unique():
        undCod = frente.split('-')[0]
        if undCod in GruposPR.keys():
            mensagem = f'🚜 *Tratores {frente}:* Apontamentos e Comunicação'
            ParaGrupoPR = GruposPR[frente.split('-')[0]]
            caminho_anexo = gerar_imagem_preparo(com[com.Frente==frente], f'Apontamento_Preparo\\{frente}.png')
            contato, tipo_contato = verificar_tipo_de_contato(ParaGrupoPR)
            gravar_em_banco_para_envio([('PROD_Apontamento_Preparo',datetime.now(),contato, tipo_contato, mensagem, caminho_anexo)])
        else: print(f'Unidade {undCod} sem grupo!')
    com['Última Comunicação'] = pd.to_datetime(com['Última Comunicação'], dayfirst=True)
    com_sem_dados = com[com['Última Comunicação'] < datetime.now()-timedelta(hours=6)].sort_values(by='Última Comunicação')
    mensagem = f'🚜 *Tratores Preparo:* Frotas a mais de 6 horas sem comunicar.'
    caminho_anexo = gerar_imagem_preparo(com_sem_dados, f'Apontamento_Preparo\\sem_dados.png')
    contato, tipo_contato = verificar_tipo_de_contato('19 99847-9246')
    gravar_em_banco_para_envio([('PROD_Apontamento_Preparo',datetime.now(),contato, tipo_contato, mensagem, caminho_anexo)])

#### Kronos APOTNAMENTO

def gerar_mensagens_kronos():
    print('Inicio geração de apontamentos do Kronos.')
    def carregar_grupos_kronos():
        contatoBT = pd.read_excel(r'\\CSCLSFSR01\Agricola$\Logistica Agroindustrial\CIA 22.23\11. Analytics\BOT CIA\Contatos\PROD_Apontamento.xlsx', sheet_name='BT')
        contatoBT = contatoBT[contatoBT.Ativo=='SIM'].drop_duplicates().dropna()
        contatoBT = dict(zip(contatoBT.Frente, contatoBT.Grupo))
        return contatoBT

    def carregar_grupos_preparo():
        contatoPR = pd.read_excel(r'\\CSCLSFSR01\Agricola$\Logistica Agroindustrial\CIA 22.23\11. Analytics\BOT CIA\Contatos\PROD_Apontamento.xlsx', sheet_name='PR')
        contatoPR = contatoPR[contatoPR.Ativo=='SIM'].drop_duplicates().dropna()
        contatoPR = dict(zip(contatoPR.Frente, contatoPR.Grupo))
        return contatoPR

    def carregar_grupos_HB():
        contatoHB = pd.read_excel(r'\\CSCLSFSR01\Agricola$\Logistica Agroindustrial\CIA 22.23\11. Analytics\BOT CIA\Contatos\PROD_Apontamento.xlsx', sheet_name='HB')
        contatoHB = contatoHB[contatoHB.Ativo=='SIM'].drop_duplicates().dropna()
        contatoHB = dict(zip(contatoHB.Frente, contatoHB.Grupo))
        return contatoHB

    def gerar_imagem_kronos(dataframe_alvo, nome_a_ser_salvo):
        estilo_colunas = [
                {'selector': '.col0', 'props': [('width', '150px')]},
                {'selector': '.col1', 'props': [('width', '140px')]},
                {'selector': '.col3', 'props': [('width', '210px')]},
                {'selector': '.col4', 'props': [('width', '210px')]}
            ]
        estilo_centralizado = {'selector': 'th, td',
                            'props': [('text-align', 'center'), ('font-family', 'sans-serif')]}
        estilo_cabecalho = {'selector': 'th',
                            'props': [('font-weight', 'bold'), ('font-family', 'sans-serif'),
                                    ('background-color', '#781E77'), ('color', 'white')]}     
        df_style = dataframe_alvo.style \
            .hide(axis="index") \
            .set_table_styles(estilo_colunas + [estilo_centralizado, estilo_cabecalho])
        html = df_style.to_html()
        #display(HTML(html)) 
        options = {'format': 'png','quiet': ''}
        caminho_robo = r'C:\CIAANALYTICS\Python 3\imgkit\wkhtmltopdf\bin\wkhtmltoimage.exe'
        img_path = caminho_robo #r'C:\Users\ciaanalytics\Downloads\Python3\imgkit\wkhtmltopdf\bin\wkhtmltoimage.exe' #
        imgkit.from_string(html, nome_a_ser_salvo, options=options, config=imgkit.config(wkhtmltoimage=img_path))
        return os.path.abspath(nome_a_ser_salvo)
    # Base SGPA3 MONITORAMENTO
    com = pd.read_excel(verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\CIA Analytics - BOT CIA\Extrator\SGPA3\monitoramento_sgpa3.xlsx'))
    com = com.dropna(subset='Registro mais recente')
    com['Registro mais recente'] = pd.to_datetime(com['Registro mais recente'], dayfirst=True, errors='coerce')
    com = com.dropna(subset='Registro mais recente')
    com = com[com['Frente associada'].str.contains('-BT-|-PR-|-HB-')]
    com = com[['Número do Equipamento','Atividade','Registro mais recente']]
    com = com.rename(columns={
            'Número do Equipamento':'Frota',
            'Atividade':'Apontamento',
            'Registro mais recente':'Último Registro'})
    com['Último Registro'] = com['Último Registro'].dt.strftime('%d/%m/%Y %H:%M:%S')
    # Base SGPA3 APONTAMENTO
    com_ap = pd.read_excel(verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\CIA Analytics - BOT CIA\Extrator\SGPA3\apontamento_sgpa3.xlsx'))
    com_ap = com_ap.sort_values(by='vlTempoSemComunicacao', ascending=False)
    com_ap = com_ap[['cdEquipamento','descGrupoEquipamento','vlTempoSemComunicacao']] #descTpEquipamento
    com_ap = com_ap.rename(columns={'cdEquipamento':'Frota', 'descGrupoEquipamento':'Frente', 'vlTempoSemComunicacao':'Tempo Sem Dados'})
    com_ap = com_ap[com_ap['Frente'].str.contains('-BT-|-PR-|-HB-')]
    # Unindo Apontamento e Monitoramento
    com = pd.merge(left=com_ap, right=com, on='Frota', how='left')
    com['TRAVA_DIAS'] = [int(str(row).split(' ')[0]) for row in com['Tempo Sem Dados']]
    com = com[com['TRAVA_DIAS'] < 90]
    com = com.drop(columns=['TRAVA_DIAS'])
    com = com[['Frota','Frente','Apontamento','Último Registro','Tempo Sem Dados']]
    com = com.sort_values(by='Tempo Sem Dados', ascending=False)
    com = com.dropna(subset='Apontamento')#.fillna('SEM REGISTRO*')

    # Grupos Preparo
    GruposBT = carregar_grupos_kronos()
    GruposPR = carregar_grupos_preparo()
    GruposHB = carregar_grupos_HB()
    # Geraçcao de imagens
    if not os.path.exists('Apontamento_Kronos'): os.mkdir('Apontamento_Kronos')
    if not os.path.exists('Apontamento_Preparo'): os.mkdir('Apontamento_Preparo')
    if not os.path.exists('Apontamento_Tratos'): os.mkdir('Apontamento_Tratos')

    comBT = com[com['Frente'].str.contains('-BT-')]
    for frente in comBT.Frente.unique():
        undCod = frente.split('-')[0]
        if undCod in GruposBT.keys():
            mensagem = f'🚜 *Tratores {frente}:* Apontamentos e Comunicação'
            ParaGrupoPR = GruposBT[frente.split('-')[0]]
            caminho_anexo = gerar_imagem_kronos(comBT[comBT.Frente==frente], f'Apontamento_Kronos\\{frente}.png')
            contato, tipo_contato = verificar_tipo_de_contato(ParaGrupoPR)
            gravar_em_banco_para_envio([('PROD_Apontamento_Kronos',datetime.now(),contato, tipo_contato, mensagem, caminho_anexo)])
        else: print(f'Unidade {undCod} -BT- sem grupo!')
    #mensagem = f'🚜 *Tratores Kronos:* TOP 40 frotas a mais tempo sem enviar dados.'
    #caminho_anexo = gerar_imagem_kronos(comBT.head(40), f'Apontamento_Kronos\\sem_dados.png')
    #contato, tipo_contato = verificar_tipo_de_contato('19 97165-8319')
    #gravar_em_banco_para_envio([('PROD_Apontamento_Kronos',datetime.now(),contato, tipo_contato, mensagem, caminho_anexo)])
    #contato, tipo_contato = verificar_tipo_de_contato('CIA Produção ID999')
    #gravar_em_banco_para_envio([('PROD_Apontamento_Kronos',datetime.now(),contato, tipo_contato, mensagem, caminho_anexo)])

    comPR = com[com['Frente'].str.contains('-PR-')]
    for frente in comPR.Frente.unique():
        undCod = frente.split('-')[0]
        if undCod in GruposPR.keys():
            mensagem = f'🚜 *Tratores {frente}:* Apontamentos e Comunicação'
            ParaGrupoPR = GruposPR[frente.split('-')[0]]
            caminho_anexo = gerar_imagem_kronos(comPR[comPR.Frente==frente], f'Apontamento_Preparo\\{frente}.png')
            contato, tipo_contato = verificar_tipo_de_contato(ParaGrupoPR)
            gravar_em_banco_para_envio([('PROD_Apontamento_Kronos',datetime.now(),contato, tipo_contato, mensagem, caminho_anexo)])
        else: print(f'Unidade {undCod} -PR- sem grupo!')
    #mensagem = f'🚜 *Tratores Preparo:* TOP 40 frotas a mais tempo sem enviar dados.'
    #caminho_anexo = gerar_imagem_kronos(comPR.head(40), f'Apontamento_Preparo\\sem_dados.png')
    #contato, tipo_contato = verificar_tipo_de_contato('19 99847-9246')
    #gravar_em_banco_para_envio([('PROD_Apontamento_Preparo',datetime.now(),contato, tipo_contato, mensagem, caminho_anexo)])
    #contato, tipo_contato = verificar_tipo_de_contato('CIA Produção ID999')
    #gravar_em_banco_para_envio([('PROD_Apontamento_Preparo',datetime.now(),contato, tipo_contato, mensagem, caminho_anexo)])

    comHB = com[com['Frente'].str.contains('-HB-')]
    for frente in comHB.Frente.unique():
        undCod = frente.split('-')[0]
        if undCod in GruposHB.keys():
            mensagem = f'🚜 *Tratores {frente}:* Apontamentos e Comunicação'
            ParaGrupoHB = GruposHB[frente.split('-')[0]]
            caminho_anexo = gerar_imagem_kronos(comHB[comHB.Frente==frente], f'Apontamento_Tratos\\{frente}.png')
            contato, tipo_contato = verificar_tipo_de_contato(ParaGrupoHB)
            gravar_em_banco_para_envio([('PROD_Apontamento_Apontamento_Tratos',datetime.now(),contato, tipo_contato, mensagem, caminho_anexo)])
        else: print(f'Unidade {undCod} -HB- sem grupo!')
    #mensagem = f'🚜 *Tratores Apontamento_Tratos:* TOP 40 frotas a mais tempo sem enviar dados.'
    #caminho_anexo = gerar_imagem_kronos(comHB.head(40), f'Apontamento_Tratos\\sem_dados.png')
    #contato, tipo_contato = verificar_tipo_de_contato('19 99847-9246')
    #gravar_em_banco_para_envio([('PROD_Apontamento_Tratos',datetime.now(),contato, tipo_contato, mensagem, caminho_anexo)])
    #contato, tipo_contato = verificar_tipo_de_contato('CIA Produção ID999')
    #gravar_em_banco_para_envio([('PROD_Apontamento_Tratos',datetime.now(),contato, tipo_contato, mensagem, caminho_anexo)])

##### COMPLIANCE

# Envio Compliance (Chuva e Cerca)
def contatos_cia():
    agron = pd.read_excel(verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\CIA Analytics - BOT CIA\Extrator\AGRON\agron_comunicacao.xlsx'))
    sgpa3 = pd.read_excel(verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\CIA Analytics - BOT CIA\Banco Dados\Compliance\frotas_historico.xlsx'))
    frentes_possiveis = set(list(set(agron['Frente associada']))+list(set(sgpa3.Grupo)))
    contatos_cia = {}
    prod = pd.read_excel(r'\\CSCLSFSR01\Agricola$\Logistica Agroindustrial\CIA 22.23\11. Analytics\BOT CIA\Contatos\Segunda_Funcao_Prod.xlsx')
    cct = pd.read_excel(r'\\CSCLSFSR01\Agricola$\Logistica Agroindustrial\CIA 22.23\11. Analytics\BOT CIA\Contatos\CCT_Contatos.xlsx')
    prod = prod.dropna(subset=['CONTROLE_Compliance','Torre_Numero'])
    prod['Frente'] = prod['Sigla_Unidade'] + "-" + prod['Sigla_Frente']
    prod = prod[['Frente','Torre_Numero']]
    cct = cct.dropna(subset=['CONTROLE_Compliance','Torre_Numero'])
    cct['Frente'] = cct['Sigla_Unidade'] + "-" + cct['Sigla_Frente']
    cct = cct[['Frente','Torre_Numero']]
    for id, linha in prod.iterrows():
        resultados = [item for item in frentes_possiveis if linha.Frente in item]
        for frente in resultados:
            if '-PL-' in frente or '-MU-' in frente:
                seg = frente.split('-')
                seg = f"{seg[0]}-{seg[2]}"
                contatos_cia[seg] = linha.Torre_Numero
            contatos_cia[frente] = linha.Torre_Numero
    for id, linha in cct.iterrows():
        resultados = [item for item in frentes_possiveis if linha.Frente in item]
        for frente in resultados:
            if '-MO-' in frente:
                seg = frente.split('-')
                seg = f"{seg[0]}-{seg[2]}"
                contatos_cia[seg] = linha.Torre_Numero
            contatos_cia[frente] = linha.Torre_Numero
    return contatos_cia

contatos_cia_C = contatos_cia()

def atualizar_apontamentos():
    def atualizar_bases_clima(): #abrir as 3 planilhas e verificar
        # Chuva CCT
        df1 = pd.read_excel(verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\Metas_Gov_Indicadores - Documentos\CCT\Parâmetros Relatórios CCT (Rotina).xlsx'), sheet_name='Ajuste', dtype={'Frente':str})
        df1 = df1[['Und.','Frente','PARADA CHUVA DATA/H','RETORNO CHUVA DATA/H','MOTIVO']]
        df1['Frente'] = df1['Frente'].astype(str).str.zfill(3)
        df1 = df1.dropna(subset='MOTIVO')
        # Linhas correspondentes serão filtradas
        df_filtrado = df1.query("MOTIVO == 'Trajeto' and `RETORNO CHUVA DATA/H` != 'NaT'") 
        df1 = df1.drop(df_filtrado.index)
        df1['RETORNO CHUVA DATA/H'] = df1['RETORNO CHUVA DATA/H'].fillna(datetime.now()+timedelta(days=10))
        df1 = df1[(~df1['PARADA CHUVA DATA/H'].isna()) & (df1['RETORNO CHUVA DATA/H'] > datetime.now()-timedelta(days=1))]
        frentes_cct = ['0'+f if len(f) == 2 else '00'+f if len(f) == 1 else f for f in list(df1.Frente.unique())]
        frentes_cct = ['-MO-'+f for f in frentes_cct]
        frentes_cct_reserva = ['-RE-'+f for f in frentes_cct]
        #frentes_cct = frentes_cct + frentes_cct_reserva
        frentes_cct = frentes_cct + [el.replace('MO','RE') for el in frentes_cct] + [el.replace('MO','LN') for el in frentes_cct] + frentes_cct_reserva
        # Chuva Plantio
        df2 = pd.read_excel(verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\MinhaTI\CIA Analytics - Plantio\Parametros Relatório Plantio.xlsx'), sheet_name='Dados')
        df2 = df2[['Frente Plantio','Frente Muda','Parada por chuva [dd/mm/aaaa hh:mm]','Retorno [dd/mm/aaaa hh:mm]','Cenário']]
        df2 = df2[df2['Cenário'] != 'Real']
        df2['Frente Plantio'] = df2['Frente Plantio'].astype(str).str.zfill(3)
        df2['Frente Muda'] = df2['Frente Muda'].astype(str).str.zfill(3)
        df2 = df2.dropna(subset='Parada por chuva [dd/mm/aaaa hh:mm]')
        df2['Retorno [dd/mm/aaaa hh:mm]'] = df2['Retorno [dd/mm/aaaa hh:mm]'].fillna(datetime.now()+timedelta(days=10))
        df2 = df2.dropna(subset='Cenário')
        df2['Frente'] = [f[-1] for f in df2['Frente Plantio'].str.split('-')]
        frentes_plantio = list(df2.Frente.unique())
        # Chuva Vinhaça
        df3 = pd.read_excel(verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\CIA Analytics - Vinhaça\Planilha de Chuva.xlsx'), sheet_name='FRENTES CHUVA')
        df3 = df3.dropna(subset=['Frente','Parada'])
        df3 = df3.fillna(datetime.now()+timedelta(days=10))
        df3.Retorno = pd.to_datetime(df3.Retorno, errors='coerce')
        df3 = df3[df3.Retorno > datetime.now()-timedelta(days=1)]
        frentes_vinhaca = list(df3.Frente.unique())
        # Retorna somente frentes que estejam paradas por 208
        return {'CCT':frentes_cct,'PL':frentes_plantio,'VN':frentes_vinhaca}
    chuva = atualizar_bases_clima()
    #Frentes paradas
    frente_parada = []
    for f in chuva.values(): frente_parada.extend(f)
    # Apontamento Atual
    df_apt = pd.read_excel(verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\CIA Analytics - BOT CIA\Extrator\AGRON\agron_comunicacao.xlsx'))
    #df_apt['Tempo em atividade'] = pd.to_timedelta(df_apt['Tempo em atividade'])
    df_apt['Tempo em atividade'] = pd.to_timedelta(df_apt['Tempo em atividade'], unit='s')
    df_apt = df_apt[(df_apt['Frente associada'] != 'Sem frente')] # & (df_apt['Tempo em atividade'] > timedelta(minutes=.5))]
    df_apt['Registro mais recente'] = pd.to_datetime(df_apt['Registro mais recente'], format='%Y-%b-%d %H:%M:%S', errors='coerce')
    df_apt['Frente_Chuva'] = df_apt['Frente associada'].str.contains('|'.join(frente_parada))
    def checar_comunicacao(row, frente):
        if frente.split('-')[0] in ['CAA', 'PTP', 'RBR','CAR']: trava = timedelta(minutes=30,hours=1)
        else: trava = timedelta(minutes=30)
        if row is not None and row <= trava:
            return True
        else:
            return False
    df_apt['Comunica'] = df_apt.apply(lambda row: checar_comunicacao(datetime.now() - row['Registro mais recente'], row['Frente associada']), axis=1)
    df_apt = df_apt[df_apt['Registro mais recente'] >= datetime.now()-timedelta(hours=6)]
    return df_apt

# Controles para cenário de compliance em cenário de chuva:
def carregar_acionamento_frota_compliance():
    def criar_dict_acionamento_frota_compliance():
        relacao = atualizar_apontamentos()
        relacao['Registro'] = datetime.now()-timedelta(days=1)
        relacao = relacao[['Número do Equipamento','Registro']].rename(columns={'Número do Equipamento':'Frota'})
        relacao = dict(zip(relacao.Frota, relacao.Registro))
        return relacao
    pasta_registro = os.getcwd()+'\compliance_dados'
    arquivo_registro_json = pasta_registro+'/acionamento_frota_c.json'
    # Se pasta registro não existe iremos criá-la:
    if os.path.exists(pasta_registro) == False: os.mkdir('compliance_dados')
    # Se arquivo de registro não existe criá-lo:
    if os.path.exists(arquivo_registro_json) == False:
        print('Arquivo de registro JSON não existe: Foi criado.')
        acionamento_frota_c = criar_dict_acionamento_frota_compliance()
        salvar_acionamento_frota_compliance(acionamento_frota_c)
    # Carregando o arquivo
    try:
        with open(arquivo_registro_json, "r") as arquivo:
            acionamento_frota_c = json.load(arquivo)
        return acionamento_frota_c
    except:
        print('\n------>>>>> NÃO CONSEGUIMOS ATUALIZAR "acionamento_frota_c"!!!')
        return dict()
#
def salvar_acionamento_frota_compliance(meu_dict_alvo):
    pasta_registro = os.getcwd()+'\compliance_dados'
    arquivo_registro_json = pasta_registro+'/acionamento_frota_c.json'
    def serializar_datetime(obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        raise TypeError(f"Tipo '{type(obj)}' não é serializável.")
    with open(arquivo_registro_json, "w") as arquivo:
        json.dump(meu_dict_alvo, arquivo, default=serializar_datetime)
#
# Controle para cenário de compliance em Cerca Eletrônica:
def carregar_acionamento_frota_compliance_cerca_eletronica():
    def salvar_acionamento_frota_compliance_cerca(meu_dict_alvo):
        pasta_registro = os.getcwd()+'\compliance_dados'
        arquivo_registro_json = pasta_registro+'/acionamento_frota_cerca_eletronica.json'
        def serializar_datetime(obj):
            if isinstance(obj, datetime):
                return obj.isoformat()
            raise TypeError(f"Tipo '{type(obj)}' não é serializável.")
        with open(arquivo_registro_json, "w") as arquivo:
            json.dump(meu_dict_alvo, arquivo, default=serializar_datetime)
    def criar_dict_acionamento_frota_compliance_cerca_eletronica():
        conn = sqlite3.Connection(verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\CIA Analytics - BOT CIA\Extrator\SGPA3\Exportacao Monit Mapa.db'))
        frotas = pd.read_sql("SELECT * FROM 'Exportacao Monit Mapa'", con=conn)
        frotas.sort_values(by=['Equipamento','Data/Hora'], ascending=False, inplace=True)
        frotas.drop_duplicates(subset='Equipamento', inplace=True)
        relacao = frotas.reset_index(drop=True)
        relacao['Registro'] = datetime.now()-timedelta(days=1)
        relacao = relacao[['Equipamento','Registro']].rename(columns={'Número do Equipamento':'Frota'})
        relacao = dict(zip(relacao.Equipamento, relacao.Registro))
        return relacao
    pasta_registro = os.getcwd()+'\compliance_dados'
    arquivo_registro_json = pasta_registro+'\\acionamento_frota_cerca_eletronica.json'
    # Se pasta registro não existe iremos criá-la:
    if os.path.exists(pasta_registro) == False: os.mkdir('compliance_dados')
    # Se arquivo de registro não existe criá-lo:
    if os.path.exists(arquivo_registro_json) == False:
        print('Arquivo de registro JSON não existe: Foi criado.')
        acionamento_frota_cerca_eletronica = criar_dict_acionamento_frota_compliance_cerca_eletronica()
        salvar_acionamento_frota_compliance_cerca(acionamento_frota_cerca_eletronica)
    # Carregando o arquivo
    try:
        with open(arquivo_registro_json, "r") as arquivo:
            acionamento_frota_cerca_eletronica = json.load(arquivo)
        return acionamento_frota_cerca_eletronica
    except IndexError:
        print('\n------>>>>> NÃO CONSEGUIMOS ATUALIZAR "acionamento_frota_cerca_eletronica"!!!')
        return dict()
#
def salvar_acionamento_frota_compliance_cerca_eletronica(meu_dict_alvo):
    pasta_registro = os.getcwd()+'\compliance_dados'
    arquivo_registro_json = pasta_registro+'/acionamento_frota_cerca_eletronica.json'
    def serializar_datetime(obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        raise TypeError(f"Tipo '{type(obj)}' não é serializável.")
    with open(arquivo_registro_json, "w") as arquivo:
        json.dump(meu_dict_alvo, arquivo, default=serializar_datetime)
#
# Contatos:
def contatos_cia():
    agron = pd.read_excel(verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\CIA Analytics - BOT CIA\Extrator\AGRON\agron_comunicacao.xlsx'))
    sgpa3 = pd.read_excel(verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\CIA Analytics - BOT CIA\Banco Dados\Compliance\frotas_historico.xlsx'))
    frentes_possiveis = set(list(set(agron['Frente associada']))+list(set(sgpa3.Grupo)))
    contatos_cia = {}
    prod = pd.read_excel(r'\\CSCLSFSR01\Agricola$\Logistica Agroindustrial\CIA 22.23\11. Analytics\BOT CIA\Contatos\Segunda_Funcao_Prod.xlsx')
    cct = pd.read_excel(r'\\CSCLSFSR01\Agricola$\Logistica Agroindustrial\CIA 22.23\11. Analytics\BOT CIA\Contatos\CCT_Contatos.xlsx')
    prod = prod.dropna(subset=['CONTROLE_Compliance','Torre_Numero'])
    prod['Frente'] = prod['Sigla_Unidade'] + "-" + prod['Sigla_Frente']
    prod = prod[['Frente','Torre_Numero']]
    cct = cct.dropna(subset=['CONTROLE_Compliance','Torre_Numero'])
    cct['Frente'] = cct['Sigla_Unidade'] + "-" + cct['Sigla_Frente']
    cct = cct[['Frente','Torre_Numero']]
    for id, linha in prod.iterrows():
        resultados = [item for item in frentes_possiveis if linha.Frente in item]
        for frente in resultados:
            if '-PL-' in frente or '-MU-' in frente:
                seg = frente.split('-')
                seg = f"{seg[0]}-{seg[2]}"
                contatos_cia[seg] = linha.Torre_Numero
            contatos_cia[frente] = linha.Torre_Numero
    for id, linha in cct.iterrows():
        resultados = [item for item in frentes_possiveis if linha.Frente in item]
        for frente in resultados:
            if '-MO-' in frente:
                seg = frente.split('-')
                seg = f"{seg[0]}-{seg[2]}"
                contatos_cia[seg] = linha.Torre_Numero
            contatos_cia[frente] = linha.Torre_Numero
    return contatos_cia

def gatilho_tipo_1_compliance():
    # MENSAGEM PARA APONTAMENTO CLIMA FROTA DE CENÁRIO
    df_atual = atualizar_apontamentos()
    def mensagem_compliance_chuva(frota, tipo_frota, frente,apontamento):
        try: momento_expiracao = retorno_chuva[frente[-3:]] if 'VN' not in frente else retorno_chuva[frente]
        except: momento_expiracao = False
        mensagem_compliance = []
        mensagem_compliance.append('🔵 *AVISO: Possível Cenário de Compliance!*')
        mensagem_compliance.append(f'Frota: {frota} - {tipo_frota}')
        mensagem_compliance.append(f'Frente: {frente}')
        mensagem_compliance.append(f'Apontamento: {apontamento}')
        if momento_expiracao != False: mensagem_compliance.append(f'\nSegundo regra de apontamentos, o limite para utilizarmos apontamento 208 foi até {momento_expiracao.strftime("%d/%m/%Y %H:%M:%S")}.\nFavor averiguar com a frente o que está ocorrendo.')
        else: mensagem_compliance.append(f'\nNão existem registros na planilha de chuva para frente {frente} apontar 208.\nFavor averiguar com a frente o que está ocorrendo.')
        return '\n'.join(mensagem_compliance)

    def atualizar_bases_clima_com_retorno(): #abrir as 3 planilhas e verificar
        # Chuva CCT
        df1 = pd.read_excel(verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\Metas_Gov_Indicadores - Documentos\CCT\Parâmetros Relatórios CCT (Rotina).xlsx'), sheet_name='Ajuste', dtype={'Frente':str})
        df1 = df1[['Und.','Frente','PARADA CHUVA DATA/H','RETORNO CHUVA DATA/H','MOTIVO']]
        df1a = pd.read_excel(verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\Metas_Gov_Indicadores - Documentos\CCT\Parâmetros Relatórios CCT (Rotina).xlsx'), sheet_name='Chuvas FF', dtype={'Frente':str})
        df1a = df1a[['Und.','Frente','PARADA CHUVA DATA/H','RETORNO CHUVA DATA/H','MOTIVO']].copy()
        df1a['PARADA CHUVA DATA/H'] = pd.to_datetime(df1a['PARADA CHUVA DATA/H'], errors='coerce')
        df1a = df1a.dropna(subset=['PARADA CHUVA DATA/H'])
        df1a = df1a[df1a['PARADA CHUVA DATA/H'] > datetime.now()-timedelta(hours=24)]
        df1 = pd.concat([df1a,df1])
        df1 = df1.dropna(subset=['MOTIVO','Frente','PARADA CHUVA DATA/H'])
        df1['Frente'] = df1['Frente'].astype(str).str.zfill(3)
        df1['Frente'] = ['MO-'+str(f) for f in df1['Frente']]
        #df1 = df1.dropna(subset='MOTIVO')
        # Linhas correspondentes serão filtradas
        df_filtrado = df1.query("MOTIVO == 'Trajeto' and `RETORNO CHUVA DATA/H` != 'NaT'") 
        df1 = df1.drop(df_filtrado.index)
        df1['RETORNO CHUVA DATA/H'] = df1['RETORNO CHUVA DATA/H'].fillna(datetime.now()+timedelta(days=10))
        df1 = df1[(~df1['PARADA CHUVA DATA/H'].isna()) & (df1['RETORNO CHUVA DATA/H'] > datetime.now()-timedelta(days=1))]
        df1 = df1[['Frente','RETORNO CHUVA DATA/H']]
        df1['chave'] = 'MO'
        # Chuva Plantio
        df2 = pd.read_excel(verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\CIA Analytics - Plantio\Parametros Relatório Plantio.xlsx'), sheet_name='Dados')
        df2 = df2[['Frente Plantio','Frente Muda','Parada por chuva [dd/mm/aaaa hh:mm]','Retorno [dd/mm/aaaa hh:mm]','Cenário']]
        df2 = df2[df2['Cenário'] != 'Real']
        df2['Frente Plantio'] = df2['Frente Plantio'].astype(str).str.zfill(3)
        df2['Frente Muda'] = df2['Frente Muda'].astype(str).str.zfill(3)
        df2 = df2.dropna(subset='Parada por chuva [dd/mm/aaaa hh:mm]')
        df2['Retorno [dd/mm/aaaa hh:mm]'] = df2['Retorno [dd/mm/aaaa hh:mm]'].fillna(datetime.now()+timedelta(days=10))
        df2 = df2.dropna(subset='Cenário')
        df2['Frente'] = [f[-1] for f in df2['Frente Plantio'].str.split('-')]
        df2 = df2[['Frente Plantio','Retorno [dd/mm/aaaa hh:mm]']]
        df2['chave'] = 'PL'
        # Chuva Vinhaça
        df3 = pd.read_excel(verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\CIA Analytics - Vinhaça\Planilha de Chuva.xlsx'), sheet_name='FRENTES CHUVA')
        df3 = df3.dropna(subset=['Frente','Parada'])
        df3 = df3.fillna(datetime.now()+timedelta(days=10))
        df3.Retorno = pd.to_datetime(df3.Retorno, errors='coerce')
        df3 = df3[df3.Retorno > datetime.now()-timedelta(days=1)]
        df3 = df3[['Frente','Parada']]
        df3['chave'] = 'VN'
        # Retorna somente frentes que estejam paradas por 208
        df1.columns = ['Frente', 'Retorno', 'chave']
        df2.columns = ['Frente', 'Retorno', 'chave']
        df3.columns = ['Frente', 'Retorno', 'chave']
        dfU = pd.concat([df1,df2,df3], ignore_index=True)
        return dict(zip(dfU.Frente, dfU.Retorno)) 

    apontamento_compliance = ['208 - Fatores Climáticos', '208 - Chuva Solo Umido','1118 - Man Corret - Oportunidade','1119 - Man Preven - Oportunidade', '208 - Parada por condicoes climaticas','1143 - Deslocamento Chuva']
    acionamento_frota_c = carregar_acionamento_frota_compliance()
    retorno_chuva = atualizar_bases_clima_com_retorno()
    # Cenário: Frota em apontamento de 208 sem cenário:
    apt_fora_de_cenario = df_atual[(df_atual.Frente_Chuva==False) & (df_atual.Atividade.isin(apontamento_compliance))]
    for id, linha in apt_fora_de_cenario.iterrows():
        frota = str(linha['Número do Equipamento'])
        if frota not in acionamento_frota_c.keys():
            # Verificar se está dentro do envio recente:
            acionamento_frota_c[frota] = datetime.now()
        if type(acionamento_frota_c[frota]) == str: ultimo_envio = datetime.strptime(acionamento_frota_c[frota][:19], '%Y-%m-%dT%H:%M:%S')
        else: ultimo_envio = acionamento_frota_c[frota]
        if linha['Frente associada'][-3:] in retorno_chuva.keys() and retorno_chuva[linha['Frente associada'][-3:]] > datetime.now():
            pass
        elif datetime.now() > (ultimo_envio+timedelta(minutes=120)):
            mensagem_compliance = mensagem_compliance_chuva(frota, linha['Tipo do equipamento'],linha['Frente associada'],linha['Atividade'])
            if linha['Frente associada'] in contatos_cia_C.keys():
                contato_envio = contatos_cia_C[linha["Frente associada"]]
                #else: contato_envio = '19 99832-6554'
                for contato_ in contato_envio.split(';'):
                    contato, tipo_contato = verificar_tipo_de_contato(contato_)
                    gravar_em_banco_para_envio([('CIA_Compliance',datetime.now(),contato, tipo_contato,mensagem_compliance,'')])
                if '-PL-' in mensagem_compliance or '-MU-' in mensagem_compliance:
                    contato, tipo_contato = verificar_tipo_de_contato('BOT CIA - Produção')
                    gravar_em_banco_para_envio([('CIA_Compliance',datetime.now(),contato, tipo_contato,mensagem_compliance,'')])
            acionamento_frota_c[frota] = datetime.now() # Acionar 
        else: pass # Ignorar
    salvar_acionamento_frota_compliance(acionamento_frota_c)

def gatilho_tipo_2_compliance():
    # APONTAMENTO DE CHUVA SENDO QUE FRENTE OPERANDO E DENTRO DE CERCA
    df_atual = atualizar_apontamentos()
    def mensagem_compliance_cerca_eletronica(frota, tipo_frota, frente,apontamento):
        mensagem_compliance = []
        mensagem_compliance.append('🟠 *AVISO: Possível Cenário de Compliance!*')
        mensagem_compliance.append(f'Frota: {frota} - {tipo_frota}')
        mensagem_compliance.append(f'Frente: {frente}')
        mensagem_compliance.append(f'Apontamento: {apontamento}')
        mensagem_compliance.append(f'\nFrota recolhida (Dentro da Oficina Interna) e em apontamento de clima, sua frente {frente} está em cenário REAL.\nFavor averiguar o que está ocorrendo e se necessário solicitar correção do apontamento.')
        return '\n'.join(mensagem_compliance)
    # Cenário: Frota em apontamento de 208 porém status anterior Cerca Eletrônica era Apt Real
    # Usando dados Comunicação:
    apontamento_compliance = ['208 - Fatores Climáticos', '208 - Chuva Solo Umido','1118 - Man Corret - Oportunidade','1119 - Man Preven - Oportunidade', '208 - Parada por condicoes climaticas','1143 - Deslocamento Chuva']
    apt_fora_de_cenario = df_atual[(df_atual.Atividade.isin(apontamento_compliance))]
    acionamento_frota_cerca_eletronica = carregar_acionamento_frota_compliance_cerca_eletronica()
    passado_cerca = pd.read_excel(verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\CIA Analytics - BOT CIA\Banco Dados\Compliance\frotas_historico.xlsx'))
    for id, linha in apt_fora_de_cenario.iterrows():
        frota = str(linha['Número do Equipamento'])
        if frota not in acionamento_frota_cerca_eletronica.keys():
            acionamento_frota_cerca_eletronica[frota] = datetime.now()-timedelta(days=1)
            # Verificar se está dentro do envio recente:
        if int(frota) in list(passado_cerca.Equipamento) and passado_cerca[passado_cerca.Equipamento==int(frota)].Cerca_Oficina.values[0] == True:
            if type(acionamento_frota_cerca_eletronica[frota]) == str: ultimo_envio = datetime.strptime(acionamento_frota_cerca_eletronica[frota][:19], '%Y-%m-%dT%H:%M:%S')
            else: ultimo_envio = acionamento_frota_cerca_eletronica[frota]
            # Verificar se está fora do cenario e com apontamento de clima
            if datetime.now() > (ultimo_envio+timedelta(minutes=120)) and linha['Frente_Chuva'] == False:
                mensagem_compliance = mensagem_compliance_cerca_eletronica(frota, linha['Tipo do equipamento'],linha['Frente associada'],linha['Atividade'])
                if linha['Frente associada'] in contatos_cia_C.keys():
                    contato_envio = contatos_cia_C[linha["Frente associada"]]
                    #else: contato_envio = '19 99832-6554'
                    for contato_ in contato_envio.split(';'):
                        contato, tipo_contato = verificar_tipo_de_contato(contato_)
                        gravar_em_banco_para_envio([('CIA_Compliance',datetime.now(),contato, tipo_contato,mensagem_compliance,'')])
                    if '-PL-' in mensagem_compliance or '-MU-' in mensagem_compliance:
                        contato, tipo_contato = verificar_tipo_de_contato('BOT CIA - Produção')
                        gravar_em_banco_para_envio([('CIA_Compliance',datetime.now(),contato, tipo_contato,mensagem_compliance,'')])
                acionamento_frota_cerca_eletronica[frota] = datetime.now()
    salvar_acionamento_frota_compliance_cerca_eletronica(acionamento_frota_cerca_eletronica)

def gatilho_tipo_3_compliance():
    # MONIT A MONIT: Frota mudou de apt real para chuva dentro da cerca eletrônica
    def mensagem_compliance_cerca_eletronica_MONIT(frota, tipo_frota, frente):
        mensagem_compliance = []
        mensagem_compliance.append('🟠 *AVISO: Possível Cenário de Compliance!*')
        mensagem_compliance.append(f'Frota: {frota} - {tipo_frota}')
        mensagem_compliance.append(f'Frente: {frente}')
        mensagem_compliance.append(f'\nApontamento de frota recolhida alterado para chuva.\nSe frota foi recolhida em cenário real o apontamento não pode ser mudado para chuva!\nFavor averiguar o que está ocorrendo e se necessário solicitar correção do apontamento.')
        return '\n'.join(mensagem_compliance)
    acionamento_frota_cerca_eletronica = carregar_acionamento_frota_compliance_cerca_eletronica()
    monit_p = pd.read_excel(verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\CIA Analytics - BOT CIA\Banco Dados\Compliance\frotas_historico_passado.xlsx'))
    monit = pd.read_excel(verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\CIA Analytics - BOT CIA\Banco Dados\Compliance\frotas_historico.xlsx'))
    apt_chuva = 'Parada por condicoes climaticas'
    for id,linha in monit[monit.Cerca_Oficina==True].iterrows():
        if str(linha.Equipamento) not in acionamento_frota_cerca_eletronica.keys():
            # Registrar no dict json
            acionamento_frota_cerca_eletronica[str(linha.Equipamento)] = datetime.now()-timedelta(days=1)
        if type(acionamento_frota_cerca_eletronica[str(linha.Equipamento)]) == str: ultimo_envio = datetime.strptime(acionamento_frota_cerca_eletronica[str(linha.Equipamento)][:19], '%Y-%m-%dT%H:%M:%S')
        else: ultimo_envio = acionamento_frota_cerca_eletronica[str(linha.Equipamento)]
        # Verificando se é gatilho para envio
        if linha.Equipamento in list(monit_p.Equipamento) and linha.Operacao == apt_chuva and monit_p[monit_p.Equipamento==linha.Equipamento].Operacao.values[0] != apt_chuva and monit_p[monit_p.Equipamento==linha.Equipamento].Cerca_Oficina.values[0] == True and datetime.now() > (ultimo_envio+timedelta(minutes=120)):
            mensg = mensagem_compliance_cerca_eletronica_MONIT(linha.Equipamento,str(linha['Tipo de Equipamento']).capitalize(),linha.Grupo)
            if linha['Frente associada'] in contatos_cia_C.keys():
                contato_envio = contatos_cia_C[linha["Frente associada"]]
                #else: contato_envio = '19 99832-6554'
                for contato_ in contato_envio.split(';'):
                    contato, tipo_contato = verificar_tipo_de_contato(contato_)
                    gravar_em_banco_para_envio([('CIA_Compliance',datetime.now(),contato, tipo_contato,mensg,'')])
                if 'PLANTADORA' in mensg:
                    contato, tipo_contato = verificar_tipo_de_contato('BOT CIA - Produção')
                    gravar_em_banco_para_envio([('CIA_Compliance',datetime.now(),contato, tipo_contato,mensg,'')])
            acionamento_frota_cerca_eletronica[str(linha.Equipamento)] = datetime.now()
    salvar_acionamento_frota_compliance_cerca_eletronica(acionamento_frota_cerca_eletronica)  

def mensagens_compliance_SGPA3():
    dfn = carregar_df_monitoramento_SGPA3()
    #dfn = dfn.drop(columns="_id")

    def modificacao_arquivo(caminho):
            try: modificacao = datetime.fromtimestamp(os.path.getmtime(caminho))
            except: modificacao = datetime(1999,3,12)
            return modificacao

    def coletar_contatos():
        caminho_contatos_cct = r'\\CSCLSFSR01\Agricola$\Logistica Agroindustrial\CIA 22.23\11. Analytics\BOT CIA\Contatos\CCT_Contatos.xlsx'
        caminho_contatos_prod = r'\\CSCLSFSR01\Agricola$\Logistica Agroindustrial\CIA 22.23\11. Analytics\BOT CIA\Contatos\Segunda_Funcao_Prod.xlsx'
        caminho_cotatos = r'Compliance/contatos.json'

        if not os.path.exists('Compliance'):
            os.mkdir('Compliance')
            
        mod_cct = modificacao_arquivo(caminho_contatos_cct)
        mod_prod = modificacao_arquivo(caminho_contatos_prod)
        mod_cont = modificacao_arquivo(caminho_cotatos)

        if mod_cont > mod_cct and mod_cont > mod_prod:
            with open(caminho_cotatos, 'r') as file:
                contatos = file.read()
                contatos = json.loads(contatos)
        else:
            # Carregamento PROD
            prod = pd.read_excel(caminho_contatos_prod)
            prod = prod.dropna(subset=['CONTROLE_Compliance','Torre_Numero'])
            prod['Frente'] = prod['Sigla_Unidade'] + "-" + prod['Sigla_Frente']
            prod['Torre_Numero'] = prod['Torre_Numero'].astype(str)
            prod = prod[['Frente','Torre_Numero']]
            # CArregadno CCT
            cct = pd.read_excel(caminho_contatos_cct)
            cct = cct.dropna(subset=['CONTROLE_Compliance','Torre_Numero'])
            cct['Torre_Numero'] = cct['Torre_Numero'].astype(str)
            cct['Frente'] = cct['Sigla_Unidade'] + "-" + cct['Sigla_Frente']
            cct = cct[['Frente','Torre_Numero']]
            # Juntando
            contatos = pd.concat([cct,prod])
            contatos = dict(zip(contatos['Frente'],contatos['Torre_Numero']))
            with open(caminho_cotatos, 'w') as file:
                file.write(json.dumps(contatos))
        return contatos

    def atualizar_cenario():
        caminho_chuva_cct = verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\MinhaTI\Metas_Gov_Indicadores - Documentos\CCT\Parâmetros Relatórios CCT (Rotina).xlsx')
        caminho_chuva_plant = verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\MinhaTI\CIA Analytics - Plantio\Parametros Relatório Plantio.xlsx' )
        caminho_chuva_vn_pr = verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\MinhaTI\CIA Analytics - Vinhaça\Planilha de Chuva.xlsx')
        caminho_cenario = r'Compliance/cenario.json'

        mod_cct = modificacao_arquivo(caminho_chuva_cct)
        mod_plant = modificacao_arquivo(caminho_chuva_plant)
        mod_vn_pr = modificacao_arquivo(caminho_chuva_vn_pr)
        mod_chuva = modificacao_arquivo(caminho_cenario)

        if mod_chuva > mod_cct and mod_chuva > mod_plant and mod_chuva > mod_vn_pr:
            with open(caminho_cenario, 'r') as file:
                cenario = file.read()
                cenario = json.loads(cenario)
        else:
            df1 = pd.read_excel(caminho_chuva_cct, sheet_name='Ajuste', dtype={'Frente':str})
            df1 = df1[['Und.','Frente','PARADA CHUVA DATA/H','RETORNO CHUVA DATA/H','MOTIVO']]
            df1['Frente'] = df1['Frente'].astype(str).str.zfill(3)
            todas_frentes_CCT = ['0'+f if len(f) == 2 else '00'+f if len(f) == 1 else f for f in list(df1.Frente.unique())]
            df1 = df1.dropna(subset='MOTIVO')
            # Linhas correspondentes serão filtradas
            df_filtrado = df1.query("MOTIVO == 'Trajeto' and `RETORNO CHUVA DATA/H` != 'NaT'") 
            df1 = df1.drop(df_filtrado.index)
            df1['RETORNO CHUVA DATA/H'] = df1['RETORNO CHUVA DATA/H'].fillna(datetime.now()+timedelta(days=10))
            df1 = df1[(~df1['PARADA CHUVA DATA/H'].isna()) & (df1['RETORNO CHUVA DATA/H'] > datetime.now()-timedelta(days=1))]
            frentes_cct = ['0'+f if len(f) == 2 else '00'+f if len(f) == 1 else f for f in list(df1.Frente.unique())]
            frentes_cct = frentes_cct
            # Chuva Plantio
            df2 = pd.read_excel(caminho_chuva_plant, sheet_name='Dados')
            df2 = df2[['Frente Plantio','Frente Muda','Parada por chuva [dd/mm/aaaa hh:mm]','Retorno [dd/mm/aaaa hh:mm]','Cenário']]
            todas_frentes_plantio = list(df2['Frente Plantio'].str[-3:].unique())
            df2 = df2[df2['Cenário'] != 'Real']
            df2['Frente Plantio'] = df2['Frente Plantio'].astype(str).str.zfill(3)
            df2['Frente Muda'] = df2['Frente Muda'].astype(str).str.zfill(3)
            df2 = df2.dropna(subset='Parada por chuva [dd/mm/aaaa hh:mm]')
            df2['Retorno [dd/mm/aaaa hh:mm]'] = df2['Retorno [dd/mm/aaaa hh:mm]'].fillna(datetime.now()+timedelta(days=10))
            df2 = df2.dropna(subset='Cenário')
            df2['Frente'] = [str(f[-1]).zfill(3) for f in df2['Frente Plantio'].str.split('-')]
            frentes_plantio = list(df2.Frente.unique())
            # Chuva Vinhaça
            df3 = pd.read_excel(caminho_chuva_vn_pr, sheet_name='FRENTES CHUVA', header=1)
            #df3['Frente'] = df3['Frente'].str[:7]
            todas_frentes_vinhaca = list(df3.Frente.unique())
            df3 = df3.dropna(subset=['Frente','Parada'])
            df3 = df3.fillna(datetime.now()+timedelta(days=10))
            df3.Retorno = pd.to_datetime(df3.Retorno, errors='coerce')
            df3 = df3[df3.Retorno > datetime.now()-timedelta(days=1)]
            frentes_vinhaca = list(df3.Frente.unique())
            # Preparo
            df4 = pd.read_excel(caminho_chuva_vn_pr, sheet_name='CHUVA - PREPARO', header=1)
            #df4['Frente'] = df4['Frente'].str[:7]
            todas_frentes_preparo = list(df4.Frente.unique())
            df4 = df4.dropna(subset=['Frente','Parada por Chuva'])
            df4 = df4.fillna('NÃO')
            df4['Frente'] = df4['Frente']
            df4 = df4[df4['Parada por Chuva']=='SIM']
            frentes_preparo = list(df4.Frente.unique())
            # Retorna somente frentes que estejam paradas por 208
            #cenario_chuva = {'CCT':frentes_cct,'PL':frentes_plantio,'VN':frentes_vinhaca,'PR':frentes_preparo}
            cenario_chuva = frentes_cct + frentes_plantio + frentes_vinhaca + frentes_preparo
            cenario_chuva = [frente for frente in cenario_chuva if 'nan' not in frente]
            todas_frentes = todas_frentes_CCT + todas_frentes_plantio + todas_frentes_preparo + todas_frentes_vinhaca
            cenario_real = [frente for frente in todas_frentes if frente not in cenario_chuva and 'nan' != str(frente)]
            cenario = {'chuva':cenario_chuva,'real':cenario_real}
            with open(caminho_cenario, 'w') as file:
                    file.write(json.dumps(cenario))
        return cenario

    def salvar_acionamento(meu_dict_alvo):
        arquivo_registro_json = f'Compliance/acionamento_frota.json'
        def serializar_datetime(obj):
            if isinstance(obj, datetime):
                return obj.isoformat()
            raise TypeError(f"Tipo '{type(obj)}' não é serializável.")
        with open(arquivo_registro_json, "w") as arquivo:
            json.dump(meu_dict_alvo, arquivo, default=serializar_datetime)

    def carregar_acionamento():
        arquivo_registro_json = f'Compliance/acionamento_frota.json'
        if not os.path.exists(arquivo_registro_json):
            salvar_acionamento({})
            print('Criamos arquivo: Compliance/acionamento_frota.json')
        with open(arquivo_registro_json, "r") as arquivo:
            meu_dict = json.load(arquivo)
        for chave in meu_dict.keys():
            meu_dict[chave] = pd.to_datetime(meu_dict[chave], dayfirst=False)
        return meu_dict

    contatos = coletar_contatos()
    cenario = atualizar_cenario()
    acionamento = carregar_acionamento()

    '''
    Aplicadores(VN/BT), Tratores(PR), Plantadoras(PL) = 0 (instantâneo) 
    Colhedora(MU) = 5min no apontamento
    Caminhões(VN/MU) = 15min no apontamento'''
    dfn["Tempo em atividade"] = dfn["Tempo em atividade"].str.replace('DIA(S)','days').str.replace(': ',':').str.replace(' :',':')
    dfn["Tempo em atividade"] = pd.to_timedelta(dfn["Tempo em atividade"])
    df_caminh = dfn[(dfn["Tipo do equipamento"].str.upper().str.contains("CAMINH")) & (dfn["Tempo em atividade"]>=timedelta(minutes=15))]
    df_cdmu = dfn[(dfn["Tipo do equipamento"]=="COLHEDORA") & (dfn["Tempo em atividade"]>=timedelta(minutes=5)) & (dfn["Frente associada"].str.contains('-MU-'))]
    df_rest = dfn[~((dfn["Tipo do equipamento"]=="COLHEDORA") & (dfn["Frente associada"].str.contains('-MU-'))) & ~(dfn["Tipo do equipamento"].str.upper().str.contains("CAMINH"))]
    dfn = pd.concat([df_rest,df_caminh,df_cdmu])
    
    df_real = dfn[(dfn['Frente associada'].str.contains('|'.join(cenario['real']))) & ~(dfn['Frente associada'].str.contains('|'.join(cenario['chuva'])))]
    
    if cenario['chuva'] != []: df_chuva = dfn[dfn['Frente associada'].str.contains('|'.join(cenario['chuva']))]
    else: df_chuva = dfn[dfn['Frente associada'] == 'Frente que não é pra existir!!!']
    # Frente em cen[ario real que estejam em apontamento clima
    apontamentos_compliance = ['1143 -', '208 -'] #, '977 -', '227 -', '233 -']
    df_real_gat = df_real[df_real['Atividade'].str.contains('|'.join(apontamentos_compliance))]
    # Frotas que estejam em cenário cluva e que estão em FALTA/improdutivo
    apontamentos_improdutivos = ['840 -', '237 -']  # '224 -' Roberta pediu para tirar 29/07/2024
    df_chuva_gat = df_chuva[df_chuva['Atividade'].str.contains('|'.join(apontamentos_improdutivos))]
    # Gatilhos
    df_gatilhos = pd.concat([df_chuva_gat,df_real_gat]).reset_index(drop=True)
    df_gatilhos['Registro mais recente'] = pd.to_datetime(df_gatilhos['Registro mais recente'], dayfirst=True)
    df_gatilhos = df_gatilhos[df_gatilhos['Registro mais recente'] > (datetime.now()-timedelta(hours=6))]

    frentes_cct = [frente[-3:] for frente in dfn["Frente associada"].unique() if "-MO-" in frente]
    frentes_prod = [frente[-3:] for frente in dfn["Frente associada"].unique() if "-PL-" in frente or "-MU-" in frente]

    def gerar_compliance_chuva(row):
        apt = row.Atividade
        if '1143 -' in apt or '208 -' in apt:
            cor_aviso = '🔵'
            mensagem_auxiliar = '\n*NOTA*: Não existem parâmetros de chuva para essa frente.'
        elif '840 -' in apt or '237 -' in apt or '224 -' in apt:
            cor_aviso = '🟡'
            mensagem_auxiliar = f'\n*NOTA*: Avaliar apontamento atual da frota pois ela está em cenário de chuva.'
        duracao = calcular_tempo(row['Tempo em atividade'])
        ultima_comunicao = row['Registro mais recente'] + duracao
        mensagem_compliance = f'''{cor_aviso} *AVISO: Possível Cenário de Compliance!*
*Frota*: {row['Número do Equipamento']} - {row['Tipo do equipamento'].split(' ')[0]}
*Frente*: {row['Frente associada']}
*Apontamento*: {apt}
*Ultimo Comunicação*: {ultima_comunicao.strftime("%d/%m/%Y %H:%M:%S")} [{duracao}]'''#datetime.now()-ultima_comunicao-duracao
        mensagem_compliance = mensagem_compliance + mensagem_auxiliar
        return mensagem_compliance

    df_gatilhos['Número do Equipamento'] = df_gatilhos['Número do Equipamento'].astype(str)
    frotas_recentes = [str(frota) for frota,data in zip(acionamento.keys(), acionamento.values()) if data > datetime.now()]

    for id,row in df_gatilhos[(~df_gatilhos['Frente associada'].str.contains('-BT-')) & (~df_gatilhos['Número do Equipamento'].isin(frotas_recentes))].iterrows():
        frota = row['Número do Equipamento']
        mensagem_compliance = gerar_compliance_chuva(row)
        #if frota in acionamento.keys() and acionamento[frota] < datetime.now():
        acionamento[row['Número do Equipamento']] = datetime.now()+timedelta(hours=4)
        salvar_acionamento(acionamento)
        frente = row['Frente associada']
        und, und_frente, numero_frente = frente[:3], frente[:6], frente[-3:]
        frente_chave = und_frente if und_frente in contatos.keys() else und+'-MO' if numero_frente in frentes_cct else und+'-PL' if numero_frente in frentes_prod else 'NÃO ENCONTRADA!'
        destinos = contatos[frente_chave] if frente_chave in contatos.keys() else 'NÃO ENCONTRADA!'
        if destinos != 'NÃO ENCONTRADA!':
            destinos = destinos.split(';') if ';' in destinos else [destinos]
            for contatox in destinos:
                contato, tipo_contato = verificar_tipo_de_contato(contatox)
                gravar_em_banco_para_envio([('CIA_Compliance',datetime.now(),contato, tipo_contato,mensagem_compliance,'')])
            if '-PL-' in frente or '-MU-' in frente or '-PR-' in frente or '-VN-' in frente:
                contato, tipo_contato = verificar_tipo_de_contato('BOT CIA - Produção')
                if '-PL-' in frente or '-MU-' in frente:
                    if 'PLANTADORA' in mensagem_compliance:
                        gravar_em_banco_para_envio([('CIA_Compliance',datetime.now(),contato, tipo_contato,mensagem_compliance,'')])

def velocidade_plantadoras_DMC():
   #Lógica Desenvolvida por Daniel Martins Chiacchio
  #importando o arquivo
  df = pd.read_parquet(verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\CIA Analytics - BOT CIA\Extrator\Azure\SGPA2_DDN_HORAS_OPERACIONAIS_ON_EQUIP_PL_MU.parquet'))
  dfr = pd.read_parquet(verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\CIA Analytics - BOT CIA\Extrator\Azure\SGPA2_DDN_HORAS_OPERACIONAIS_ON_EQUIP_RESERVA.parquet'))
  df = pd.concat([df,dfr])
  # Selecionando somente operações com plantadoras.
  df_pl = df[df['FG_TP_EQUIPAMENTO']==58]
  h_norm = (datetime.now()-timedelta(hours=1)).hour
  h_fuso = (datetime.now()-timedelta(hours=2)).hour
  unidades_fuso = ['PASSATEMPO','CAARAPO']
  # Velocidade operacional das plantadoras
  df_pl['HR_LOCAL'] = df_pl['HR_LOCAL'].astype(int)
  df_pl_op = df_pl[(df_pl["CD_OPERACAO"]==789) 
                  & ((df_pl['HR_LOCAL']==h_norm) & ~(df_pl['DESC_UNIDADE'].isin(unidades_fuso)))
                  | ((df_pl['HR_LOCAL']==h_fuso) & (df_pl['DESC_UNIDADE'].isin(unidades_fuso)))]
  df_pl_op['VEL_P'] = df_pl_op['VL_VELOCIDADE_MEDIA']

  #agrupar por unidade, frente, plantadora
  result = df_pl_op.groupby(['DESC_UNIDADE','DESC_GRUPO_EQUIPAMENTO',"CD_EQUIPAMENTO"]).apply(lambda x: round(np.average(x['VEL_P'], weights=x['HR_OPERACIONAIS_VEL']),1))
  df2 = result.to_frame('Velocidade').reset_index(drop=False)

  cadastro = pd.read_excel(r'\\CSCLSFSR01\Agricola$\Logistica Agroindustrial\CIA 22.23\11. Analytics\BOT CIA\Contatos\Velocidades_PL.xlsx')
  cadastro = cadastro.dropna(subset='Destino_envio')
  cadastro = dict(zip(cadastro["Unidade"],cadastro["Destino_envio"]))

  #criar um dicionário frame para as metas de velocidade de plantadoras.
  df_metas_OBZ = pd.read_excel(verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\Metas_Gov_Indicadores - Documentos\PRODUÇÃO AGRÍCOLA\PLANTIO\DO\CADASTROS_PL.xlsx'), sheet_name="PLANOS_OBZ")
  df_metas_OBZ = df_metas_OBZ[(df_metas_OBZ["Índice_1"]=="Velocidade [km/h]") & (df_metas_OBZ["Mês"] == datetime.now().month) & (df_metas_OBZ["Safra"] == "S'2425")]
  rename_metas = {
      'BONF': 'BONFIM',
      'CAAR': 'CAARAPO',
      'CONTI': 'CONTINENTAL',
      'COPI': 'COSTA PINTO',
      'DEST':'DESTIVALE',
      'DIA': 'DIAMANTE',
      'IASF': 'SÃO FRANCISCO',
      'IPA':'IPAUSSU',
      'JUN': 'JUNQUEIRA',
      'LPRAT': 'LAGOA DA PRATA',
      'MUND': 'MUNDIAL',
      'PARAI': 'PARAISO',
      'PASSA': 'PASSATEMPO',
      'RAF': 'RAFARD',
      'RBRIL':'RIO BRILHANTE',
      'SCAND': 'SANTA CÂNDIDA',
      'SELIS': 'SANTA ELISA',
      'UNI': 'UNIVALEM',
      'VALER': 'VALE DO ROSARIO',
      'BARRA': 'BARRA',
      'BENA': 'BENALCOOL',
      'GASA': 'GASA',
      'JATAI': 'JATAI',
      'LEME': 'LEME',
      'SERRA': 'SERRA',
      'UMB': 'UMB',
      'ZANIN': 'ZANIN'}

  df_metas_OBZ["Unidade"] = df_metas_OBZ["Unidade"].map(rename_metas, na_action='ignore')
  dfn = df_metas_OBZ[['Unidade','Valor']]
  metas_obz = {f:m for f,m in zip(dfm["Unidade"],dfm["Valor"])}

  #para cada unidade montar a mensagem;
  def montar_mensagem(data):
    # data: DataFrame
    mensagem = [f'🌱 *Velocidade Plantadoras {unidade}*']

    dff = data.sort_values(by=["DESC_GRUPO_EQUIPAMENTO",'Velocidade'], ascending=False)

    meta_vel_und = round(metas_obz[unidade],1) if unidade in metas_obz.keys() else 'Sem meta!'
    #titulo = f"*Reporte Velocidade de Plantadora*"
    for frente,df_frente in dff.groupby(['DESC_GRUPO_EQUIPAMENTO']):
      corpo = f"\n*{frente[0]}* Meta: {meta_vel_und} km/h"
      mensagem.append(corpo)

      for id,frota in df_frente.iterrows():
        frota_alvo = frota["CD_EQUIPAMENTO"]
        vel_alvo = frota["Velocidade"]

        if meta_vel_und != 'Sem meta!':
                  icone = "🔴" if vel_alvo < meta_vel_und else "✅"
                  mensagem.append(f'PL {frota_alvo} - {vel_alvo} km/h {icone}')
        else: mensagem.append(f'PL {frota_alvo} - {vel_alvo} km/h')
    return '\n'.join(mensagem)

  for unidade,dados in df2.groupby('DESC_UNIDADE'):
        if unidade in cadastro.keys():
            destino_grupo = cadastro[unidade]
            mensagem = montar_mensagem(dados)
            contato, tipo_contato = verificar_tipo_de_contato(destino_grupo)
            gravar_em_banco_para_envio([('PROD_VelocidadePL',datetime.now(),contato, tipo_contato,mensagem,'')])

def gatilho_atualizar_hist_frotas_compliance():
    def atualizar_historico_frotas():
        # pip install GDAL
        # pip install pykml
        # pip install lxml
        from zipfile import ZipFile
        from bs4 import BeautifulSoup
        from shapely.geometry import Point, Polygon
        import pandas as pd
        from lxml import html
        from shapely.ops import unary_union
        import sqlite3

        #>>>>>># Atualizar cercas em única variavel: todas_cercas_oficina
        # Abrindo arquivo dentro do .kmz
        caminho_arquivo = r'C:\Users\ciaanalytics\Downloads\CercasSgpa3 v7 230523.kmz'
        kmz = ZipFile(caminho_arquivo, 'r')
        kml = kmz.open('doc.kml', 'r').read()
        doc = html.fromstring(kml)
        # Lendo dados
        soup = BeautifulSoup(kml, 'html.parser')
        dados = []
        for placemark in soup.find_all('placemark'):
            # Extrair informações específicas do placemark | # Exemplo: extrair o nome e as coordenadas
            nome = placemark.find('name').text
            coordenadas = placemark.find('coordinates').text
            # Adicionar as informações extraídas à lista de dados
            dados.append({'nome': nome, 'coordenadas': coordenadas})
        # Convertendo coordenadas em Polygonos
        def cerca_polida(dados_cerca_alvo): 
            # Converte a String de coordenadas do arquivo lido para polygonos
            linha = dados_cerca_alvo.replace('\n','').replace('\t','')
            objeto = [ponto.split(',')[:2] for ponto in linha.split(' ')]
            list_t_coord = []
            for coords in objeto:
                if len(coords) == 2: # Se tivermos 2 valores (Lat e Lon) [Exceções tem 3 valores ou 1]
                    list_t_coord.append(tuple(coords)) #Foram desenhadas como (Lon, Lat)
            return Polygon(list_t_coord) if Polygon(list_t_coord).is_valid else Polygon(list_t_coord).buffer(0)
        for dicionario in dados:
            coordenadas = dicionario['coordenadas']
            dicionario['coordenadas'] = cerca_polida(dicionario['coordenadas'])
        # Converte em banco estruturado.
        df_cerca = pd.DataFrame(dados)
        df_cerca.rename(columns={'nome':'Nome_Cerca','coordenadas':'Coordenadas'}, inplace=True)
        todas_cercas_oficina = unary_union(list(df_cerca.Coordenadas))
        #>>>>>># Gerando arquivo histórico
        def ponto_dentro_cerca_LonLat(row):
            if todas_cercas_oficina.contains(Point(row.Longitude, row.Latitude)):
                return True
            return False
        conn = sqlite3.Connection(verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\CIA Analytics - BOT CIA\Extrator\SGPA3\Exportacao Monit F.db'))
        frotas = pd.read_sql("SELECT * FROM 'Exportacao Monit F'", con=conn)
        frotas.sort_values(by=['Equipamento','Data/Hora'], ascending=False, inplace=True)
        frotas.drop_duplicates(subset='Equipamento', inplace=True)
        frotas.reset_index(drop=True, inplace=True)
        frotas['Cerca_Oficina'] = frotas.apply(ponto_dentro_cerca_LonLat, axis=1)
        frotas['Data/Hora'] = pd.to_datetime(frotas['Data/Hora'])
        #frotas = frotas[['Equipamento','Data/Hora','Grupo','Operacao','Descricao Equipamento','Tipo de Equipamento','Estado Operacional','Latitude','Longitude']]
        # Gerando arquivo para salvar histórico
        df_apt = atualizar_apontamentos()
        df_apt_merge = df_apt[['Número do Equipamento','Frente_Chuva','Comunica','Tempo em atividade']].rename(columns={'Número do Equipamento':'Equipamento','Tempo em atividade':'Duração'})
        frotas_hist = pd.merge(frotas, df_apt_merge, on='Equipamento', how='left')
        frotas_hist.Comunica = frotas_hist.Comunica.fillna('So_SGPA3')
        frotas_hist.Frente_Chuva = frotas_hist.Frente_Chuva.fillna('So_SGPA3')
        frotas_hist['Duração'] = frotas_hist['Duração'].fillna('So_SGPA3')
        past = pd.read_excel(verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\CIA Analytics - BOT CIA\Banco Dados\Compliance\frotas_historico.xlsx'))
        past.to_excel(verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\CIA Analytics - BOT CIA\Banco Dados\Compliance\frotas_historico_passado.xlsx'), index=False)
        frotas_hist = pd.concat([frotas_hist, past], ignore_index=True)
        frotas_hist.sort_values(by=['Equipamento','Data/Hora'], ascending=False, inplace=True)
        frotas_hist.drop_duplicates(subset='Equipamento', inplace=True)
        frotas_hist.to_excel(verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\CIA Analytics - BOT CIA\Banco Dados\Compliance\frotas_historico.xlsx'), index=False)
    def ler_atualizacao_mapa_frotas():
        pasta_registro = os.getcwd()+'\compliance_dados'
        arquivo_registro = pasta_registro+'/registro_mapa_frotas.txt'
        # Se pasta registro não existe iremos criá-la:
        if os.path.exists(pasta_registro) == False: os.mkdir('compliance_dados')
        # Se arquivo de registro não existe criá-lo:
        if os.path.exists(arquivo_registro) == False:
            print('Arquivo de registro não existe: Foi criado.')
            with open(arquivo_registro, 'w') as arquivo: 
                arquivo.write(str(datetime.now()-timedelta(days=1)))
        with io.open(arquivo_registro, 'r') as arquivo:
            data_str = arquivo.read()
            data = datetime.strptime(data_str[:19], '%Y-%m-%d %H:%M:%S')
            return data
    def salvar_atualizacao_mapa_frotas():
        data = datetime.fromtimestamp(os.path.getmtime(verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\CIA Analytics - BOT CIA\Extrator\SGPA3\Exportacao Monit Mapa.db')))
        pasta_registro = os.getcwd()+'\compliance_dados'
        arquivo_registro = pasta_registro+'/registro_mapa_frotas.txt'
        # Se pasta registro não existe iremos criá-la:
        if os.path.exists(pasta_registro) == False: os.mkdir('compliance_dados')
        # Se arquivo de registro não existe criá-lo:
        if os.path.exists(arquivo_registro) == False:
            print('Arquivo de registro não existe: Foi criado.')
            with open(arquivo_registro, 'w') as arquivo: pass
        # Salvar dados dentro do arquivo
        with io.open(pasta_registro+'/registro_mapa_frotas.txt', 'w') as arquivo:
            arquivo.write(str(data))
    caminho_mapa_equip = verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\CIA Analytics - BOT CIA\Extrator\SGPA3\Exportacao Monit Mapa.db')
    dt_arquivo_atual = datetime.fromtimestamp(os.path.getmtime(caminho_mapa_equip))
    if dt_arquivo_atual != ler_atualizacao_mapa_frotas():
        # Salvar o momento em que o mapa de frotas foi atualizado para ser próximo parâmetro de comparação.
        salvar_atualizacao_mapa_frotas()
        # Atualizar último registro de todas as frotas, lat/lon e demais dados.
        atualizar_historico_frotas()
        # Retorne positiva para a lógica prosseguir gatilho.
        return True
    return False

###### ADERENCIA analise de cenario CCT

def aderencia_analise_cenario_CCT():
    CAMINHO_db_analise_cenario_CCT = r'C:\CIAANALYTICS\1 - Producao\1 4 - Banco\analise_cenario_cct.db'
    CAMINHO_db_envio_msg = r'C:\CIAANALYTICS\1 - Producao\1 4 - Banco\envio_msg.db'
    #CAMINHO_db_analise_cenario_CCT = r'\\CSCLSFSR01\Agricola$\Logistica Agroindustrial\CIA 22.23\11. Analytics\1 4 - Banco\analise_cenario_cct.db'
    #CAMINHO_db_envio_msg = r'\\CSCLSFSR01\Agricola$\Logistica Agroindustrial\CIA 22.23\11. Analytics\1 4 - Banco\envio_msg.db'

    def gerar_tabela_aderencia_analise_cenario_CCT(dataframe):
        # Se pasta analise_cenario_CCT não existir, crie ela:
        if not os.path.exists('analise_cenario_CCT'): os.makedirs('analise_cenario_CCT')
        # Formatar tabela
        gat_cct_export_s = dataframe.style.set_table_styles([
            {'selector': 'th',
            'props': [('background-color', '#781E77'),
                    ('color', 'white')]}])
        gat_cct_export_s = gat_cct_export_s.set_properties(**{
            'text-align': 'center'})
        caminho_arquivo = 'analise_cenario_CCT/Acompanhamento_Gatilhos_Analise_de_Cenario_CCT.png'
        def style_index(row):
            if 'Turno A' in row.name[0]:
                return ['background-color: #c7c7c7; color: black'] * len(row)
            elif 'Turno B' in row.name[0]:
                return ['background-color: #ededed; color: black'] * len(row)
            elif 'Turno C' in row.name[0]:
                return ['background-color: #d4d4d4; color: black'] * len(row)
            else:
                return [''] * len(row)
        gat_cct_export_s = gat_cct_export_s.apply(style_index, axis=1, subset=pd.IndexSlice[:, :, :])
        # Gerar imagem tabela
        dfi.export(gat_cct_export_s, caminho_arquivo)
        # Devolver caminho absoluto do arquivo
        return os.path.abspath(caminho_arquivo)
    def carregar_base_respostas_analise_cenario_CCT(CAMINHO_db_analise_cenario_CCT):
        # Base de Analises de Cenario do CCT
        conn_analise_cen_cct = sqlite3.connect(CAMINHO_db_analise_cenario_CCT)
        res_cct = pd.read_sql('SELECT * FROM analise_cenario_cct', conn_analise_cen_cct)
        conn_analise_cen_cct.close()
        res_cct.gerada_em = [datetime.fromtimestamp(f) for f in res_cct.gerada_em]
        res_cct = res_cct[(res_cct.gerada_em >= datetime(2023,7,1))]
        res_cct = res_cct[res_cct.de_.str.contains('@c.us')]
        return res_cct
    def carregar_base_envio_mesagens_gatilho(CAMINHO_db_envio_msg):
        # Base de Analises de Cenario do CCT
        conn_env_msg_cct_ = sqlite3.connect(CAMINHO_db_envio_msg)
        gat_cct = pd.read_sql('''
                SELECT * 
                FROM envio_msg
                WHERE gerada_por = "CCT_Analise_Cenario"
                AND destino = "Contato"
                AND envio_status = 1''', conn_env_msg_cct_)
        conn_env_msg_cct_.close()
        gat_cct.gerada_em = pd.to_datetime(gat_cct.gerada_em)
        gat_cct = gat_cct[gat_cct.gerada_em >= datetime(2023,7,1)]
        gat_cct.reset_index(drop=True, inplace=True)
        gat_cct['DATA'] = [r.date() for r in gat_cct.gerada_em]
        gat_cct['HORA'] = [r.hour for r in gat_cct.gerada_em]
        gat_cct['TURNO'] = ['A' if(r.hour in [7,8,9,10,11,12,13,14]) else 'B' if(r.hour in [15,16,17,18,19,20,21,22]) else 'C' for r in gat_cct.gerada_em]
        gat_cct['Turno'] = [f"{(r-timedelta(hours=7)).day}/{(r-timedelta(hours=7)).month}" for r in gat_cct.gerada_em]
        gat_cct['Turno'] = 'Turno ' + gat_cct.TURNO.astype(str) + ' ' + gat_cct.Turno.astype(str)
        gat_cct['Unidade'] = [r.split('\n')[0].replace('📍 *GATILHO - Analise Cenário de ','')[:-1] for r in gat_cct.mensagem]
        gat_cct = gat_cct[~gat_cct.Unidade.isin['GASA', 'UNI', 'BENA', 'DEST', 'IPA', 'LEME']]
        gat_cct = gat_cct[~gat_cct['HORA'].isin([6,7,14,15,22,23])]
        polo_centro_sul = ['VALER','CAAR','MORRO','SELIS', 'RBRIL', 'PASSA','LPRAT','CONTI','LEME','JUN','BONF']
        gat_cct['Polo'] = ['Centro Sul' if r in polo_centro_sul else 'Leste & Oeste' for r in gat_cct.Unidade]
        gat_cct = gat_cct[~gat_cct.para_.str.contains('997366501|996063796|971030732')]
        gat_cct.drop(columns=['gerada_por','destino','anexo','envio_status'], inplace=True)
        return gat_cct
    def criar_mensagem_aderencia_analise_cenario_CCT(turno_anterior):
        link_historico = 'https://minhaticloud.sharepoint.com/:x:/s/CIAAnalytics/EY-NXSWVk2ZHmbAwYE4X1VkBLbSHnC3HY6jRJ7xA0q1HAg?e=SZXsJ1'
        comprimento = 'Bom dia!' if turno_anterior == 'C' else 'Boa tarde!' if turno_anterior == 'A' else 'Boa noite!' 
        texto_da_mensagem = f'{comprimento} segue aderência de respostas dos analistas CIA frente gatilhos de baixa autonomia gerados pelo BOT CIA das últimas 24 horas.\nPara verificar as respostas consulte o link: {link_historico}'
        return texto_da_mensagem

    res_cct = carregar_base_respostas_analise_cenario_CCT(CAMINHO_db_analise_cenario_CCT)
    gat_cct = carregar_base_envio_mesagens_gatilho(CAMINHO_db_envio_msg)
    # Coletando respostas
    resposta = []
    for id,gat in gat_cct.iterrows(): # Base Gatilho
        inicio_gatilho = gat.gerada_em
        if inicio_gatilho.hour in [11,12,13,18,19,20,2,3,4]:
            fim_gatilho = inicio_gatilho+timedelta(hours=1, minutes=45)
        else:
            fim_gatilho = inicio_gatilho+timedelta(hours=1, minutes=5)
        contato_gatilho = str(gat.para_)
        resp_db = res_cct[(res_cct.gerada_em > inicio_gatilho) & (res_cct.gerada_em < fim_gatilho) & (res_cct.de_.str.contains(contato_gatilho))]
        resposta.append(resp_db.mensagem.values)
    gat_cct['resp'] = resposta
    gat_cct['resp_OK'] = [True if len(r) > 0 else False for r in gat_cct.resp]
    #gat_cct = gat_cct[~gat_cct.para_.str.contains('997366501|996063796|999487792')]
    # Exportando histórico
    gat_cct.rename(columns={'gerada_em':'Inicio Gatilho','para_':'Para Celular','mensagem':'Descrição Gatilho','resp':'Resposta do Analista','resp_OK':'Houve resposta'}).to_excel(verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\CIA Analytics - CCT\Gatilhos_Analise_de_Cenario_CCT.xlsx'), index=False)
    # Mensagens recentes
    turno_anterior = 'C' if(datetime.now().hour in [7,8,9,10,11,12,13,14]) else 'A' if(datetime.now().hour in [15,16,17,18,19,20,21,22]) else 'B'
    agr = datetime.now()
    ont = datetime.now()-timedelta(days=1)
    momento_turno_ant = datetime(ont.year,ont.month,ont.day,23) if turno_anterior[0] == 'C' else datetime(agr.year,agr.month,agr.day,7) if turno_anterior[0] == 'A' else datetime(agr.year,agr.month,agr.day,15)
    gat_cct = gat_cct[(gat_cct.gerada_em >= momento_turno_ant-timedelta(hours=16)) & (gat_cct.gerada_em < momento_turno_ant+timedelta(hours=8))]
    def contar_resp_OK(row):
        eventos = row.resp_OK.count()
        eventos_OK = row.resp_OK.sum()
        porc = f"{round((eventos_OK / eventos)*100,1)}%"
        return pd.Series([porc, eventos, eventos_OK], index=['Aderência','Eventos','Eventos OK'])
    gat_cct_export = gat_cct.groupby(['Turno','Polo','Unidade']).apply(contar_resp_OK)
    # Envio de mensagem:
    texto_da_mensagem = criar_mensagem_aderencia_analise_cenario_CCT(turno_anterior)
    caminho_arquivo_aderencia_cen_cct = gerar_tabela_aderencia_analise_cenario_CCT(gat_cct_export)
    contato, tipo_contato = verificar_tipo_de_contato('BOT CIA - CCT')
    gravar_em_banco_para_envio([('CCT_Analise_Cenario_ADERENCIA',datetime.now(),contato, tipo_contato, texto_da_mensagem, caminho_arquivo_aderencia_cen_cct)])

################ Apontamentos Herbicida

def gerar_mensagens_herbicida():
    print('Inicio geração de apontamentos do Herbicida.')
    ARQUIVO_EXPORT_MONIT_F = verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\CIA Analytics - BOT CIA\Extrator\SGPA3\Exportacao Monit F.db')
    if not os.path.exists('Apontamento_Herbicida'): os.mkdir('Apontamento_Herbicida')
    CAMINHO_PASTA = r'Apontamento_Herbicida'

    def qual_data_arquivo_recente(caminho):
        data_recente = datetime(1999,3,12)
        for root, dirs, files in os.walk(caminho, topdown=False):
            for name in files:
                data_ = datetime.fromtimestamp(os.path.getmtime(os.path.join(root, name)))
                if data_ > data_recente:
                    data_recente = data_
        return data_recente
    
    print('Ultima atualização do herbicidade apontamentos: ', qual_data_arquivo_recente(CAMINHO_PASTA))
    if qual_data_arquivo_recente(CAMINHO_PASTA) < datetime.now()-timedelta(minutes=20):
        def carregar_grupos_hb():
            contatoHB = pd.read_excel(r'\\CSCLSFSR01\Agricola$\Logistica Agroindustrial\CIA 22.23\11. Analytics\BOT CIA\Contatos\PROD_Apontamento.xlsx', sheet_name='HB')
            contatoHB = contatoHB[contatoHB.Ativo=='SIM'].drop_duplicates().dropna()
            contatoHB = dict(zip(contatoHB.Frente, contatoHB.Grupo))
            return contatoHB

        def gerar_imagem_preparo(dataframe_alvo, nome_a_ser_salvo):
            estilo_colunas = [
                    {'selector': '.col0', 'props': [('width', '150px')]},
                    {'selector': '.col1', 'props': [('width', '140px')]},
                    {'selector': '.col3', 'props': [('width', '210px')]},
                    {'selector': '.col4', 'props': [('width', '210px')]}
                ]
            estilo_centralizado = {'selector': 'th, td',
                                'props': [('text-align', 'center'), ('font-family', 'sans-serif')]}
            estilo_cabecalho = {'selector': 'th',
                                'props': [('font-weight', 'bold'), ('font-family', 'sans-serif'),
                                        ('background-color', '#781E77'), ('color', 'white')]}     
            df_style = dataframe_alvo.style \
                .hide(axis="index") \
                .set_table_styles(estilo_colunas + [estilo_centralizado, estilo_cabecalho])
            html = df_style.to_html()
            #display(HTML(html)) 
            options = {'format': 'png','quiet': ''}
            caminho_robo = r'C:\ciaanalytics\Python 3\imgkit\wkhtmltopdf\bin\wkhtmltoimage.exe'
            img_path = caminho_robo #r'C:\Users\ciaanalytics\Downloads\Python3\imgkit\wkhtmltopdf\bin\wkhtmltoimage.exe'
            imgkit.from_string(html, nome_a_ser_salvo, options=options, config=imgkit.config(wkhtmltoimage=img_path))
            return os.path.abspath(nome_a_ser_salvo)

        # Base de dados
        conn_em = sqlite3.Connection(ARQUIVO_EXPORT_MONIT_F)
        df_com = pd.read_sql('SELECT * FROM "Exportacao Monit F"', conn_em)
        conn_em.close()
        df_com = df_com[df_com.Grupo.str.contains('-HB-')]
        df_com['Data/Hora'] = pd.to_datetime(df_com['Data/Hora'])
        df_com = df_com.sort_values(by='Data/Hora', ascending=False)
        df_com = df_com.drop_duplicates(subset='Equipamento', keep='first') 
        df_com = df_com.reset_index(drop=True)
        if 'index' in df_com.columns: df_com.drop(columns='index', inplace=False)
        df_com = df_com[['Equipamento','Grupo','Operacao','Data/Hora']]
        df_com = df_com.rename(columns={
                'Equipamento':'Frota',
                'Grupo':'Frente',
                'Operacao':'Apontamento',
                'Data/Hora':'Última Comunicação'})
        data_modificacao_arquivo = datetime.fromtimestamp(os.path.getmtime(ARQUIVO_EXPORT_MONIT_F))
        df_com['Tempo sem comunicar'] = [data_modificacao_arquivo-r for r in df_com['Última Comunicação']]

        def duracao_tempo(row):
            valor = row['Tempo sem comunicar']
            dias = valor.days
            hh,mm,ss = str(valor).split(' ')[-1].split('.')[0].split(':')
            return str((dias * 24) + int(hh)) + ":" + ':'.join([mm,ss])

        df_com['Tempo sem comunicar'] = df_com.apply(duracao_tempo, axis=1)
        df_com = df_com.sort_values(by=['Frente','Última Comunicação'], ascending=False)
        df_com['Última Comunicação'] = df_com['Última Comunicação'].dt.strftime('%d/%m/%Y %H:%M:%S')
        # Grupos HB
        GruposHB = carregar_grupos_hb()
        # Geraçcao de imagens
        
        for frente in df_com.Frente.unique(): #
            undCod = frente.split('-')[0]
            if undCod in GruposHB.keys():
                mensagem = f'🚜 *Tratores {frente}:* Apontamentos e Comunicação'
                ParaGrupoHB = GruposHB[frente.split('-')[0]]
                caminho_anexo = gerar_imagem_preparo(df_com[df_com.Frente==frente], f'Apontamento_Herbicida\\{frente}.png')
                contato, tipo_contato = verificar_tipo_de_contato(ParaGrupoHB)
                gravar_em_banco_para_envio([('PROD_Apontamento_Tratos',datetime.now(),contato, tipo_contato, mensagem, caminho_anexo)])
            else: print(f'Unidade {undCod} sem grupo!')
        mensagem = f'🚜 *Tratores Tratos:* Apontamentos e Comunicação'
        caminho_anexo = gerar_imagem_preparo(df_com, 'Apontamento_Herbicida\\resumo.png')
        contato, tipo_contato = verificar_tipo_de_contato('19 99847-9246')
        gravar_em_banco_para_envio([('PROD_Apontamento_Tratos', datetime.now(), contato, tipo_contato, mensagem, caminho_anexo)])
        contato, tipo_contato = verificar_tipo_de_contato('CIA Produção ID999')
        gravar_em_banco_para_envio([('PROD_Apontamento_Tratos', datetime.now(), contato, tipo_contato, mensagem, caminho_anexo)])
    else:
        print('Apontamentos de Harbicidas pulados! Foram gerados a pouco tempo.')

#### Lógica OS Aguardando Informação PMA

def logica_os_ag_info():
    CAMINHO_BASE_PMA = verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\CIA Analytics - BOT CIA\Extrator\Manutencao\PMA\PMA_site.xlsx')

    if not os.path.exists('Controle_PMA'):
        os.makedirs('Controle_PMA')
        print("Criamos diretório: 'Controle_PMA'.")
    if not os.path.exists('Controle_PMA/registro_PMA.json'):
        dict_exemplo_pma = {
            'numero_frota':datetime.now().isoformat()}
        with open(os.path.join(os.getcwd(), "Controle_PMA\\registro_PMA.json"), "w") as file:
            for key in dict_exemplo_pma.keys():
                if type(dict_exemplo_pma[key]) == datetime:
                    dict_exemplo_pma[key] = dict_exemplo_pma[key].isoformat()
            json.dump(dict_exemplo_pma, file)
        print("Criamos diretório: 'Controle_PMA\\registro_PMA.json'.")

    def salvar_registro_pma_json(dict_registro_pma):
        with open(os.path.join(os.getcwd(), "Controle_PMA\\registro_PMA.json"), "w") as file:
            for key in dict_registro_pma.keys():
                if type(dict_registro_pma[key]) == datetime:
                    dict_registro_pma[key] = dict_registro_pma[key].isoformat()
            json.dump(dict_registro_pma, file)

    def carregar_registro_pma_json():
        try:
            with open(os.path.join(os.getcwd(), "Controle_PMA\\registro_PMA.json"), "r") as file:
                loaded_data = json.load(file)
                for key in loaded_data.keys():
                        if type(loaded_data[key]) == str:
                            loaded_data[key] = datetime.fromisoformat(loaded_data[key])
            return loaded_data
        except:
            os.remove('Controle_PMA\\registro_PMA.json')

    registro_pma = carregar_registro_pma_json()

    def base_gatilho_os_ag_info(df_alvo):
        base_ag = df_alvo
        # Tratamento de base
        base_ag = base_ag.dropna(subset='DS_DESCRICAO_TRABALHO')
        base_ag.DS_DESCRICAO_TRABALHO = base_ag.DS_DESCRICAO_TRABALHO.str.lower()
        possiveis_ag_info = ['ag inf','aguardando inf','ag. inf','aguard. inf',
                             'aguardando anformações','aguardando informacoes']
        base_ag = base_ag[(base_ag.DS_DESCRICAO_TRABALHO.str.contains('|'.join(possiveis_ag_info))) & ~base_ag.DS_DESCRICAO_TRABALHO.str.contains('alterada')]
        base_ag = base_ag.dropna(subset='CD_OS')
        base_ag["DH_INICIO_OPERACAO"] = pd.to_datetime(base_ag["DH_INICIO_OPERACAO"])
        for id, row in base_ag.iterrows():
            if str(row.FRENTE)[:3] in 'CAA|CAR|PTP|RBR':
                row.DH_INICIO_OPERACAO = row.DH_INICIO_OPERACAO + timedelta(1/24) #adicionando 1h pros casos de MS fuso horário

        '''def controle_os_ag_info():
            und_controle_pma = pd.read_excel(r'\\CSCLSFSR01\Agricola$\Logistica Agroindustrial\CIA 22.23\11. Analytics\BOT CIA\Contatos\MANUT_OS_Aguardando_Info.xlsx', sheet_name='Controle_UND')
            return '|'.join(list(und_controle_pma.COD_UND)), dict(zip(und_controle_pma.COD_UND,und_controle_pma.Celular))'''

        #lista_und_tratar, contatos_ag = controle_os_ag_info()

        base_ag['DELTA'] = [datetime.now()-x for x in base_ag.DH_INICIO_OPERACAO]
        base_ag2 = base_ag
        base_ag = base_ag[base_ag.DELTA >= timedelta(1/24)]
        # Filtrar Modelo FIXO
        base_ag.MODELO = base_ag.MODELO.str.lower()
        #base_ag = base_ag[base_ag.MODELO.str.contains('colhe')]
        # Filtrar Unidade por código da frente
        # base_ag = base_ag[base_ag.FRENTE.str.contains(lista_und_tratar)]

        return base_ag 
    
    def base_gatilho_os_ag_info_finalizado(df_alvo):
        base_ag = df_alvo
        # Tratamento de base
        base_ag = base_ag.dropna(subset='DS_DESCRICAO_TRABALHO')
        base_ag.DS_DESCRICAO_TRABALHO = base_ag.DS_DESCRICAO_TRABALHO.str.lower()
        possiveis_ag_info = ['ag inf','aguardando inf','ag. inf','aguard. inf',
                             'aguardando anformações','aguardando informacoes']
        base_ag = base_ag[(base_ag.DS_DESCRICAO_TRABALHO.str.contains('|'.join(possiveis_ag_info))) & ~base_ag.DS_DESCRICAO_TRABALHO.str.contains('alterada')]
        base_ag = base_ag.dropna(subset='CD_OS')
        base_ag["DH_INICIO_OPERACAO"] = pd.to_datetime(base_ag["DH_INICIO_OPERACAO"])
        for id, row in base_ag.iterrows():
            if str(row.FRENTE)[:3] in 'CAA|CAR|PTP|RBR':
                row.DH_INICIO_OPERACAO = row.DH_INICIO_OPERACAO + timedelta(1/24) #adicionando 1h pros casos de MS fuso horário

        '''def controle_os_ag_info():
            und_controle_pma = pd.read_excel(r'\\CSCLSFSR01\Agricola$\Logistica Agroindustrial\CIA 22.23\11. Analytics\BOT CIA\Contatos\MANUT_OS_Aguardando_Info.xlsx', sheet_name='Controle_UND')
            return '|'.join(list(und_controle_pma.COD_UND)), dict(zip(und_controle_pma.COD_UND,und_controle_pma.Celular))'''

        #lista_und_tratar, contatos_ag = controle_os_ag_info()

        base_ag['DELTA'] = [datetime.now()-x for x in base_ag.DH_INICIO_OPERACAO]
        base_ag2 = base_ag
       # base_ag = base_ag[base_ag.DELTA >= timedelta(1/24)]
        # Filtrar Modelo FIXO
        base_ag.MODELO = base_ag.MODELO.str.lower()
        #base_ag = base_ag[base_ag.MODELO.str.contains('colhe')]
        # Filtrar Unidade por código da frente
        # base_ag = base_ag[base_ag.FRENTE.str.contains(lista_und_tratar)]

        return base_ag2

    def mensagem_os_ag_info(el_d):
        num_frota = el_d.CD_EQUIPAMENTO
        num_os = int(el_d.CD_OS)
        tipo_frota = el_d.TIPO
        frente = el_d.FRENTE
        apontamento = el_d.DS_OPERACAO
        informacao = el_d.DS_DESCRICAO_TRABALHO
        inicio_operacao = el_d.DH_INICIO_OPERACAO.to_pydatetime()
        inicio_operacao_str = inicio_operacao.strftime('%d/%m/%Y %H:%M:%S')
        racional = str(datetime.now()-inicio_operacao).split('.')[0]
        mensagem = f"""🚜💬 *OS Aguardando informação*\n*Frota:* {num_frota} - {tipo_frota}\n*OS:* {num_os} - {frente}\n*Apontando:* {apontamento}\n*Inicio Apontamento:* {inicio_operacao_str}\n*Informação PMA:* {informacao}\n\n❗ *Tempo sem informação:* {racional}"""# \n\nRealizar scalation de informação para a unidade."""
        return mensagem

    base_pma = pd.read_excel(CAMINHO_BASE_PMA)
    base_pma = base_pma.drop_duplicates(subset=['CD_EQUIPAMENTO'])
    base_ag = base_gatilho_os_ag_info(base_pma)

    for id, row in base_ag.iterrows():
        if str(row.CD_EQUIPAMENTO) in registro_pma.keys() and registro_pma[str(row.CD_EQUIPAMENTO)] > datetime.now():
            #print('Frota existe em registro, gatilho já acionado')
            pass
        else:
            registro_pma[str(row.CD_EQUIPAMENTO)] = (datetime.now()+timedelta(hours=1)).isoformat()
            #print('Frota adicionada em registro')
            msg_pma = mensagem_os_ag_info(row)
            '''try: cont_ = str(contatos_ag[row.FRENTE.split('-')[0]])
            except: cont_ = '19 998326554'
            contato, tipo_contato = verificar_tipo_de_contato(cont_)
            gravar_em_banco_para_envio([('MANUT_OS_Ag_Info', datetime.now(), contato, tipo_contato, msg_pma, '')])'''
            contato, tipo_contato = verificar_tipo_de_contato('BOT CIA - Manut. & Comb.')
            gravar_em_banco_para_envio([('MANUT_OS_Ag_Info', datetime.now(), contato, tipo_contato, msg_pma, '')])

    salvar_registro_pma_json(registro_pma)
    


###### Teste de Report Moagem


def gerar_mensagem_report_colheita_rapido():
    und_hora_dif = 'PASSA|RBRIL|CAAR'
    de_para_und = {
        'Zanin': 'ZANIN', 'Bonfim': 'BONF', 'Junqueira': 'JUN', 'Serra': 'SERRA',
        'Paraíso': 'PARAI', 'Santa Cândida': 'SCAND', 'Barra': 'BARRA', 
        'Diamante': 'DIA', 'Costa Pinto': 'COPI', 'Rafard': 'RAF',
        'São Francisco': 'IASF', 'Jataí': 'JATAI', 'Gasa': 'GASA',
        'Benálcool': 'BENA', 'Destivale': 'DEST', 'Mundial': 'MUND',
        'Univalem': 'UNI', 'Ipaussu': 'IPA', 'Maracaí': 'MARA', 'Paraguaçú': 'PARAG',
        'Tarumã': 'TARU', 'Caarapó': 'CAAR', 'Passa Tempo': 'PASSA',
        'Lagoa da Prata': 'LPRAT', 'Leme': 'LEME', 'Rio Brilhante': 'RBRIL',
        'Santa Elisa': 'SELIS', 'Vale do Rosário': 'VALER', 'Continental': 'CONTI',
        'Morro Agudo': 'MORRO'}

    # META MOAGEM
    meta_moagem = pd.read_excel(verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\Metas_Gov_Indicadores - Documentos\CCT\Parâmetros Relatórios CCT (Metas).xlsx'), sheet_name='Metas')
    meta_moagem = dict(zip(list(meta_moagem['UNIDADE']),list(meta_moagem['Meta Moagem TCD'])))
    meta_moagem['MORRO'] = meta_moagem['UMB']
    meta_moagem['DDC'] = meta_moagem['DIA']
    meta_moagem['COP'] = meta_moagem['COPI']
    meta_moagem['PASSA'] = meta_moagem['PTP']
    meta_moagem['LEME'] = meta_moagem['LEM']
    meta_moagem['SELIS'] = meta_moagem['SEL']
    meta_moagem['VALER'] = meta_moagem['VRO']
    meta_moagem['LPRAT'] = meta_moagem['LPT']
    meta_moagem['CONTI'] = meta_moagem['CNT']
    meta_moagem['RBRIL'] = meta_moagem['RBR']

    piv_moagem = pd.read_excel(verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\CIA Analytics - BOT CIA\Extrator\PI System\dados_moenda.xlsx'))
    piv_moagem = piv_moagem.sort_values(by='Time', ascending=False)
    piv_moagem = piv_moagem.drop_duplicates(subset=['Nome','Tipo'])
    piv_moagem['escrita'] = piv_moagem['Tipo'].astype(str) + ": " + piv_moagem['Value'].astype(str)
    piv_moagem['escrita'] = [f'{row} RPM' if 'moenda' in row.lower() else f'{row} M/min' for row in piv_moagem['escrita']]
    piv_moagem = piv_moagem[['Nome','escrita']]
    velocidades_und = dict([(und,'\n'.join(piv_moagem[piv_moagem.Nome==und].escrita.str.replace("\xa0",'.').values)) for und in piv_moagem.Nome.unique()])

    # Base de Pátio (Interno, Externo)
    patio_PI = pd.read_excel(verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\CIA Analytics - BOT CIA\Extrator\PI System\dados_cargas.xlsx'))
    patio_PI.UND = patio_PI.UND.map(de_para_und)
    patio_excecoes = pd.read_excel(verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\Metas_Gov_Indicadores - Documentos\CCT\Parâmetros Relatórios CCT (Rotina).xlsx'), sheet_name='Pátio Externo FF')
    patio_excecoes = patio_excecoes.fillna(0)
    hora_ant = (datetime.now()-timedelta(hours=1)).hour
    try:  ext_jatai = patio_excecoes[(patio_excecoes["Unid."]=='JATAI') & (patio_excecoes["Hora"]==hora_ant)][["Cargas FF Picada - Ext.","Cargas FF Inteira - Ext."]].values.sum()
    except: ext_jatai = 0
    try: 
        jun1 = patio_excecoes[(patio_excecoes["Unid."]=='JATAI') & (patio_excecoes["Hora"]==hora_ant)][["RESUMO DE CARGAS PP - EXTERNO JUNQUEIRA"]].values.sum()
        jun2 = patio_excecoes[(patio_excecoes["Unid."]=='JUN') & (patio_excecoes["Hora"]==hora_ant)][["Cargas FF Picada - Ext.","Cargas FF Inteira - Ext."]].values.sum()
        ext_jun = jun1 + jun2
    except: ext_jun = 0
    patio_PI.loc[(patio_PI['UND'] == 'JUN') & (patio_PI['LOCAL'] == 'Cargas Pátio Externo'), 'Value'] = ext_jun
    patio_PI.loc[(patio_PI['UND'] == 'JATAI') & (patio_PI['LOCAL'] == 'Cargas Pátio Externo'), 'Value'] = ext_jatai
    patio_PI_rel = patio_PI.groupby(['UND']).apply(lambda x: (x[x['LOCAL']=='Cargas Pátio Interno'].Value.sum(), x[x['LOCAL']=='Cargas Pátio Externo'].Value.sum()))

    # Base Moagem (AcumuloDia, Media3h, MoagemHAtual, ProjecaoDia)
    ton_PI = pd.read_excel(verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\CIA Analytics - BOT CIA\Extrator\PI System\dados_moagem.xlsx'))
    if 'Unnamed: 0' in ton_PI.columns:
        ton_PI = ton_PI.rename(columns={'Unnamed: 0':'UND'})
    ton_PI['UND'] = ton_PI['UND'].map(de_para_und)
    ton_PI = ton_PI.dropna(subset='UND')
    #ton_PI['ProjecaoDia'] = round(ton_PI['AcumuladoDia'] + (ton_PI['Acumulado3h'] / 3 * (24 - datetime.now().hour)))
    und_pi_fuso = ['Caarapó','CAAR','Passa Tempo','PASSA','Rio Brilhante','RBRIL']
    ton_PI['ProjecaoDia'] = [round(acum+(acum3/3*(24 - datetime.now().hour))) if und not in und_pi_fuso else round(acum+(acum3/3*(25 - (datetime.now().hour)))) for und,acum,acum3 in zip(ton_PI['UND'],ton_PI['AcumuladoDia'],ton_PI['Acumulado3h'])]
    ton_PI['ProjecaoIcone'] = ['✅' if row[1]>row[2] else '🔻' for row in [(und,proj,meta_moagem[und] if und in meta_moagem.keys() else 0) for (und,proj) in list(zip(ton_PI.UND,ton_PI.ProjecaoDia))]]
    moagem_PI = ton_PI.groupby(['UND']).apply(lambda x: (x.AcumuladoDia.item(), x.Acumulado3h.item(), x.Moagem1h.item(), f'{round(x.ProjecaoDia.item())} {x.ProjecaoIcone.item()}'))

    # Caminhões em T's (t1, t2, t3)
    def sub_formatar_base(caminho_downloads,tipo_arquivo):
        global arquivo_mais_recente
        folder_path = caminho_downloads
        file_type =  tipo_arquivo
        files = glob.glob(folder_path + file_type)
        arquivo_mais_recente = max(files, key=os.path.getctime)

    ## Base: 'REF segunda func CCT.xslx'
    caminho_arquiv_suport_cct_2 = r'\\CSCLSFSR01\Agricola$\Logistica Agroindustrial\CIA 22.23\11. Analytics\BOT CIA\Contatos\REF segunda func CCT.xlsx'
    # Lista Unidades
    df_referencia_cct = pd.read_excel(caminho_arquiv_suport_cct_2, sheet_name='unidade')
    dict_ref_und_cct = dict(zip(df_referencia_cct.INSTANCIA, df_referencia_cct.REF1))
    inv_dict_ref_und_cct = dict(zip(df_referencia_cct.REF1, df_referencia_cct.INSTANCIA))
    for key in list(inv_dict_ref_und_cct.keys()): inv_dict_ref_und_cct[key.upper()] = inv_dict_ref_und_cct[key]
    # Lista Canavieiros
    lista_caminhoes = pd.read_excel(caminho_arquiv_suport_cct_2, sheet_name='canavieiro')
    lista_caminhoes = list(lista_caminhoes.CANAVIEIROS)

    # AGREGADOS:
    ap_cam_t1 = ['895 - Deslocamento Vazio']
    ap_cam_t2 = ['779 - Carregamento de cana p/ moagem', '779 - Carregamento', '891 - Troca Carretas - BV Campo', '886 - Aguardando carregamento', '886 - Aguardando Transbordo']
    ap_cam_t3 = ['881 - Deslocamento Carregado', '1068 - Enlonamento']

    # APONTAMENTOS: Traga as comunicações, filtes os canavieiros e filtre somente os mais recentes.
    sub_formatar_base(r'\\CSCLSFSR03\SoftsPRD\Extrator\PRD\CCT\Apontamentos Atual','\*csv')
    check_x = 0
    while check_x < 1:
        try:
            df_apontamento_atual = pd.read_csv(arquivo_mais_recente, encoding="ISO-8859-1", sep=';', on_bad_lines='skip')
            check_x = 1
        except IndexError:
            sleep(0.5)
            print(f'{datetime.now()} --> Erro em base APONTAMENTO ATUAL\n')

    df_apontamento_atual.DESC_UNIDADE = df_apontamento_atual.DESC_UNIDADE.replace(inv_dict_ref_und_cct)
    df_apontamento_atual = df_apontamento_atual[df_apontamento_atual['DESC_GRUPO_EQUIPAMENTO'].str.contains('-LN-|-BV-')]
    df_apontamento_atual.ULTIMA_COMUNICACAO = pd.to_datetime(df_apontamento_atual.ULTIMA_COMUNICACAO, dayfirst=True, errors='ignore')
    df_apontamento_atual = df_apontamento_atual[((df_apontamento_atual.DESC_UNIDADE.str.contains(und_hora_dif))
                                                & (df_apontamento_atual.ULTIMA_COMUNICACAO > datetime.now()-timedelta(hours=2, minutes=datetime.now().minute+.001)))
                                                |
                                                ((~df_apontamento_atual.DESC_UNIDADE.str.contains(und_hora_dif))
                                                & (df_apontamento_atual.ULTIMA_COMUNICACAO > datetime.now()-timedelta(hours=1, minutes=datetime.now().minute+.001)))]

    rel_cam_t = df_apontamento_atual.groupby('DESC_UNIDADE').apply(lambda x: (x[x['DESC_OPERACAO'].isin(ap_cam_t1)]['DESC_UNIDADE'].count(),x[x['DESC_OPERACAO'].isin(ap_cam_t2)]['DESC_UNIDADE'].count(),x[x['DESC_OPERACAO'].isin(ap_cam_t3)]['DESC_UNIDADE'].count()))

    # Geração da mensagem
    def geracao_mensagem_5(unidade,kit_moagem, kit_patio, kit_cam):
        escrita_final = "\n"+velocidades_und['Lagoda da Prata']+"\n" if unidade == "LPRAT" else ""
        hora_atual = (datetime.now()-timedelta(hours=1)).hour if unidade in und_hora_dif else datetime.now().hour
        dados_ate = (datetime.now()-timedelta(hours=2)).hour if unidade in und_hora_dif else (datetime.now()-timedelta(hours=1)).hour
        mensagem = f'''🏭 *Report {unidade} {hora_atual}h*\nPROJEÇÃO [3h]: {kit_moagem[3]} ton\nMoagem Atual [Dia]: {round(kit_moagem[0])} ton\nMoagem {dados_ate}h: {round(kit_moagem[2])} ton\nMoagem med. 3h : {round(kit_moagem[1]/3 if kit_moagem[1] > 0 else 0)} ton\n\n📌 *Cargas em Pátio*\nCargas Externo: {str(kit_patio[1]).split('.')[0]}\nCargas Interno: {str(kit_patio[0]).split('.')[0]}\n\n🚚 *Caminhões*\nT1: {kit_cam[0]}\nT2: {kit_cam[1]}\nT3: {kit_cam[2]}\nMoagem CM {escrita_final}h: {round(kit_moagem[2]/60)}\n\n_Dados até {dados_ate}h59._''' #T1: {kit_cam[0]}\nT2: {kit_cam[1]}\nT3: {kit_cam[2]}\n
        return  mensagem

    contatos_envio_rep_c = pd.read_excel(r'\\CSCLSFSR01\Agricola$\Logistica Agroindustrial\CIA 22.23\11. Analytics\BOT CIA\Contatos\REF segunda func CCT.xlsx', sheet_name='contatos_n')
    contatos_envio_rep_c = contatos_envio_rep_c[['INSTANCIA','CONTATO_n']]
    contatos_envio_rep_c.dropna(inplace=True)
    contatos_envio_rep_c = dict(zip(contatos_envio_rep_c.INSTANCIA,contatos_envio_rep_c.CONTATO_n))
    for und_ in contatos_envio_rep_c.keys():
        contato_destino = contatos_envio_rep_c[und_]
        relacao_cam_check = rel_cam_t[und_] if und_ in rel_cam_t.keys() else (0,0,0)
        mensagem = geracao_mensagem_5(und_,moagem_PI[und_],patio_PI_rel[und_],relacao_cam_check)
        for grupo_alvo in contato_destino.split('/'):
                try:
                    contato, tipo_contato = verificar_tipo_de_contato(grupo_alvo)
                    gravar_em_banco_para_envio([('CCT_Report_Rapido_Colheita',datetime.now(),contato, tipo_contato, mensagem, '')])    
                    pass
                except: print('Erro para envio de report rápido em grupo:',grupo_alvo)

################ TRAVA IFROTA MANUTENCAO

CAMINHO_MANUTENCAO_IFROTA = r'\\csclsfsr03\SoftsPRD\Extrator\PRD\Logistica Agroindustrial\CIA\MANUTENCAO CD IFROTA'

def manut_ifrota_trava():
    try: 
        def get_latest_csv_file(directory_path):
            # Lista todos os arquivos .csv no diretório
            all_csv_files = [file for file in os.listdir(directory_path) if file.endswith('.csv')]

            # Retorna None se não houver nenhum arquivo .csv
            if not all_csv_files:
                return None

            # Ordena os arquivos com base na data de modificação (mais recente primeiro) e retorna o primeiro
            latest_csv_file = max(all_csv_files, key=lambda file: os.path.getmtime(os.path.join(directory_path, file)))
            
            return os.path.join(directory_path, latest_csv_file)

        latest_file = get_latest_csv_file(CAMINHO_MANUTENCAO_IFROTA)
        trava_momento_manut_ifrota = datetime.fromtimestamp(os.path.getmtime(latest_file))
        return trava_momento_manut_ifrota
    except: 
        print('Não conseguimos atualizar o momento da última base trava manut ifrota.')
        return False
    
def gerar_mensagens_MANUTENCAO_IFROTA():
    print('Iniciando: gerar_mensagens_MANUTENCAO_IFROTA')
    def get_latest_csv_file(directory_path):
        # Lista todos os arquivos .csv no diretório
        all_csv_files = [file for file in os.listdir(directory_path) if file.endswith('.csv')]

        # Retorna None se não houver nenhum arquivo .csv
        if not all_csv_files:
            return None

        # Ordena os arquivos com base na data de modificação (mais recente primeiro) e retorna o primeiro
        latest_csv_file = max(all_csv_files, key=lambda file: os.path.getmtime(os.path.join(directory_path, file)))
        
        return os.path.join(directory_path, latest_csv_file)

    latest_file = get_latest_csv_file(CAMINHO_MANUTENCAO_IFROTA)

    man_i = pd.read_csv(latest_file, sep=';')

    for id, row in man_i.iterrows():
        para_contato = 'BOT CIA - CCT'
        mensagem = (f'''❗❗ *Atenção* ❗❗
    *Unidade*: {row.NOME}
    *Frota*: {row.NUMERO} / *Tipo*: {row['NOME.2']}

    Temos uma ocorrência do Ifrota tentado fechar a manutenção deste equipamento sem sucesso.

    Favor verificar o apontamento e se necessário fechar a manutenção manualmente!''')
        contato, tipo_contato = verificar_tipo_de_contato(para_contato) #CIA CCT
        gravar_em_banco_para_envio([('CCT_MANUT_Ifrota', datetime.now(), contato, tipo_contato, mensagem, '')])
        contato, tipo_contato = verificar_tipo_de_contato('CIA CCT')
        gravar_em_banco_para_envio([('CCT_MANUT_Ifrota', datetime.now(), contato, tipo_contato, mensagem, '')])
        # print(mensagem)

######## Monitoramento Sem Apontamento SGPA3 Bacchi

def sem_apontamentoSGPA3():
    frota = 'Número do Equipamento'
    duracao = 'Tempo em atividade'
    colunas = ['Número do Equipamento','Frente associada','Registro mais recente','Tempo em atividade','Atividade','Tipo do equipamento']
    apontamentos = ['834 - Sem apontamento']
    caminho = verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\CIA Analytics - BOT CIA\Extrator\SGPA3\monitoramento_sgpa3.xlsx')
    caminho_dados = 'dadosSemApt.json'
    caminho_dic = r'\\CSCLSFSR01\Agricola$\Logistica Agroindustrial\CIA 22.23\11. Analytics\BOT CIA\Contatos\Segunda_Funcao_Prod.xlsx'


    def import_base(caminho,colunas,apontamentos):
        base = pd.read_excel(caminho)
        base_filtro = base[base['Atividade'].isin(apontamentos) ][colunas].reset_index(drop=True)
        return base_filtro

    def dados_ver(caminho_dados,colunas):
        if not os.path.exists(caminho_dados):
            df_dados = pd.DataFrame(columns=colunas)
            df_dados.to_json(caminho_dados, orient='records',lines=True)

    def convert_delta(delta_str):
        delta_str = delta_str.replace("DIA(S)", "days").replace(" ", "")
        delta = pd.to_timedelta(delta_str)
        return delta


    def envio(row,dic):
        duracao = 'Tempo em atividade'
        #if (row[duracao] <300):
            # print('Sem Envio \n') 
        if (row['Frente associada'][:6] in dic['CONTROLE_SemApontamento']):
            if(dic['CONTROLE_SemApontamento'][row['Frente associada'][:6]] =='SIM' ):
                if (row[duracao] >= 300) & (row[duracao] < 3600) & (row['Envio_1'] == False):
                    mensagem = mostrar(row) 
                    contato, tipo_contato = verificar_tipo_de_contato(dic['Torre_Numero'][row['Frente associada'][:6]]) #TORRE
                    gravar_em_banco_para_envio([('CCT_MANUT_Ifrota', datetime.now(), contato, tipo_contato, mensagem, '')])
                    row['Envio_1'] = True
                elif (row[duracao] >= 3600):
                    op = int(row[duracao]/3600)
                    if (row['Envio_2'] < op ):

                        mensagem = mostrar(row) 
                        contato, tipo_contato = verificar_tipo_de_contato(dic['Torre_Numero'][row['Frente associada'][:6]]) #TORRE
                        gravar_em_banco_para_envio([('CCT_MANUT_Ifrota', datetime.now(), contato, tipo_contato, mensagem, '')])
                        
                        contato, tipo_contato = verificar_tipo_de_contato('11963208908')#BECK
                        gravar_em_banco_para_envio([('CCT_MANUT_Ifrota', datetime.now(), contato, tipo_contato, mensagem, '')]) 
                        contato, tipo_contato = verificar_tipo_de_contato('19997441803') #JEV
                        gravar_em_banco_para_envio([('CCT_MANUT_Ifrota', datetime.now(), contato, tipo_contato, mensagem, '')])
                        contato, tipo_contato = verificar_tipo_de_contato('19998561495') #MANU
                        gravar_em_banco_para_envio([('CCT_MANUT_Ifrota', datetime.now(), contato, tipo_contato, mensagem, '')])
                        if datetime.now().hour>=7 and datetime.now().hour<=14:
                            contato, tipo_contato = verificar_tipo_de_contato('14998583973') #PROD A
                            gravar_em_banco_para_envio([('CCT_MANUT_Ifrota', datetime.now(), contato, tipo_contato, mensagem, '')])
                            contato, tipo_contato = verificar_tipo_de_contato('19996823186') #ANDERSON
                            gravar_em_banco_para_envio([('CCT_MANUT_Ifrota', datetime.now(), contato, tipo_contato, mensagem, '')])
                        if datetime.now().hour>=15 and datetime.now().hour<=22:
                            contato, tipo_contato = verificar_tipo_de_contato('19997244537') #PROD B
                            gravar_em_banco_para_envio([('CCT_MANUT_Ifrota', datetime.now(), contato, tipo_contato, mensagem, '')])
                        if datetime.now().hour>=23 and datetime.now().hour<=6:
                            contato, tipo_contato = verificar_tipo_de_contato('19996679285') #PROD C
                            gravar_em_banco_para_envio([('CCT_MANUT_Ifrota', datetime.now(), contato, tipo_contato, mensagem, '')])

                        
                        row['Envio_1'] = True
                        row['Envio_2'] = op

                        '''PLANTADORA MAG100	PL
                            COLHEDORA	MU
                            TRATOR DE PNEU LEVE/MAG 100 	VN
                            PLANTADORA MAG100	PR
                            TRATOR DE PNEU LEVE/MAG 100 	PR
                            TRATOR DE PNEU LEVE/MAG 100 	BT'''

        return row


    def mostrar(row):
        valores = (row['Frente associada'],row['Número do Equipamento'],row['Registro mais recente'], str(pd.to_timedelta(row['Tempo em atividade'],unit='s')).replace('days','dia(s)'))
        return '⚠️ *Atenção:* Frota *Sem Apontamento*\n*Frente:* %s\n*Frota:* %d\n⏱ *Início do Apontamento:* %s\n⏱ *Duração:* %s \n'% valores

    def atualizar(row,df):
        df = df.set_index('Número do Equipamento')
        ind = row['Número do Equipamento']
        row['Tempo em atividade'] = df.loc[ind,'Tempo em atividade']
        return row

    #dicionário de numeros de telefone
    num = pd.read_excel(caminho_dic)
    num = num[['Sigla_Unidade','Sigla_Frente','Torre_Numero','CONTROLE_SemApontamento']]
    num['Frente'] = num['Sigla_Unidade'] +'-'+num['Sigla_Frente']
    num = num.drop(columns=['Sigla_Unidade','Sigla_Frente'])
    num.set_index('Frente', inplace=True)
    dic_num = num.to_dict()
    #print(dic_num)

    dados_ver(caminho_dados,colunas)

    df_excel = import_base(caminho,colunas,apontamentos)
    filtro_equipamento = ['PLANTADORA MAG100', 'COLHEDORA', 'TRATOR DE PNEU LEVE/MAG 100']
    df_excel = df_excel[df_excel["Tipo do equipamento"].isin (filtro_equipamento)]
    df_excel[duracao] = df_excel[duracao].apply(convert_delta)
    df_excel[duracao] = df_excel[duracao].apply(lambda row: row.total_seconds())
    df_json = pd.read_json(caminho_dados, orient='records',lines=True)
    if not df_json.empty:
        df_jsonF = df_json[df_json[frota].isin(df_excel[frota])] #dados json que não estão mais sem apontamento
        df_new = df_excel[~df_excel[frota].isin(df_json[frota])] #dados novos sem apontamento
        df_jsonFN = df_jsonF.apply(lambda row:atualizar(row,df_excel),axis=1)
        df_new['Envio_1'] = False
        df_new['Envio_2'] = 0
        df = pd.concat([df_jsonFN,df_new],ignore_index=True)
    else:
        df = df_excel
        df['Envio_1'] = False
        df['Envio_2'] = 0
    df = df.apply(lambda row: envio(row,dic_num),axis=1)
    df.to_json(caminho_dados, orient ='records', lines=True) 
       
######## Monitoramento Relatórios PDF Whatsapp

def verificacao_pdfs_atualizados_whatsapp():
    con_pdf = sqlite3.connect(r'C:\CIAANALYTICS\1 - Producao\1 4 - Banco\db_pdf_gov_op.db')
    #con_pdf = sqlite3.connect(r'\\CSCLSFSR01\Agricola$\Logistica Agroindustrial\CIA 22.23\11. Analytics\1 4 - Banco\db_pdf_gov_op.db')
    df = pd.read_sql("SELECT * FROM db_pdf_gov_op WHERE datetime(momento / 1000, 'unixepoch', 'localtime') >= datetime('now', '-123 hours')", con_pdf)
    con_pdf.close()

    df['momento_zap'] = [datetime.fromtimestamp(row) for row in df['momento_zap']]
    df = df[df['momento_zap'] >= datetime(2023,11,14)]
    df['ID'] = [row.split('-')[0] if '-' in row else 'SEM_ID' for row in df['relatorio']]
    def verifica_converte_id(row):
        try:
            return int(str(row).replace('_',''))
        except ValueError:
            return 'SEM_ID'
    df['ID'] = df['ID'].apply(verifica_converte_id)
    df = df[df['ID']!='SEM_ID']
    df = df.sort_values(by='momento_zap', ascending=False)
    df = df.drop_duplicates(subset=['ID'], keep='first')

    df['ultimo_envio_a'] = [datetime.now()-row for row in df['momento_zap']]
    df = df[['ID','momento_zap','ultimo_envio_a']]

    matriz1 = r'\\CSCLSFSR01\Agricola$\Governanca Operacional EAB\03 - Indicadores\PREMISSAS RPA - WHATSAPP\Matriz\Matriz Relatórios - CIA.xlsx'
    matriz2 = r'\\CSCLSFSR01\Agricola$\Governanca Operacional EAB\03 - Indicadores\PREMISSAS RPA - WHATSAPP\Matriz\Matriz Relatórios 2 - CIA.xlsx'
    mat = pd.read_excel(matriz1, sheet_name='Matriz')
    mat1 = pd.read_excel(matriz2, sheet_name='Matriz')
    mat = pd.concat([mat,mat1])
    mat = mat.reset_index(drop=True)
    mat = mat[mat['ENVIAR'].isin(['Sim','sim'])]
    mat = mat[['ID','NOME GENÉRICO RELATÓRIO','NOME RELATÓRIO (ARQUIVO DIRETÓRIO REDE)','PROCESSO','UNIDADE','TÍTULO WHATS','GRUPOS WHATS','FREQUÊNCIA HORA','HORA INICIAL','PERIODICIDADE']]

    mat = pd.merge(left=mat, right=df, left_on='ID', right_on='ID', how='left')
    mat['FREQUÊNCIA HORA'] = [timedelta(hours=row.hour,minutes=row.minute,seconds=row.second) for row in mat['FREQUÊNCIA HORA']]

    if not os.path.exists('registro_pdf_govOp.json'):
        with open(os.path.join(os.getcwd(), "registro_pdf_govOp.json"), "w") as file:
            json.dump(dict(zip(mat.ID, [datetime(1999,12,3).timestamp() for x in mat.ID])), file)
        print(f'Criamos o arquivo "registro_pdf_govOp.json" [{datetime.now()}]')

    def carregar_registro_pma_json():
        with open("registro_pdf_govOp.json", "r") as file:
            loaded_data = json.load(file)
            for k in loaded_data.keys():
                loaded_data[k] = datetime.fromtimestamp(loaded_data[k])
        return loaded_data

    ids_data = carregar_registro_pma_json()

    def salvar_registro_pma_json(dict_alvo):
        dict_temp = dict_alvo
        with open(os.path.join(os.getcwd(), "registro_pdf_govOp.json"), "w") as file:
            for k in dict_temp.keys():
                dict_temp[k] = dict_temp[k].timestamp()
            json.dump(dict_temp, file)

    def gerar_mensagem_pdf_sem_enviar(row_i):
        mensagem = f"""📄 *Relatório Atrasado ID{row_i['ID']}*\n*Relatório*: {row_i['NOME GENÉRICO RELATÓRIO']}\n*Unidade*: {row_i['UNIDADE']}\n*Frequência*: {'Diário' if row_i['FREQUÊNCIA HORA'] == timedelta(hours=23,minutes=59,seconds=59) else f"{str(row_i['FREQUÊNCIA HORA']).split('days ')[-1]}"}\n*Último Envio*: {'Maior que 5 dias' if pd.isna(row_i['momento_zap']) else row_i['momento_zap'].strftime('%d/%m/%Y %H:%M:%S')}"""
        return mensagem

    contato, tipo_contato = verificar_tipo_de_contato('Teste monit PDF ID999')
    for id, row in mat.iterrows():
        # Existe gatilho envio
        if row['ultimo_envio_a'] > (row['FREQUÊNCIA HORA']+timedelta(hours=1)) and datetime.now().time() > row['HORA INICIAL']:
            # É válido Gatilho
            if str(row['ID']) in ids_data.keys() and ids_data[str(row['ID'])] < datetime.now()-row['FREQUÊNCIA HORA']:
                mensagem_a_ser_enviada = gerar_mensagem_pdf_sem_enviar(row)
                gravar_em_banco_para_envio([('MANUT_OS_Ag_Info', datetime.now(), contato, tipo_contato, mensagem_a_ser_enviada, '')])
                ids_data[str(row['ID'])] = datetime.now()
            elif str(row['ID']) not in ids_data.keys():
                mensagem_a_ser_enviada = gerar_mensagem_pdf_sem_enviar(row)
                gravar_em_banco_para_envio([('MANUT_OS_Ag_Info', datetime.now(), contato, tipo_contato, mensagem_a_ser_enviada, '')])
                ids_data[str(row['ID'])] = datetime.now()

    salvar_registro_pma_json(ids_data)
    ids_data = carregar_registro_pma_json()


def gerar_gatilho_troca_de_cana():
    from datetime import datetime, timedelta
    from time import sleep
    import requests
    import json
    import pandas as pd
    import os

    def obter_credenciais_via_navegador():
        from webdriver_manager.chrome import ChromeDriverManager
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.chrome.service import Service
        from datetime import datetime, timedelta
        from seleniumwire import webdriver #wire
        from time import sleep
        from io import BytesIO
        import pandas as pd
        import requests
        import urllib
        import os
        import numpy as np
        

        path_driver = ChromeDriverManager().install()
        path_driver = path_driver.replace('THIRD_PARTY_NOTICES.chromedriver','chromedriver.exe') if 'THIRD_PARTY_NOTICES.chromedriver' in path_driver else path_driver

        link = "https://app.powerbi.com/groups/4c787d14-ccc6-4d8f-adf7-e53e2d5c2357/reports/c8833308-c8ac-4c1d-afa4-2c4b63e09b37/ReportSection2ec5424815c56cb4d125?language=pt-BR&experience=power-bi"

        now = (datetime.now()).strftime("%d/%m/%Y %H:%M:%S")
        print(f'\n[ON] Gravando secret pbi_projecao_CCT.json [{now}]')
        # Sessão Sharepoint
        def abrir_navegador(visivel:bool):
            if not os.path.exists('Sessao PowerBI'):
                os.mkdir('Sessao PowerBI')
            options = Options()
            profile_path = os.path.join(os.getcwd(), "Sessao PowerBI")
            options.add_argument(f"user-data-dir={profile_path}")
            options.add_argument("--window-size=1600,1000")
            if visivel == False: options.add_argument("--headless=True")
            options.add_argument("--start-minimized=True")
            navegador = webdriver.Chrome(options=options, service=Service(path_driver))
            navegador.get(link)
            sleep(7)
            return navegador

        navegador = abrir_navegador(visivel=True)

        navegador.get(link)

        for n in range(100):
            if len([req for req in navegador.requests if 'public/query' in req.url]) > 3:
                break
            sleep(1)

        queys = [req for req in navegador.requests if 'public/query' in req.url]
        headers_exemplo = dict(queys[0].headers)
        body_exemplo = queys[0].body.decode('utf-8')

        pbi_projecao_CCT = {

            'headers':json.dumps(headers_exemplo),

            'body':json.dumps(body_exemplo),

            'status':json.dumps({"version":"1.0.0","queries":[{"Query":{"Commands":[{"SemanticQueryDataShapeCommand":{"Query":{"Version":2,"From":[{"Name":"t","Entity":"Teste_Cenarios","Type":0},{"Name":"u","Entity":"Unidade_Filtro","Type":0},{"Name":"c","Entity":"Calendario_Filtro","Type":0}],"Select":[{"Column":{"Expression":{"SourceRef":{"Source":"t"}},"Property":"Metas_Proprio.Frente"},"Name":"Teste_Cenarios.Metas_Proprio.Frente"},{"Column":{"Expression":{"SourceRef":{"Source":"t"}},"Property":"ULTIMO_GATILHO"},"Name":"Teste_Cenarios.ULTIMO_GATILHO"}],"Where":[{"Condition":{"In":{"Expressions":[{"Column":{"Expression":{"SourceRef":{"Source":"t"}},"Property":"Filtro"}}],"Values":[[{"Literal":{"Value":"'OK'"}}]]}}},{"Condition":{"In":{"Expressions":[{"Column":{"Expression":{"SourceRef":{"Source":"u"}},"Property":"UNIDADE"}}],"Values":[[{"Literal":{"Value":"'SANTA ELISA'"}}]]}}},{"Condition":{"In":{"Expressions":[{"Column":{"Expression":{"SourceRef":{"Source":"c"}},"Property":"Date"}}],"Values":[[{"Literal":{"Value":"datetime'2024-08-23T00:00:00'"}}]]}}},{"Condition":{"Not":{"Expression":{"In":{"Expressions":[{"Column":{"Expression":{"SourceRef":{"Source":"u"}},"Property":"UNIDADE"}}],"Values":[[{"Literal":{"Value":"null"}}],[{"Literal":{"Value":"'TAMOIOS'"}}]]}}}}}],"OrderBy":[{"Direction":2,"Expression":{"Column":{"Expression":{"SourceRef":{"Source":"t"}},"Property":"Metas_Proprio.Frente"}}}]},"Binding":{"Primary":{"Groupings":[{"Projections":[0,1],"Subtotal":1}]},"DataReduction":{"DataVolume":3,"Primary":{"Window":{"Count":500}}},"Version":1}}}]},"QueryId":"e76eabc2-7bdd-6bcb-16db-afe6513bc5e3"}],"cancelQueries":[],"modelId":6471392,"userPreferredLocale":"pt-BR","allowLongRunningQueries":True}),

            'Autonomia':json.dumps({'version': '1.0.0',
                'queries': [{'Query': {'Commands': [{'SemanticQueryDataShapeCommand': {'Query': {'Version': 2,
                        'From': [{'Name': 'p', 'Entity': 'CALCULO FRENTE DIA', 'Type': 0},
                        {'Name': 'u', 'Entity': 'Unidade_Filtro', 'Type': 0},
                        {'Name': 'c1', 'Entity': 'Calendario_Filtro', 'Type': 0}],
                        'Select': [{'Aggregation': {'Expression': {'Column': {'Expression': {'SourceRef': {'Source': 'p'}},
                            'Property': 'META AJUSTE/PPC (h)'}},
                        'Function': 0},
                        'Name': 'Sum(PROJEÇÃO FRENTE.META AJUSTE/PPC (h))'},
                        {'Aggregation': {'Expression': {'Column': {'Expression': {'SourceRef': {'Source': 'p'}},
                            'Property': 'SOLICITAÇÃO GO (h)'}},
                        'Function': 0},
                        'Name': 'Sum(PROJEÇÃO FRENTE.SOLICITAÇÃO GO (h))'},
                        {'Aggregation': {'Expression': {'Column': {'Expression': {'SourceRef': {'Source': 'p'}},
                            'Property': 'REALIZADO (média ult. 3h)'}},
                        'Function': 0},
                        'Name': 'Sum(PROJEÇÃO FRENTE.REALIZADO (média ult. 3h))'},
                        {'Aggregation': {'Expression': {'Column': {'Expression': {'SourceRef': {'Source': 'p'}},
                            'Property': 'Colheitabilidade'}},
                        'Function': 0},
                        'Name': 'Sum(PROJEÇÃO FRENTE.Colheitabilidade)'},
                        {'Aggregation': {'Expression': {'Column': {'Expression': {'SourceRef': {'Source': 'p'}},
                            'Property': 'Taxa_Comunicacao'}},
                        'Function': 0},
                        'Name': 'Sum(PROJEÇÃO FRENTE.Taxa_Comunicacao)'},
                        {'Column': {'Expression': {'SourceRef': {'Source': 'p'}},
                        'Property': 'CD_FREN_TRAN'},
                        'Name': 'PROJEÇÃO FRENTE.CD_FREN_TRAN'}],
                        'Where': [{'Condition': {'In': {'Expressions': [{'Column': {'Expression': {'SourceRef': {'Source': 'p'}},
                            'Property': 'Tipo Transporte'}}],
                            'Values': [[{'Literal': {'Value': "'Próprio'"}}]]}}},
                        {'Condition': {'In': {'Expressions': [{'Column': {'Expression': {'SourceRef': {'Source': 'u'}},
                            'Property': 'UNIDADE'}}],
                            'Values': [[{'Literal': {'Value': "'SANTA ELISA'"}}]]}}},
                        {'Condition': {'In': {'Expressions': [{'Column': {'Expression': {'SourceRef': {'Source': 'c1'}},
                            'Property': 'Date'}}],
                            'Values': [[{'Literal': {'Value': "datetime'2024-08-22T00:00:00'"}}]]}}},
                        {'Condition': {'Not': {'Expression': {'In': {'Expressions': [{'Column': {'Expression': {'SourceRef': {'Source': 'u'}},
                                'Property': 'UNIDADE'}}],
                            'Values': [[{'Literal': {'Value': 'null'}}],
                            [{'Literal': {'Value': "'TAMOIOS'"}}]]}}}}}],
                        'OrderBy': [{'Direction': 1,
                        'Expression': {'Column': {'Expression': {'SourceRef': {'Source': 'p'}},
                            'Property': 'CD_FREN_TRAN'}}}]},
                    'Binding': {'Primary': {'Groupings': [{'Projections': [0,
                            1,
                            2,
                            3,
                            4,
                            5]}]},
                        'DataReduction': {'DataVolume': 4,
                        'Primary': {'Window': {'Count': 1000}}},
                        'Version': 1},
                    'ExecutionMetricsKind': 1}}]},
                'QueryId': 'd2f7f8ca-7e73-4d6e-d0f6-a3334899c705',
                'ApplicationContext': {'DatasetId': 'b0c73c9b-d68e-4e8e-8ceb-7e89488f686a',
                    'Sources': [{'ReportId': 'c8833308-c8ac-4c1d-afa4-2c4b63e09b37',
                    'VisualId': '6dfdad5e6c5bbd131f59',
                    'HostProperties': {'ConsumptionMethod': 'Power BI Web App',
                    'UserSession': '194991ec-1d0a-44cd-9e2c-d1deae2f20d0'}}]}},
                {'Query': {'Commands': [{'SemanticQueryDataShapeCommand': {'Query': {'Version': 2,
                        'From': [{'Name': 'p1', 'Entity': 'PREVISIBILIDADE', 'Type': 0},
                        {'Name': 'u', 'Entity': 'Unidade_Filtro', 'Type': 0}],
                        'Select': [{'Aggregation': {'Expression': {'Column': {'Expression': {'SourceRef': {'Source': 'p1'}},
                            'Property': 'Patio_Horario'}},
                        'Function': 0},
                        'Name': 'Sum(PREVISIBILIDADE.Patio_Horario)'},
                        {'Aggregation': {'Expression': {'Column': {'Expression': {'SourceRef': {'Source': 'p1'}},
                            'Property': 'Cargas_Atuais'}},
                        'Function': 0},
                        'Name': 'Sum(PREVISIBILIDADE.Cargas_Atuais)'},
                        {'Column': {'Expression': {'SourceRef': {'Source': 'p1'}},
                        'Property': 'Contador_Num'},
                        'Name': 'Sum(PREVISIBILIDADE.Contador_Num)'},
                        {'Aggregation': {'Expression': {'Column': {'Expression': {'SourceRef': {'Source': 'p1'}},
                            'Property': 'Entrega média ult. 3h FF - Inteira (ton)'}},
                        'Function': 0},
                        'Name': 'Sum(PREVISIBILIDADE.Entrega média ult. 3h FF - Inteira (ton))'},
                        {'Aggregation': {'Expression': {'Column': {'Expression': {'SourceRef': {'Source': 'p1'}},
                            'Property': 'Entrega média ult. 3h FF - Picada (ton)'}},
                        'Function': 0},
                        'Name': 'Sum(PREVISIBILIDADE.Entrega média ult. 3h FF - Picada (ton))'},
                        {'Aggregation': {'Expression': {'Column': {'Expression': {'SourceRef': {'Source': 'p1'}},
                            'Property': 'Entrega média ult. 3h PP - Inteira (ton)'}},
                        'Function': 0},
                        'Name': 'Sum(PREVISIBILIDADE.Entrega média ult. 3h PP - Inteira (ton))'},
                        {'Aggregation': {'Expression': {'Column': {'Expression': {'SourceRef': {'Source': 'p1'}},
                            'Property': 'Entrega média ult. 3h PP - Picada (ton)'}},
                        'Function': 0},
                        'Name': 'Sum(PREVISIBILIDADE.Entrega média ult. 3h PP - Picada (ton))'},
                        {'Aggregation': {'Expression': {'Column': {'Expression': {'SourceRef': {'Source': 'p1'}},
                            'Property': 'Total Entrega (Média ult. 3h)'}},
                        'Function': 0},
                        'Name': 'Sum(PREVISIBILIDADE.Total Entrega (Média ult. 3h))'},
                        {'Column': {'Expression': {'SourceRef': {'Source': 'p1'}},
                        'Property': 'Saldo_Estoque'},
                        'Name': 'PREVISIBILIDADE.Saldo_Estoque'},
                        {'Column': {'Expression': {'SourceRef': {'Source': 'p1'}},
                        'Property': 'Tempo Moagem Nominal'},
                        'Name': 'PREVISIBILIDADE.Tempo Moagem Nominal'},
                        {'Column': {'Expression': {'SourceRef': {'Source': 'p1'}},
                        'Property': 'Tempo Moagem Real'},
                        'Name': 'PREVISIBILIDADE.Tempo Moagem Real'},
                        {'Aggregation': {'Expression': {'Column': {'Expression': {'SourceRef': {'Source': 'p1'}},
                            'Property': 'Troca de Cana'}},
                        'Function': 0},
                        'Name': 'Sum(PREVISIBILIDADE.Troca de Cana)'},
                        {'Aggregation': {'Expression': {'Column': {'Expression': {'SourceRef': {'Source': 'p1'}},
                            'Property': 'Hora Prev'}},
                        'Function': 0},
                        'Name': 'Sum(PREVISIBILIDADE.Hora Prev)'},
                        {'Aggregation': {'Expression': {'Column': {'Expression': {'SourceRef': {'Source': 'p1'}},
                            'Property': 'Tempo_Moagem_Real'}},
                        'Function': 0},
                        'Name': 'Sum(PREVISIBILIDADE.Tempo_Moagem_Real)'},
                        {'Aggregation': {'Expression': {'Column': {'Expression': {'SourceRef': {'Source': 'p1'}},
                            'Property': 'Tempo_Moagem_Nominal'}},
                        'Function': 0},
                        'Name': 'Sum(PREVISIBILIDADE.Tempo_Moagem_Nominal)'}],
                        'Where': [{'Condition': {'In': {'Expressions': [{'Column': {'Expression': {'SourceRef': {'Source': 'p1'}},
                            'Property': 'Filtro'}}],
                            'Values': [[{'Literal': {'Value': "'Ok'"}}]]}}},
                        {'Condition': {'In': {'Expressions': [{'Column': {'Expression': {'SourceRef': {'Source': 'u'}},
                            'Property': 'UNIDADE'}}],
                            'Values': [[{'Literal': {'Value': "'SANTA ELISA'"}}]]}}},
                        {'Condition': {'Not': {'Expression': {'In': {'Expressions': [{'Column': {'Expression': {'SourceRef': {'Source': 'u'}},
                                'Property': 'UNIDADE'}}],
                            'Values': [[{'Literal': {'Value': 'null'}}],
                            [{'Literal': {'Value': "'TAMOIOS'"}}]]}}}}}],
                        'OrderBy': [{'Direction': 2,
                        'Expression': {'Aggregation': {'Expression': {'Column': {'Expression': {'SourceRef': {'Source': 'p1'}},
                            'Property': 'Entrega média ult. 3h PP - Picada (ton)'}},
                            'Function': 0}}}]},
                    'Binding': {'Primary': {'Groupings': [{'Projections': [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14]}]},
                        'DataReduction': {'DataVolume': 3,
                        'Primary': {'Window': {'Count': 500}}},
                        'SuppressedJoinPredicates': [13, 14],
                        'Version': 1},
                    'ExecutionMetricsKind': 1}}]},
                'QueryId': 'c3b5fd15-dd2c-be95-62f9-4c07262d30ac',
                'ApplicationContext': {'DatasetId': 'b0c73c9b-d68e-4e8e-8ceb-7e89488f686a',
                    'Sources': [{'ReportId': 'c8833308-c8ac-4c1d-afa4-2c4b63e09b37',
                    'VisualId': 'e723fa539fb8c357121a',
                    'HostProperties': {'ConsumptionMethod': 'Power BI Web App',
                    'UserSession': '194991ec-1d0a-44cd-9e2c-d1deae2f20d0'}}]}},
                {'Query': {'Commands': [{'SemanticQueryDataShapeCommand': {'Query': {'Version': 2,
                        'From': [{'Name': 'p1', 'Entity': 'PREVISIBILIDADE', 'Type': 0},
                        {'Name': 'u', 'Entity': 'Unidade_Filtro', 'Type': 0},
                        {'Name': 'c', 'Entity': 'Calendario_Filtro', 'Type': 0}],
                        'Select': [{'Aggregation': {'Expression': {'Column': {'Expression': {'SourceRef': {'Source': 'p1'}},
                            'Property': 'Qtde Checkin 3hrs'}},
                        'Function': 1},
                        'Name': 'Sum(PREVISIBILIDADE.Qtde Checkin 3hrs)'}],
                        'Where': [{'Condition': {'In': {'Expressions': [{'Column': {'Expression': {'SourceRef': {'Source': 'p1'}},
                            'Property': 'Filtro'}}],
                            'Values': [[{'Literal': {'Value': "'Ok'"}}]]}}},
                        {'Condition': {'In': {'Expressions': [{'Column': {'Expression': {'SourceRef': {'Source': 'u'}},
                            'Property': 'UNIDADE'}}],
                            'Values': [[{'Literal': {'Value': "'SANTA ELISA'"}}]]}}},
                        {'Condition': {'In': {'Expressions': [{'Column': {'Expression': {'SourceRef': {'Source': 'c'}},
                            'Property': 'Date'}}],
                            'Values': [[{'Literal': {'Value': "datetime'2024-08-22T00:00:00'"}}]]}}},
                        {'Condition': {'Not': {'Expression': {'In': {'Expressions': [{'Column': {'Expression': {'SourceRef': {'Source': 'u'}},
                                'Property': 'UNIDADE'}}],
                            'Values': [[{'Literal': {'Value': 'null'}}],
                            [{'Literal': {'Value': "'TAMOIOS'"}}]]}}}}}]},
                    'Binding': {'Primary': {'Groupings': [{'Projections': [0]}]},
                        'DataReduction': {'DataVolume': 3, 'Primary': {'Top': {}}},
                        'Version': 1},
                    'ExecutionMetricsKind': 1}}]},
                'QueryId': '534cbee9-504f-67b4-6be4-a6c14cf0a81e',
                'ApplicationContext': {'DatasetId': 'b0c73c9b-d68e-4e8e-8ceb-7e89488f686a',
                    'Sources': [{'ReportId': 'c8833308-c8ac-4c1d-afa4-2c4b63e09b37',
                    'VisualId': '1f9793d6f8459dfddd28',
                    'HostProperties': {'ConsumptionMethod': 'Power BI Web App',
                    'UserSession': '194991ec-1d0a-44cd-9e2c-d1deae2f20d0'}}]}}],
                'cancelQueries': [],
                'modelId': 6471392,
                'userPreferredLocale': 'pt-BR',
                'allowLongRunningQueries': True}),

            "moagem":json.dumps({"version":"1.0.0","queries":[{"Query":{"Commands":[{"SemanticQueryDataShapeCommand":{"Query":{"Version":2,"From":[{"Name":"p","Entity":"CALCULO UNIDADE DIA","Type":0},{"Name":"u","Entity":"Unidade_Filtro","Type":0},{"Name":"c","Entity":"Calendario_Filtro","Type":0}],"Select":[{"Aggregation":{"Expression":{"Column":{"Expression":{"SourceRef":{"Source":"p"}},"Property":"Realizado_Ton"}},"Function":0},"Name":"Sum(PROJEÇÃO UNIDADE.Realizado_Ton)"},{"Aggregation":{"Expression":{"Column":{"Expression":{"SourceRef":{"Source":"p"}},"Property":"Meta Moagem TCD"}},"Function":1},"Name":"Sum(PROJEÇÃO UNIDADE.Meta Moagem TCD)"},{"Aggregation":{"Expression":{"Column":{"Expression":{"SourceRef":{"Source":"p"}},"Property":"Entrega_Prevista_Hr_Ton"}},"Function":1},"Name":"Sum(PROJEÇÃO UNIDADE.Entrega_Prevista_Hr_Ton)"},{"Measure":{"Expression":{"SourceRef":{"Source":"p"}},"Property":"Format_moagem"},"Name":"CALCULO UNIDADE DIA.Format_moagem"}],"Where":[{"Condition":{"In":{"Expressions":[{"Column":{"Expression":{"SourceRef":{"Source":"u"}},"Property":"UNIDADE"}}],"Values":[[{"Literal":{"Value":"'SANTA ELISA'"}}]]}}},{"Condition":{"In":{"Expressions":[{"Column":{"Expression":{"SourceRef":{"Source":"c"}},"Property":"Date"}}],"Values":[[{"Literal":{"Value":"datetime'2024-08-22T00:00:00'"}}]]}}},{"Condition":{"Not":{"Expression":{"In":{"Expressions":[{"Column":{"Expression":{"SourceRef":{"Source":"u"}},"Property":"UNIDADE"}}],"Values":[[{"Literal":{"Value":"null"}}],[{"Literal":{"Value":"'TAMOIOS'"}}]]}}}}}]},"Binding":{"Primary":{"Groupings":[{"Projections":[0,1,2]}]},"Projections":[3],"Version":1},"ExecutionMetricsKind":1}}]},"QueryId":"0551eef1-a2b8-9396-f5b2-e099f89af6b0","ApplicationContext":{"DatasetId":"b0c73c9b-d68e-4e8e-8ceb-7e89488f686a","Sources":[{"ReportId":"c8833308-c8ac-4c1d-afa4-2c4b63e09b37","VisualId":"542aa7125ebc096ade21","HostProperties":{"ConsumptionMethod":"Power BI Web App","UserSession":"17f62eb1-de04-4afa-a335-915ed71ada41"}}]}},{"Query":{"Commands":[{"SemanticQueryDataShapeCommand":{"Query":{"Version":2,"From":[{"Name":"t","Entity":"TC","Type":0},{"Name":"h","Entity":"Horas_Filtro","Type":0},{"Name":"p","Entity":"CALCULO UNIDADE DIA","Type":0},{"Name":"u","Entity":"Unidade_Filtro","Type":0},{"Name":"c","Entity":"Calendario_Filtro","Type":0}],"Select":[{"Column":{"Expression":{"SourceRef":{"Source":"t"}},"Property":"tipo_final_tc"},"Name":"TC.tipo_final_tc"},{"Column":{"Expression":{"SourceRef":{"Source":"h"}},"Property":"Hora"},"Name":"Horas_Filtro.Hora"},{"Aggregation":{"Expression":{"Column":{"Expression":{"SourceRef":{"Source":"t"}},"Property":"Realizado"}},"Function":0},"Name":"Sum(TC.Realizado)"},{"Aggregation":{"Expression":{"Column":{"Expression":{"SourceRef":{"Source":"p"}},"Property":"Meta Moagem Hora"}},"Function":1},"Name":"Sum(PROJEÇÃO UNIDADE.Meta Moagem Hora)"}],"Where":[{"Condition":{"In":{"Expressions":[{"Column":{"Expression":{"SourceRef":{"Source":"u"}},"Property":"UNIDADE"}}],"Values":[[{"Literal":{"Value":"'SANTA ELISA'"}}]]}}},{"Condition":{"In":{"Expressions":[{"Column":{"Expression":{"SourceRef":{"Source":"c"}},"Property":"Date"}}],"Values":[[{"Literal":{"Value":"datetime'2024-08-22T00:00:00'"}}]]}}},{"Condition":{"Not":{"Expression":{"In":{"Expressions":[{"Column":{"Expression":{"SourceRef":{"Source":"u"}},"Property":"UNIDADE"}}],"Values":[[{"Literal":{"Value":"null"}}],[{"Literal":{"Value":"'TAMOIOS'"}}]]}}}}}],"OrderBy":[{"Direction":1,"Expression":{"Column":{"Expression":{"SourceRef":{"Source":"h"}},"Property":"Hora"}}}]},"Binding":{"Primary":{"Groupings":[{"Projections":[1,3,2],"ShowItemsWithNoData":[1]}]},"Secondary":{"Groupings":[{"Projections":[0],"SuppressedProjections":[3]}]},"DataReduction":{"DataVolume":4,"Primary":{"Window":{"Count":200}},"Secondary":{"Top":{"Count":60}}},"Version":1},"ExecutionMetricsKind":1}}]},"QueryId":"3d105842-a310-ffbb-dcec-4c748e6eaec6","ApplicationContext":{"DatasetId":"b0c73c9b-d68e-4e8e-8ceb-7e89488f686a","Sources":[{"ReportId":"c8833308-c8ac-4c1d-afa4-2c4b63e09b37","VisualId":"20df792cba2241888b15","HostProperties":{"ConsumptionMethod":"Power BI Web App","UserSession":"17f62eb1-de04-4afa-a335-915ed71ada41"}}]}}],"cancelQueries":[],"modelId":6471392,"userPreferredLocale":"pt-BR","allowLongRunningQueries":True}),

            "cargas": json.dumps({'version': '1.0.0',
                'queries': [{'Query': {'Commands': [{'SemanticQueryDataShapeCommand': {'Query': {'Version': 2,
                    'From': [{'Name': 'c',
                        'Entity': 'CALCULO UNIDADE PROPRIEDADE',
                        'Type': 0},
                        {'Name': 'u', 'Entity': 'Unidade_Filtro', 'Type': 0},
                        {'Name': 'c1', 'Entity': 'Calendario_Filtro', 'Type': 0}],
                    'Select': [{'Aggregation': {'Expression': {'Column': {'Expression': {'SourceRef': {'Source': 'c'}},
                            'Property': '% Fibra'}},
                        'Function': 0},
                        'Name': 'Sum(CALCULO UNIDADE PROPRIEDADE.% Fibra)'},
                        {'Aggregation': {'Expression': {'Column': {'Expression': {'SourceRef': {'Source': 'c'}},
                            'Property': 'Meta % Fibra'}},
                        'Function': 1},
                        'Name': 'Sum(CALCULO UNIDADE PROPRIEDADE.Meta % Fibra)'},
                        {'Aggregation': {'Expression': {'Column': {'Expression': {'SourceRef': {'Source': 'c'}},
                            'Property': 'Format Fibra'}},
                        'Function': 0},
                        'Name': 'Sum(CALCULO UNIDADE PROPRIEDADE.Format Fibra)'}],
                    'Where': [{'Condition': {'In': {'Expressions': [{'Column': {'Expression': {'SourceRef': {'Source': 'c'}},
                            'Property': 'TIPO_FF_PP_UF'}}],
                        'Values': [[{'Literal': {'Value': "'PP'"}}]]}}},
                        {'Condition': {'In': {'Expressions': [{'Column': {'Expression': {'SourceRef': {'Source': 'u'}},
                            'Property': 'UNIDADE'}}],
                        'Values': [[{'Literal': {'Value': "'SANTA ELISA'"}}]]}}},
                        {'Condition': {'In': {'Expressions': [{'Column': {'Expression': {'SourceRef': {'Source': 'c1'}},
                            'Property': 'Date'}}],
                        'Values': [[{'Literal': {'Value': "datetime'2024-08-22T00:00:00'"}}]]}}},
                        {'Condition': {'Not': {'Expression': {'In': {'Expressions': [{'Column': {'Expression': {'SourceRef': {'Source': 'u'}},
                                'Property': 'UNIDADE'}}],
                            'Values': [[{'Literal': {'Value': 'null'}}],
                            [{'Literal': {'Value': "'TAMOIOS'"}}]]}}}}}]},
                    'Binding': {'Primary': {'Groupings': [{'Projections': [0, 1]}]},
                    'Projections': [2],
                    'Version': 1},
                    'ExecutionMetricsKind': 1}}]},
                'QueryId': 'fe3e0596-0aef-de01-ce65-008ffa9825e3',
                'ApplicationContext': {'DatasetId': 'b0c73c9b-d68e-4e8e-8ceb-7e89488f686a',
                'Sources': [{'ReportId': 'c8833308-c8ac-4c1d-afa4-2c4b63e09b37',
                    'VisualId': '67bc8ac2c9010703064a',
                    'HostProperties': {'ConsumptionMethod': 'Power BI Web App',
                    'UserSession': '194991ec-1d0a-44cd-9e2c-d1deae2f20d0'}}]}},
                {'Query': {'Commands': [{'SemanticQueryDataShapeCommand': {'Query': {'Version': 2,
                    'From': [{'Name': 't', 'Entity': 'TC', 'Type': 0},
                        {'Name': 'u', 'Entity': 'Unidade_Filtro', 'Type': 0},
                        {'Name': 'c', 'Entity': 'Calendario_Filtro', 'Type': 0}],
                    'Select': [{'Measure': {'Expression': {'SourceRef': {'Source': 't'}},
                        'Property': '% Fibra'},
                        'Name': 'TC.% Fibra'},
                        {'Column': {'Expression': {'SourceRef': {'Source': 't'}},
                        'Property': 'Cluster Estagio'},
                        'Name': 'TC.Cluster Estagio'},
                        {'Aggregation': {'Expression': {'Column': {'Expression': {'SourceRef': {'Source': 't'}},
                            'Property': 'Meta % Fibra'}},
                        'Function': 1},
                        'Name': 'Sum(TC.Meta % Fibra)'},
                        {'Measure': {'Expression': {'SourceRef': {'Source': 't'}},
                        'Property': 'Format Fibra'},
                        'Name': 'TC.Format Fibra'}],
                    'Where': [{'Condition': {'In': {'Expressions': [{'Column': {'Expression': {'SourceRef': {'Source': 't'}},
                            'Property': 'TIPO_FF_PP_UF'}}],
                        'Values': [[{'Literal': {'Value': "'PP'"}}]]}}},
                        {'Condition': {'In': {'Expressions': [{'Column': {'Expression': {'SourceRef': {'Source': 'u'}},
                            'Property': 'UNIDADE'}}],
                        'Values': [[{'Literal': {'Value': "'SANTA ELISA'"}}]]}}},
                        {'Condition': {'In': {'Expressions': [{'Column': {'Expression': {'SourceRef': {'Source': 'c'}},
                            'Property': 'Date'}}],
                        'Values': [[{'Literal': {'Value': "datetime'2024-08-22T00:00:00'"}}]]}}},
                        {'Condition': {'Not': {'Expression': {'In': {'Expressions': [{'Column': {'Expression': {'SourceRef': {'Source': 'u'}},
                                'Property': 'UNIDADE'}}],
                            'Values': [[{'Literal': {'Value': 'null'}}],
                            [{'Literal': {'Value': "'TAMOIOS'"}}]]}}}}}],
                    'OrderBy': [{'Direction': 1,
                        'Expression': {'Column': {'Expression': {'SourceRef': {'Source': 't'}},
                        'Property': 'Cluster Estagio'}}}]},
                    'Binding': {'Primary': {'Groupings': [{'Projections': [1, 2, 0, 3],
                        'ShowItemsWithNoData': [1]}]},
                    'DataReduction': {'DataVolume': 4,
                        'Primary': {'Window': {'Count': 1000}}},
                    'SuppressedJoinPredicates': [3],
                    'Version': 1},
                    'ExecutionMetricsKind': 1}}]},
                'QueryId': '6d035b16-c0a0-02d9-0f3d-9579f1ee9680',
                'ApplicationContext': {'DatasetId': 'b0c73c9b-d68e-4e8e-8ceb-7e89488f686a',
                'Sources': [{'ReportId': 'c8833308-c8ac-4c1d-afa4-2c4b63e09b37',
                    'VisualId': '9d2aecd1b0149bde445c',
                    'HostProperties': {'ConsumptionMethod': 'Power BI Web App',
                    'UserSession': '194991ec-1d0a-44cd-9e2c-d1deae2f20d0'}}]}},
                {'Query': {'Commands': [{'SemanticQueryDataShapeCommand': {'Query': {'Version': 2,
                    'From': [{'Name': 'c', 'Entity': 'CALCULO UNIDADE HORA', 'Type': 0},
                        {'Name': 'u', 'Entity': 'Unidade_Filtro', 'Type': 0},
                        {'Name': 'c1', 'Entity': 'Calendario_Filtro', 'Type': 0}],
                    'Select': [{'Aggregation': {'Expression': {'Column': {'Expression': {'SourceRef': {'Source': 'c'}},
                            'Property': 'Cargas FF'}},
                        'Function': 0},
                        'Name': 'Sum(CALCULO UNIDADE HORA.Cargas FF)'},
                        {'Aggregation': {'Expression': {'Column': {'Expression': {'SourceRef': {'Source': 'c'}},
                            'Property': 'Cargas PP'}},
                        'Function': 0},
                        'Name': 'Sum(CALCULO UNIDADE HORA.Cargas PP)'},
                        {'Column': {'Expression': {'SourceRef': {'Source': 'c'}},
                        'Property': 'Hora'},
                        'Name': 'CALCULO UNIDADE HORA.Hora',
                        'NativeReferenceName': 'Hora'}],
                    'Where': [{'Condition': {'In': {'Expressions': [{'Column': {'Expression': {'SourceRef': {'Source': 'u'}},
                            'Property': 'UNIDADE'}}],
                        'Values': [[{'Literal': {'Value': "'SANTA ELISA'"}}]]}}},
                        {'Condition': {'In': {'Expressions': [{'Column': {'Expression': {'SourceRef': {'Source': 'c1'}},
                            'Property': 'Date'}}],
                        'Values': [[{'Literal': {'Value': "datetime'2024-08-22T00:00:00'"}}]]}}},
                        {'Condition': {'Not': {'Expression': {'In': {'Expressions': [{'Column': {'Expression': {'SourceRef': {'Source': 'u'}},
                                'Property': 'UNIDADE'}}],
                            'Values': [[{'Literal': {'Value': 'null'}}],
                            [{'Literal': {'Value': "'TAMOIOS'"}}]]}}}}}],
                    'OrderBy': [{'Direction': 1,
                        'Expression': {'Column': {'Expression': {'SourceRef': {'Source': 'c'}},
                        'Property': 'Hora'}}}]},
                    'Binding': {'Primary': {'Groupings': [{'Projections': [2, 0, 1],
                        'ShowItemsWithNoData': [2]}]},
                    'DataReduction': {'DataVolume': 4,
                        'Primary': {'Window': {'Count': 1000}}},
                    'Version': 1},
                    'ExecutionMetricsKind': 1}}]},
                'QueryId': '1f567774-19c3-848f-f0a8-9d0948d589b5',
                'ApplicationContext': {'DatasetId': 'b0c73c9b-d68e-4e8e-8ceb-7e89488f686a',
                'Sources': [{'ReportId': 'c8833308-c8ac-4c1d-afa4-2c4b63e09b37',
                    'VisualId': '434231e978370a801155',
                    'HostProperties': {'ConsumptionMethod': 'Power BI Web App',
                    'UserSession': '194991ec-1d0a-44cd-9e2c-d1deae2f20d0'}}]}}],
                'cancelQueries': [],
                'modelId': 6471392,
                'userPreferredLocale': 'pt-BR',
                'allowLongRunningQueries': True}),
                }

        with open('pbi_projecao_CCT.json', 'w') as json_file:
            json_file.write(json.dumps(pbi_projecao_CCT))

        navegador.quit()

        now = (datetime.now()).strftime("%d/%m/%Y %H:%M:%S")
        print(f'[OK] Gravado secret pbi_projecao_CCT.json [{now}]')

    caminho_sharepointGovOp = verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\Metas_Gov_Indicadores - Documentos')

    metas_unidades = pd.read_excel(os.path.join(caminho_sharepointGovOp,'CCT/Parâmetros Relatórios CCT (Metas).xlsx'),sheet_name="Metas")[["UNIDADE","Meta Moagem TCD","Cargas p/ 227"]]
    de_para_und = {'BENA':"BENALCOOL",
        'BONF':"BONFIM",
        'CAAR':"CAARAPO",
        'COPI':"COSTA PINTO",
        'DEST':"DESTIVALE",
        'DIA':"DIAMANTE",
        'IPA':"IPAUSSU",
        'JUN':"JUNQUEIRA",
        'MUND':"MUNDIAL",
        'PARAI':"PARAISO",
        'RAF':"RAFARD",
        'SCAND':"STA. CANDIDA",
        'UNI':"UNIVALEM",
        'VRO':"VALE DO ROSARIO",
        'SEL':"SANTA ELISA",
        'LEM':"LEME",
        'RBR':"RIO BRILHANTE",
        'PTP':"PASSATEMPO",
        'LPT':"LAGOA DA PRATA",
        'CNT':"CONTINETAL"}
    metas_unidades["UNIDADE"] = metas_unidades["UNIDADE"].apply(lambda x: de_para_und[x] if x in de_para_und else x)
    meta_unidade = {und:{'meta_moagem':m_moagem,'carga_minima':m_carga} for und,m_moagem,m_carga in zip(metas_unidades["UNIDADE"],metas_unidades["Meta Moagem TCD"],metas_unidades["Cargas p/ 227"])}

    if not os.path.exists('pbi_projecao_CCT.json'):
        obter_credenciais_via_navegador()
    with open('pbi_projecao_CCT.json', 'r') as json_file:
        data = json_file.read()
        pbi_acesso = json.loads(data)
        
    url_geral = 'https://b7b27b8b2f574e3e84ee14f0666961a3.pbidedicated.windows.net/webapi/capacities/B7B27B8B-2F57-4E3E-84EE-14F0666961A3/workloads/QES/QueryExecutionService/automatic/public/query'
    dia_atual = datetime.now().isoformat().split('T')[0]+"T00:00:00"
    unidades = ['SANTA ELISA', "VALE DO ROSARIO"]
    dados_projecao = {}

    headers = json.loads(pbi_acesso["headers"]) #.encode('utf-8')
    body = json.loads(pbi_acesso["body"]).encode('utf-8')
    status_code = requests.post(url_geral, headers=headers, timeout=30, data=body).status_code

    if status_code != 200:
        obter_credenciais_via_navegador()
        with open('pbi_projecao_CCT.json', 'r') as json_file:
            data = json_file.read()
            pbi_acesso = json.loads(data)
        headers = json.loads(pbi_acesso["headers"])

    def find_key(d, target_key):
        if isinstance(d, dict):
            for key, value in d.items():
                if key == target_key:
                    return value
                elif isinstance(value, dict):
                    result = find_key(value, target_key)
                    if result is not None:
                        return result
                elif isinstance(value, list):
                    for item in value:
                        result = find_key(item, target_key)
                        if result is not None:
                            return result
        elif isinstance(d, list):
            for item in d:
                result = find_key(item, target_key)
                if result is not None:
                    return result
        return None

    def converter_str_datetime(real,nominal):
        novo_real = []
        novo_nom = []
        for real,nom in zip(real,nominal):
            try: real_ = pd.to_timedelta(real)
            except: real_ = "Falta de Cana"
            novo_real.append(real_)
            try: nom_ = pd.to_timedelta(nom)
            except: nom_ = "Falta de Cana"
            novo_nom.append(nom_)
        return novo_real,novo_nom

    for unidade in unidades:
        print(f"Iniciando ",unidade)
        dados_projecao[unidade] = {'autonomia': '', 'moagem': '', 'cargas': ''}
        # Autonomia
        payload_auto = json.loads(pbi_acesso["Autonomia"].replace("SANTA ELISA",unidade).replace("2024-08-22T00:00:00",dia_atual))
        resposta_auto = requests.post(url_geral, headers=headers, json=payload_auto)
        autonomia = json.loads(resposta_auto.content.decode('utf-8'))
        dados_autonomia = autonomia["results"][1]
        moagem_nominal = find_key(autonomia["results"], "D1")
        moagem_real = find_key(autonomia["results"], "D2")
        moagem_real,moagem_nominal = converter_str_datetime(moagem_real,moagem_nominal)
        dados_projecao[unidade]['autonomia'] = {'real':moagem_real,'nominal':moagem_nominal}
        # Moagem
        payload_moa = json.loads(pbi_acesso["moagem"].replace("SANTA ELISA",unidade).replace("2024-08-22T00:00:00",dia_atual))
        resposta_moa = requests.post(url_geral, headers=headers, json=payload_moa)
        moagem = json.loads(resposta_moa.content.decode('utf-8'))["results"]
        dados_moagem = find_key(moagem[1], "DS")[0]["PH"][0]
        moagem_hist = {}
        for n in range(24):
            try:
                foca = dados_moagem["DM0"][n]["X"][0]["M1"]
                proprio = dados_moagem["DM0"][n]["X"][1]["M1"]
                total = foca + proprio
                moagem_hist[n] = {'total':total,'proprio':proprio,'foca':foca}
            except: pass
        dados_projecao[unidade]['moagem'] = moagem_hist
        # Cargas
        payload_car = json.loads(pbi_acesso["cargas"].replace("SANTA ELISA",unidade).replace("2024-08-22T00:00:00",dia_atual))
        resposta_car = requests.post(url_geral, headers=headers, json=payload_car)
        cargas_json = json.loads(resposta_car.content.decode('utf-8'))["results"]
        cargas = find_key(cargas_json[1],"DS")
        cargas_hist = {}
        for n in range(24):
            try:
                hora,ff,pp = cargas[0]["PH"][0]["DM0"][n]["C"]
                cargas_hist[hora] = {'total':ff+pp,'ff':ff,'pp':pp}
            except: 
                pass
        dados_projecao[unidade]['cargas'] = cargas_hist
        dados_projecao[unidade]['meta_moagem'] = meta_unidade[unidade]["meta_moagem"]
        dados_projecao[unidade]['carga_minima'] = meta_unidade[unidade]["carga_minima"]
        
        payload_status = json.loads(pbi_acesso["status"].replace("SANTA ELISA",unidade).replace("2024-08-22T00:00:00",dia_atual))
        resposta_status = requests.post(url_geral, headers=headers, json=payload_status)
        status = json.loads(resposta_status.content.decode('utf-8'))["results"]
        status_atual = find_key(status,'D1')
        satatus_resumo = 'Limitação' if len([x for x in status_atual if '227' in x]) > 0 else 'Não Limitação'
        
        dados_projecao[unidade]['status'] = satatus_resumo
        print('OK ',unidade)

    linhas = []
    for unidade in dados_projecao.keys():
        # Projeção Nominal
        projecao_nominal = dados_projecao[unidade]["autonomia"]["nominal"][-1]

        # Status Moagem
        selecao_moagem = dados_projecao[unidade]["moagem"]
        meta_unidade_hora = dados_projecao[unidade]['meta_moagem']/24
        moagem_status = 'Moagem acima da meta nas últimas 3 horas'
        try:
            validacao = []
            for n in range(1,4):
                chave_moagem = list(selecao_moagem.keys())[-n]
                status = True if selecao_moagem[chave_moagem]["total"] < meta_unidade_hora else False
                validacao.append(status)
            if [True,True,True] == validacao:
                moagem_status = 'Moagem ABAIXO da meta em todas as últimas 3 horas'
        except: moagem_status = "Não Avaliado"

        # Cargas
        selecao_cargas = dados_projecao[unidade]["cargas"]
        meta_carga_minima = dados_projecao[unidade]["carga_minima"]
        cargas_status = 'Cargas ABAIXO da meta nas últimas 3 horas'
        try:
            validacao = []
            for n in range(1,4):
                chave_cargas = list(selecao_cargas.keys())[-n]
                status = True if selecao_cargas[chave_cargas]["pp"] > meta_carga_minima else False
                validacao.append(status)
            if [True,True,True] == validacao:
                cargas_status = 'Cargas ACIMA da meta em todas as últimas 3 horas'
        except: cargas_status = "Não Avaliado"

        # Status Apontamento
        status_apontamento = dados_projecao[unidade]["status"]
        linhas.append((unidade,projecao_nominal,moagem_status,cargas_status,status_apontamento))

    df = pd.DataFrame(linhas,columns=["Unidade","Projecao 6h","Status Moagem","Status Cargas","Status Apontamento"])

    def gatilho_mandar(row):
        condicoes = 0
        if isinstance(row["Projecao 6h"], timedelta) and row["Projecao 6h"] > pd.to_timedelta('03:00:00'):
            condicoes += 1
        if row["Status Moagem"] == 'Moagem ABAIXO da meta em todas as últimas 3 horas' and row["Status Apontamento"] == 'Limitação':
            condicoes += 1
        if row["Status Cargas"] == "Cargas ACIMA da meta em todas as últimas 3 horas":
            condicoes += 1
        
        if condicoes == 3:
            return True
        else:
            return False

    df["GATILHO_MANDAR"] = df.apply(gatilho_mandar, axis=1)

    df.to_excel('geracao_gatilhos_projecao_CCT.xlsx', index=False)

    dfvro = df[(df["Unidade"]=="VALE DO ROSARIO") & (df["GATILHO_MANDAR"]==True)]

    if len(dfvro) > 0:
        mensagem = "🔴 *Gatilho para Troca de Cana: VALER para SELIS*"
        mensagem += "\nCritérios considerados da Projeção CCT Vale do Rosário"
        mensagem += "\n*1º Critério* Projeção nominal da 6ª hora acima de 03:00:00"
        mensagem += "\n*2º Critério* Moagem ABAIXO da meta nas últimas 3 horas por motivo Industrial"
        mensagem += "\n*3º Critério* Cargas ACIMA do Pátio Minimo PP nas últimas 3 horas"

        contato, tipo_contato = verificar_tipo_de_contato('19998326554')
        gravar_em_banco_para_envio([('CCT_TrocaDeCana', datetime.now(), contato, tipo_contato, mensagem, '')])

        contato, tipo_contato = verificar_tipo_de_contato('19998703275')
        gravar_em_banco_para_envio([('CCT_TrocaDeCana', datetime.now(), contato, tipo_contato, mensagem, '')])

def geracao_PDF_TO_old():
    df = pd.read_excel(r'\\CSCLSFSR01\Agricola$\Logistica Agroindustrial\CIA 22.23\11. Analytics\BOT CIA\Contatos\Envio_PDF_TO.xlsx')
    df = df[df.Ativo=='SIM']
    df = df.dropna(subset='Destino')
    undGrupoTO = dict(zip(df.Unidade,df.Destino))

    caminho_pasta_pdfs = verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\MinhaTI\Metas_Gov_Indicadores - Documentos\PDF - RPA')
    lista = os.listdir(caminho_pasta_pdfs)

    pdfs_INC_RITM = dict([(file.split('-')[2].strip()+'_INC' if 'Incidentes' in file else file.split('-')[2].strip()+'_RITM',file) for file in lista if 'Requisições' in file or 'Incidentes' in file])

    def geracao_mensagem_TO(chave, nome_pdf):
        tipo_envio = 'Incidentes' if 'INC' in chave else 'Requisições'
        unidade = chave.split('_')[0]
        if nome_pdf == False:
            mensagem = f'''❌ PDF {tipo_envio} {unidade} não existe!\nVerificar desenvolvimento do mesmo com Governança Operacional.'''
            return mensagem, False
        caminho_absoluto = os.path.abspath(os.path.join(caminho_pasta_pdfs,nome_pdf))
        datamodific = os.path.getmtime(caminho_absoluto)
        datamodific_ = datetime.fromtimestamp(datamodific)
        datamodific_1 = datamodific_.strftime('%d/%m/%Y %H:%M:%S')
        if datamodific_ > datetime.now()-timedelta(hours=2):
            mensagem = f'''🛰️ PDF {tipo_envio} {unidade}\nÚltima atualização foi em {datamodific_1}.'''
            return mensagem, caminho_absoluto
        else:
            mensagem = f'''❌ PDF {tipo_envio} {unidade} não está atualizado!\nSua última atualização foi em {datamodific_1}.'''
            return mensagem, False

    for und in undGrupoTO.keys():
        for tipo in ['_INC','_RITM']:
            chave = und+tipo
            caminho_pdf = pdfs_INC_RITM[chave] if chave in pdfs_INC_RITM.keys() else False
            mensagem, anexo = geracao_mensagem_TO(chave, caminho_pdf)
            contato, tipo_contato = verificar_tipo_de_contato(undGrupoTO[und])
            gravar_em_banco_para_envio([('TO_PDF', datetime.now(), contato, tipo_contato, mensagem, '')])
            if anexo != False:
                gravar_em_banco_para_envio([('TO_PDF', datetime.now(), contato, tipo_contato, '', anexo)])

def compilado_de_panes_comboio():
    caminho_raiz = verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\MinhaTI\CIA Analytics - BOT CIA\Extrator\Azure')
    #cam_arquivos = [file for file in os.listdir(caminho_raiz) if 'DDN_' in file]
    
    hoje = (datetime.now()).date()
    arquivos = os.listdir(caminho_raiz)
    cam_arquivos = [
        file for file in arquivos
        if 'DDN_' in file and (datetime.fromtimestamp(os.path.getmtime(os.path.join(caminho_raiz, file)))).date() == hoje
    ]

    dft = pd.DataFrame([])
    for arquivo in cam_arquivos:
        caminho_parquet = os.path.join(caminho_raiz,arquivo)
        df_temp = pd.read_parquet(caminho_parquet, engine="pyarrow")
        df_temp = df_temp[["CD_EQUIPAMENTO","DESC_GRUPO_EQUIPAMENTO","HR_LOCAL","CD_OPERACAO","VL_HR_OPERACIONAIS"]]
        if 'D1_' in arquivo: df_temp["DT_LOCAL"] = (datetime.now()-timedelta(days=1)).date() 
        else: df_temp["DT_LOCAL"] = datetime.now().date()
        df_temp["data_corrida"] = pd.to_datetime(df_temp["DT_LOCAL"]) + pd.to_timedelta(df_temp["HR_LOCAL"].astype(int), unit='h')
        dft = pd.concat([dft,df_temp])

    dft = dft[dft["CD_OPERACAO"]==211]
    dft["TURNO"] = ['A' if row in [7,8,9,10,11,12,13,14] else 'B' if row in [15,16,17,18,19,20,21,22] else 'C' for row in dft["HR_LOCAL"].astype(int)]
    dft["Frota"] = dft["CD_EQUIPAMENTO"].astype(str) + " " + dft["DESC_GRUPO_EQUIPAMENTO"].astype(str)
    dft["UNIDADE"] = [row[:3] for row in dft["DESC_GRUPO_EQUIPAMENTO"].astype(str)]
    dft["Data"] = dft["DT_LOCAL"].astype(str) + " " + dft["TURNO"].astype(str)
    dft = dft[dft['data_corrida'] >= (datetime.now()-timedelta(hours=22))]

    dfg = dft.groupby(['UNIDADE', "Frota"])["VL_HR_OPERACIONAIS"].sum().reset_index()
    dfg["211"] = dfg["VL_HR_OPERACIONAIS"].apply(lambda x: timedelta(seconds=x))
    #dfg = dfg.to_frame('211')
    dfg = dfg.reset_index(drop=False)
    dfg = dfg[dfg['211'] > timedelta(minutes=10)]
    dfg = dfg.sort_values(by=["211"], ascending=False)
    data_selec = str(datetime.now().date())
    turno_selec = 'A' if datetime.now().hour in [7,8,9,10,11,12,13,14] else 'B' if datetime.now().hour in [15,16,17,18,19,20,21,22] else 'C'
    data_format = datetime.now().strftime('%d/%m/%Y')
    try: 
        dfg.to_excel(r'\\CSCLSFSR01\Agricola$\Logistica Agroindustrial\Compilado_Panes_Comboio.xlsx')
    except: pass

    mensagem = []
    mensagem.append(f'*Compilado de Panes* ⛽\nDia: {data_format}\nTurno: {turno_selec}')
    for und_s in dfg['UNIDADE'].unique():
        mensagem.append(f"\nUnidade: *{und_s}* - {str(dfg[dfg['UNIDADE']==und_s]['211'].sum()).replace('0 days ','')}")
        for id, row in dfg[dfg['UNIDADE']==und_s].iterrows():
            frota_row, frente_row = row["Frota"].split(' ')
            pane_row = str(row["211"]).replace('0 days ','')
            mensagem.append(f'Frota: {frota_row} [{frente_row}] - {pane_row}')

    mensagem_para_envio = '\n'.join(mensagem)
    contato, tipo_contato = verificar_tipo_de_contato('BOT CIA - Manut. & Comb.')
    gravar_em_banco_para_envio([('Comboio_Compilado_panes', datetime.now(), contato, tipo_contato, mensagem_para_envio, '')])
    contato, tipo_contato = verificar_tipo_de_contato('Report Pane-Seca')
    gravar_em_banco_para_envio([('Comboio_Compilado_panes', datetime.now(), contato, tipo_contato, mensagem_para_envio, '')])
    gravar_em_banco_para_envio([('DEBUG',datetime.now(),'11963208908', 'Contato',mensagem_para_envio,'')])

#################### COMPLIANCE
#19 97126-1795   19998326554


def path_atualizado_ultima_hora(caminho_arquivo):
    while True:
        try:
            arquivo__ = os.path.getmtime(caminho_arquivo)
            break
        except: 
            print(f'Não encontramos o arquvio: {caminho_arquivo}')
            sleep(1)
    arquivo__ = datetime.fromtimestamp(arquivo__)
    if arquivo__ > (datetime.now()-timedelta(minutes=datetime.now().minute)): return True
    else: return False

def path_atualizado_ultimos_5_minutos(caminho_arquivo):
    while True:
        try:
            arquivo__ = os.path.getmtime(caminho_arquivo)
            break
        except: 
            print(f'Não encontramos o arquivo: {caminho_arquivo}\n')
            sleep(1)
    arquivo__ = datetime.fromtimestamp(arquivo__)
    if arquivo__ > (datetime.now()-timedelta(minutes=3)): return True
    else: return False

def path_atualizado_ultimos_10_minutos(caminho_arquivo):
    while True:
        try:
            arquivo__ = os.path.getmtime(caminho_arquivo)
            break
        except: 
            print(f'Não encontramos o arquvio: {caminho_arquivo}')
            sleep(1)
    arquivo__ = datetime.fromtimestamp(arquivo__)
    if arquivo__ > (datetime.now()-timedelta(minutes=20)): return True
    else: return False

def pegar_ultimo_arquivo_exportacao_monit():
    pasta = verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\CIA Analytics - BOT CIA\Extrator\SGPA3\Exportacao Monit')
    arquivo = os.listdir(pasta)[-1]
    return os.path.join(pasta,arquivo)

################
gravar_em_banco_para_envio([('DEBUG',datetime.now(),'11963208908', 'Contato','Robo iniciado','')])
pma_om_h = -1
caminho_base_deslocamento = pegar_ultimo_arquivo_exportacao_monit()
caminho_base_export_monit = verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\CIA Analytics - BOT CIA\Extrator\SGPA3\Exportacao Monit.db')
caminho_base_export_monit_F = verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\CIA Analytics - BOT CIA\Extrator\SGPA3\Exportacao Monit F.db')
caminho_base_velocidade_cd = verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\CIA Analytics - BOT CIA\Extrator\CCT\CD_Hora.xlsx')
trava_atualizacao_deslocamento = os.path.getmtime(caminho_base_deslocamento)
trava_atualizacao_export_monit = os.path.getmtime(caminho_base_export_monit)
trava_atualizacao_cd_hora = os.path.getmtime(caminho_base_velocidade_cd)
trava_tablet_comboio = datetime.now()
trava_tablet_comboio_BONF = datetime.now()
trava_dds_programado = datetime.now()
trava_dds_demanda = datetime.now()
trava_aderencia_analise_cenario_CCT = -1
trava_plantio_hora = -1
trava_hora_vel_cd = -1
trava_analise_cen_cct = -1
loop_rep_cct = -1
trava_hora_vel_pl = -1
trava_analise_df = -1
trava_rpm = -1
trava_hora_PR_apontamento = -1
trava_hora_PI_report_rapido = datetime.now().hour #-1
trava_hora_BT_apontamento = -1

cam_PI_moagem = verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\CIA Analytics - BOT CIA\Extrator\PI System\dados_moagem.xlsx')
cam_PI_cargas = verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\CIA Analytics - BOT CIA\Extrator\PI System\dados_cargas.xlsx')
caminho_arquivo_plantio_hora = verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\CIA Analytics - BOT CIA\Extrator\PLANTIO\Plantio_Hora.xlsx')
caminho_ton_cana = verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\CIA Analytics - BOT CIA\Extrator\CCT\Moagem\Ton_Cana.xlsx')
caminho_agron_df = verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\CIA Analytics - BOT CIA\Extrator\AGRON\agron_comunicacao.xlsx')
caminho_PMA_HIST = verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\CIA Analytics - BOT CIA\Extrator\Manutencao\PMA\HIST_PMA.xlsx')
caminho_DW_Transb = verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\CIA Analytics - BOT CIA\Extrator\CCT\Transbordo.xlsx')
caminho_reprovadosLPI = verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\CIA Analytics - BOT CIA\Extrator\Manutencao\buffer_eixos_reprovados.xlsx')
caminho_parquet_plantio_muda = verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\CIA Analytics - BOT CIA\Extrator\Azure\SGPA2_DDN_HORAS_OPERACIONAIS_ON_EQUIP_PL_MU.parquet')
trava_manut_ifrota = manut_ifrota_trava()
trava_caminho_base_TO_inc = datetime.fromtimestamp(os.path.getmtime(verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\CIA Analytics - BOT CIA\Extrator\Service Now\INC CHAMADOS TO.xls')))
trava_caminho_base_TO_ritm = datetime.fromtimestamp(os.path.getmtime(verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\CIA Analytics - BOT CIA\Extrator\Service Now\RITM CHAMADOS TO.xlsx')))
modificacao_velocidadeDMC = (datetime.fromtimestamp(os.path.getmtime(caminho_parquet_plantio_muda))).hour
trava_horaria_to_pdf = datetime.now().hour
trava_caminho_comboio_pane = datetime.fromtimestamp(os.path.getmtime(verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\CIA Analytics - BOT CIA\Extrator\Azure\SGPA2_DDN_HORAS_OPERACIONAIS_ON_COLHEDORA_CCT_MO.parquet')))
trava_gatilho_comboio_pane = -1
gatilho_troca_de_cana = -1
gatilho_carretasLPI = -1
#velocidade_CD_V2()
#geracao_PDF_TO()
#gerar_mensagem_report_colheita_rapido()
#compilado_de_panes_comboio()
#gerar_gatilho_troca_de_cana()
#velocidade_plantadoras_DMC()

#gatilho_tipo_1_compliance()
#gatilho_tipo_2_compliance()
#gatilho_tipo_3_compliance()
#gerar_mensagens_kronos()

#verificar_previsibilidade_df()
#colhedoras_improdutivas_CCT_SPGA3()
#gerar_mensagens_herbicida()
#gerar_mensagens_MANUTENCAO_IFROTA()

#ciclo_report_moagem()
#gerar_mensagem_report_colheita_rapido()
#geracao_PDF_TO()
#compilado_de_panes_comboio()
#gerar_imagens_deslocamento()
'''for n in range(10):
    print(n)
    mensagens_compliance_SGPA3()
    sleep(60)'''

'''            if datetime.now() > trava_tablet_comboio and datetime.now().hour in [8,16,1] and datetime.now().minute >= 20 and datetime.now().minute < 27 and baseComboioExtrator():
                print(f'{datetime.now()} -> Inicio Turno Tablet')
                trava_tablet_comboio = datetime.now()+timedelta(hours=4)
                try: 
                    print('gatilho_iniciar_verificacao_tablet_Comboio()')
                    gatilho_iniciar_verificacao_tablet_Comboio()
                except: print(f'{datetime.now()} -> Fim Turno Tablet')
            if datetime.now() > trava_tablet_comboio_BONF and datetime.now().hour in [9,19,3] and datetime.now().minute >= 20 and datetime.now().minute < 25 and baseComboioExtrator():
                print(f'{datetime.now()} -> Inicio Turno Tablet (BONF)')
                trava_tablet_comboio_BONF = datetime.now()+timedelta(hours=4)
                try: 
                    print('gatilho_iniciar_verificacao_tablet_Comboio_BONF()')
                    gatilho_iniciar_verificacao_tablet_Comboio_BONF()
                except: print(f'{datetime.now()} -> Fim Turno Tablet (BONF)')'''

try: 
    print('desponibilidade_caminhoes_CCT()')
    desponibilidade_caminhoes_CCT()
except Exception as e:
    print(f'Erro encontrado em "desponibilidade_caminhoes_CCT" - {datetime.now()}\nErro: {e}')

try: 
    print('mensagens_compliance_SGPA3()')
    mensagens_compliance_SGPA3()
except Exception as e:
    print(f'Erro encontrado em "mensagens_compliance_SGPA3()" - {datetime.now()}\nErro: {e}')

try: 
    print('apontamento_manutencao_SPGA3()')
    apontamento_manutencao_SPGA3()
except Exception as e:
    print(f'Erro encontrado em "apontamento_manutencao_SPGA3()" - {datetime.now()}\nErro: {e}')

try: 
    print('bloqueio_despacho_carretas()')
    bloqueio_despacho_carretas()
except Exception as e:
    print(f'Erro encontrado em "bloqueio_despacho_carretas()" - {datetime.now()}\nErro: {e}')

try: 
    print('contatos_cia_C')
    contatos_cia_C = contatos_cia()
except Exception as e:
    print(f'Erro encontrado em "contatos_cia_C" - {datetime.now()}\nErro: {e}')

try: 
    print('analise_cenario_cct()')
    analise_cenario_cct()
except Exception as e:
    print(f'Erro encontrado em "analise_cenario_cct()" - {datetime.now()}\nErro: {e}')

try: 
    print('verificacao_pdfs_atualizados_whatsapp()')
    verificacao_pdfs_atualizados_whatsapp()
except Exception as e:
    print(f'Erro encontrado em "verificacao_pdfs_atualizados_whatsapp()" - {datetime.now()}\nErro: {e}')
    
print('Feito!')
def monitoramento():
    global con, pma_om_h, trava_tablet_comboio, trava_tablet_comboio_BONF, trava_dds_programado, trava_dds_demanda, trava_plantio_hora, trava_atualizacao_deslocamento, caminho_base_deslocamento, loop_rep_cct, trava_hora_vel_pl, caminho_arquivo_plantio_hora, caminho_agron_df, caminho_ton_cana, trava_atualizacao_cd_hora, trava_atualizacao_export_monit, trava_hora_vel_cd, trava_analise_cen_cct, trava_analise_df, trava_rpm, trava_aderencia_analise_cenario_CCT,caminho_PMA_HIST, caminho_DW_Transb, trava_hora_PR_apontamento,trava_hora_PI_report_rapido,cam_PI_moagem,cam_PI_cargas, trava_manut_ifrota, trava_hora_BT_apontamento, trava_horaria_to_pdf, trava_caminho_base_TO_inc, trava_caminho_base_TO_ritm,trava_caminho_comboio_pane, trava_gatilho_comboio_pane, gatilho_troca_de_cana, gatilho_carretasLPI, modificacao_velocidadeDMC
    for y in range(500):
        try:
            con = sqlite3.connect(r"C:\CIAANALYTICS\1 - Producao\1 4 - Banco\envio_msg.db")
            #con = sqlite3.connect(r'\\CSCLSFSR01\Agricola$\Logistica Agroindustrial\CIA 22.23\11. Analytics\1 4 - Banco\envio_msg.db")
            if datetime.now().minute >= 40 and gatilho_troca_de_cana != datetime.now().hour:
                gatilho_troca_de_cana = datetime.now().hour
                try: 
                    print('gerar_gatilho_troca_de_cana()')
                    gerar_gatilho_troca_de_cana()
                except: print('Erro em : gerar_gatilho_troca_de_cana')
            if y % 10 == 0:
                try: 
                    print('verificacao_pdfs_atualizados_whatsapp()')
                    verificacao_pdfs_atualizados_whatsapp()
                except: print('Erro encontrado em "verificacao_pdfs_atualizados_whatsapp" - ',datetime.now())

            if datetime.now().hour != trava_hora_PI_report_rapido and datetime.fromtimestamp(os.path.getmtime(cam_PI_moagem)).hour == datetime.now().hour and datetime.now().minute in [5,6,7,8]: 
                if str(datetime.now().date()) not in ['2024-08-10','2024-08-11']:
                    if datetime.fromtimestamp(os.path.getmtime(cam_PI_cargas)).hour == datetime.now().hour:
                        try:
                            print('gerar_mensagem_report_colheita_rapido()')
                            gerar_mensagem_report_colheita_rapido()
                        except: print('Erro em geração de report rápido.')
                        trava_hora_PI_report_rapido = datetime.now().hour

            if datetime.now().minute > 15 and datetime.now().hour != gatilho_carretasLPI and datetime.fromtimestamp(os.path.getmtime(caminho_reprovadosLPI)) > datetime.now()-timedelta(minutes=30):
                try: 
                    print('bloqueio_despacho_carretas()')
                    bloqueio_despacho_carretas()
                    gatilho_carretasLPI = datetime.now().hour
                except: print('Erro em: bloqueio_despacho_carretas')

            try: 
                print('apontamento_manutencao_SPGA3()')
                apontamento_manutencao_SPGA3()
            except: print('Erro em: apontamento_manutencao_SPGA3')

            if y % 2 == 0:
                try:
                    refdthr = datetime.now()
                    if refdthr.minute > 5 and refdthr.minute < 25:
                        mod_velDMC = datetime.fromtimestamp(os.path.getmtime(caminho_parquet_plantio_muda))
                        if mod_velDMC.date() == refdthr.date() and mod_velDMC.hour == refdthr.hour and mod_velDMC.hour != modificacao_velocidadeDMC:
                            modificacao_velocidadeDMC = refdthr.hour
                            velocidade_plantadoras_DMC()
                except: print('\n*******************\nTivemos um erro em velocidade_plantadoras_DMC()')
                try:
                    if datetime.now().weekday() == 0 and datetime.now().hour >= 6 and datetime.now().hour < 18:
                        caminhoPdfReportManutencao = verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\CIA Analytics - BOT CIA\Extrator\Reports\Report_Manutencao.html')
                        if os.path.exists(caminhoPdfReportManutencao):
                            geracao_reportManutEmail = datetime.fromtimestamp(os.path.getmtime(caminhoPdfReportManutencao))
                            if geracao_reportManutEmail < datetime.now() and geracao_reportManutEmail.date() != datetime.now().date():
                                geracao_relatorio_email_mautencao_OS_Aguardando_info()
                        else:
                            geracao_relatorio_email_mautencao_OS_Aguardando_info()
                except:
                    print('Tivemos um erro na geração de email Report Manutenção via E-mail.')
                try:
                    print('Verificar_Panes_Secas()') 
                    Verificar_Panes_Secas()
                except: print('Falhamo em Verificar_Panes_Secas')
                # Compliance 
                try: 
                    print('mensagens_compliance_SGPA3()') 
                    mensagens_compliance_SGPA3()
                except: print(f'Erro em mensagens_compliance_SGPA3')          
            try:
                if gatilho_atualizar_hist_frotas_compliance():
                    print('Atualizado bases comparativas')
                    contatos_cia_C = contatos_cia()
                    print('gatilho_tipo_3_compliance()') 
                    gatilho_tipo_3_compliance()
            except: print('Erro em analise cenario compliance 3')
            if datetime.now().hour in [14,22,6] and datetime.fromtimestamp(os.path.getmtime(verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\CIA Analytics - BOT CIA\Extrator\Azure\SGPA2_DDN_HORAS_OPERACIONAIS_ON_COLHEDORA_CCT_MO.parquet'))) >= datetime.now()-timedelta(minutes=10) and trava_gatilho_comboio_pane != datetime.now().hour:
                if str(datetime.now().date()) not in ['2024-08-10','2024-08-11']:
                    try: 
                        print('compilado_de_panes_comboio()') 
                        compilado_de_panes_comboio()
                    except: pass
                    trava_caminho_comboio_pane = datetime.fromtimestamp(os.path.getmtime(verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\CIA Analytics - BOT CIA\Extrator\Azure\SGPA2_DDN_HORAS_OPERACIONAIS_ON_COLHEDORA_CCT_MO.parquet')))
                    trava_gatilho_comboio_pane = datetime.now().hour
            if path_atualizado_ultimos_10_minutos(caminho_PMA_HIST) == True and path_atualizado_ultimos_10_minutos(caminho_DW_Transb) == True and path_atualizado_ultimos_10_minutos(caminho_base_velocidade_cd) == True and datetime.now().minute > 25 and datetime.now().minute < 50 and trava_analise_df != datetime.now().hour and datetime.now().hour > 3:
                print('\n---> Iniciando lógica de verificar_previsibilidade_df')
                try: 
                    print('verificar_previsibilidade_df()') 
                    verificar_previsibilidade_df()
                except: print(f'*** erro em verificar_previsibilidade_df.\n')
                trava_analise_df = datetime.now().hour
            
            if datetime.now().minute >= 20:
                try:
                    caminho_inc = r'\\CSCLSFSR01\Agricola$\Logistica Agroindustrial\CIA 22.23\11. Analytics\BOT CIA\Arquivos\bases\INC CHAMADOS TO.xls'
                    caminho_ritm = r'\\CSCLSFSR01\Agricola$\Logistica Agroindustrial\CIA 22.23\11. Analytics\BOT CIA\Arquivos\bases\RITM CHAMADOS TO.xlsx'

                    # Verifica se ambos os arquivos existem e foram atualizados recentemente
                    if (
                        datetime.fromtimestamp(os.path.getmtime(caminho_inc)) > datetime.now() - timedelta(minutes=35)
                        and datetime.fromtimestamp(os.path.getmtime(caminho_ritm)) > datetime.now() - timedelta(minutes=35)
                        and trava_horaria_to_pdf != datetime.now().hour
                    ):
                        print('\n---> Iniciando lógica de trava_horaria_to_pdf')
                        try:
                            print('geracao_PDF_TO()')
                            #geracao_PDF_TO()
                        except Exception as e:
                            print(f'*** Erro durante a geração do PDF: {e}')

                        # Atualiza as variáveis de trava
                        trava_caminho_base_TO_inc = datetime.fromtimestamp(os.path.getmtime(caminho_inc))
                        trava_caminho_base_TO_ritm = datetime.fromtimestamp(os.path.getmtime(caminho_ritm))
                        trava_horaria_to_pdf = datetime.now().hour

                except FileNotFoundError as e:
                    # Captura e exibe qual arquivo está ausente
                    print(f'Arquivo não encontrado: {e.filename}')

            if path_atualizado_ultimos_10_minutos(verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\CIA Analytics - BOT CIA\Extrator\CCT\Moagem\Ton_Cana.xlsx')) == True and datetime.now().minute > 23 and datetime.now().minute < 43 and trava_analise_cen_cct != datetime.now().hour:
                print('\n---> Iniciando lógica de analise_cenario_cct')
                try: 
                    print('analise_cenario_cct()') 
                    analise_cenario_cct()
                except IndexError as error: print(f'*** erro em analise_cenario_cct: \n{error}\n')
                trava_analise_cen_cct = datetime.now().hour
            if path_atualizado_ultimos_10_minutos(verificar_base_atualizada(r'C:\Users\ciaanalytics\MinhaTI\CIA Analytics - BOT CIA\Extrator\Azure\SGPA2_DDN_HORAS_OPERACIONAIS_ON_COLHEDORA_CCT_MO.parquet')) == True and datetime.now().minute > 10 and datetime.now().minute < 35 and trava_hora_vel_cd != datetime.now().hour:
                
                if str(datetime.now().date()) not in ['2024-08-10','2024-08-11']:
                    print('\n---> Iniciando lógica de Velocidade Colhedoras')
                    try: 
                        print('velocidade_CD_V2()')
                        velocidade_CD_V2()
                    except IndexError as error: print(f'*** erro em velocidade CD: \n{error}\n')
                    trava_hora_vel_cd = datetime.now().hour
            if datetime.now().minute > 24 and loop_rep_cct != datetime.now().hour and datetime.now().minute < 35 and datetime.now().hour != 0 and path_atualizado_ultima_hora(caminho_ton_cana) == True:
                if str(datetime.now().date()) not in ['2024-08-10','2024-08-11']:
                    print('--> Ciclo Report Colheita')
                    try: 
                        print('ciclo_report_moagem()')
                        ciclo_report_moagem()
                    except: print(f'------> Problemas com ciclo report moagem!!!\n')
                loop_rep_cct = datetime.now().hour
            try:
                # Deslocamento
                caminho_base_deslocamento = pegar_ultimo_arquivo_exportacao_monit()
                if trava_atualizacao_deslocamento != os.path.getmtime(caminho_base_deslocamento):
                    print(f'{datetime.now()} -> Inicio Deslocamento')
                    try: 
                        print('gerar_imagens_deslocamento()')
                        gerar_imagens_deslocamento()
                    except: pass
                    while True:
                        try:
                            trava_atualizacao_deslocamento = os.path.getmtime(caminho_base_deslocamento)
                            break
                        except: 
                            print('error em desloc.')
                            sleep(1)
                    print(f'{datetime.now()} -> Fim Deslocamento')
            except: print('Pulamos deslocamento')
            try:
                # Segundo Pane Seca
                if trava_atualizacao_export_monit != os.path.getmtime(caminho_base_export_monit):
                    print(f'{datetime.now()} -> Inicio Dados Pane Seca')
                    try: 
                        print('comparar_segunda_func_pane_seca()')
                        comparar_segunda_func_pane_seca()
                    except: print('Erro em comparar_segunda_func_pane_seca()')
                    while True:
                        try:
                            trava_atualizacao_export_monit = os.path.getmtime(caminho_base_export_monit)
                            break
                        except IndexError as error_: 
                            print(f'Error em atualizar momento de atualização base: Dados Pane Seca (export monit)\n{error_}')
                            sleep(1)
            except: print(f'Error em Dados Pane Seca')
            if datetime.now().hour != trava_plantio_hora and datetime.now().minute > 35 and datetime.now().minute < 40 and datetime.now().hour in [3,6,9,12,15,18,21,0] and path_atualizado_ultima_hora(caminho_arquivo_plantio_hora) == True:
               try: 
                   print('gerar_mensagen_relacao_plantio_hora()')
                   gerar_mensagen_relacao_plantio_hora()
               except: pass
               trava_plantio_hora = datetime.now().hour
               print(f'{datetime.now()} -> Plantio horario')
            if datetime.now() > trava_dds_programado and datetime.now().hour in [10,18,2] and datetime.now().minute >= 0 and datetime.now().minute < 5:
                print(f'{datetime.now()} -> Inicio DDS Programado')
                try: 
                    print('enviar_dds()')
                    enviar_dds()
                except: pass
                trava_dds_programado = datetime.now()+timedelta(hours=4)
                print(f'{datetime.now()} -> Fim DDS Programado')
            if datetime.now() > trava_dds_demanda and datetime.now().hour in [10,18,2] and datetime.now().minute >= 10 and datetime.now().minute < 20:
                print(f'{datetime.now()} -> Inicio DDS Demanda')
                try: 
                    print('enviar_dds_personalizado()')
                    enviar_dds_personalizado()
                except: pass
                trava_dds_demanda = datetime.now()+timedelta(hours=4)
                print(f'{datetime.now()} -> Fim DDS Demanda')
            if str(datetime.now().date()) not in ['2024-08-10','2024-08-11']:
                if datetime.now().minute > 1 and pma_om_h != datetime.now().hour and datetime.now().minute < 7 and basePMA_Atualizada():
                    print(f'{datetime.now()} -> Inicio Overview Manutenção')
                    try: 
                        print('envio_mensagens_PMA()')
                        envio_mensagens_PMA()
                    except Exception as e:
                        print(f'Overview manutenção: {e}')
                    pma_om_h = datetime.now().hour
                    print(f'{datetime.now()} -> Fim Overview Manutenção')
            atualizacao_df()
            if y % 2 == 0:
                try: 
                    print('colhedoras_improdutivas_CCT_SPGA3()')
                    colhedoras_improdutivas_CCT_SPGA3()
                except Exception as e:
                    print(f'ERRO colhedoras_improdutivas_CCT_SPGA3: {e}')
                try: 
                    print('desponibilidade_caminhoes_CCT()')
                    desponibilidade_caminhoes_CCT()
                except Exception as e:
                    print(f'ERRO desponibilidade_caminhoes_CCT: {e}')
                try: 
                    print('sem_apontamentoSGPA3()')
                    sem_apontamentoSGPA3()
                except Exception as e:
                    print(f'ERRO sem_apontamentoSGPA3: {e}')
            
            try: 
                print('Controle_envio_1f()')
                Controle_envio_1f()
            except Exception as e:
                    print(f'ERRO Controle_envio_1f: {e}')
            try: 
                print('atualizar_vn_df()')
                atualizar_vn_df()
            except Exception as e:
                    print(f'ERRO atualizar_vn_df: {e}')

            if datetime.now().minute in [15,16,17,18] and trava_hora_BT_apontamento != datetime.now().hour: 
                trava_hora_BT_apontamento = datetime.now().hour
                try: 
                    print('gerar_mensagens_kronos()')
                    gerar_mensagens_kronos()
                except Exception as e:
                    print(f'ERRO gerar_mensagens_kronos: {e}')
            
            if str(datetime.now().date()) not in ['2024-08-10','2024-08-11']:
                try: 
                    print('logica_os_ag_info()')
                    logica_os_ag_info()
                    
                except Exception as e:
                    print(f'ERRO logica_os_ag_info: {e}')

            print(f"\n{datetime.now()} -> COUNT:::",y,'\n\n')
            # Sleep
            if datetime.now().minute in [15,16]: sleep(10)
            else: sleep(40)
        except Exception as e:
                print(f'\n{datetime.now()}\nEncontrado erro!!!\n\n{e}')
                sleep(90)
                pass


###################################
###### LOOP
trava_tablet_comboio = datetime.now()
trava_tablet_comboio_BONF = datetime.now()
while True:
    try:
        monitoramento()
    except:
        sleep(30)
        print('\n\n\nErro!!!\n\n\n')
        sleep(30)
        pass
