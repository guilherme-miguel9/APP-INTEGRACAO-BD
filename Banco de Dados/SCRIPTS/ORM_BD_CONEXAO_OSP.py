from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import psycopg2
from psycopg2 import extras
import os
import glob
import numpy as np
import time
from datetime import datetime
from pandas.api.types import is_datetime64_any_dtype
import configparser

#Configurações do banco de dados
ini_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'config.ini')
config = configparser.ConfigParser()
config.read(ini_path)

db_host = config['database']['host']
db_user = config['database']['user']
db_password = config['database']['password']
db_name = config['database']['dbname']



caminhopasta = r""
dfs = []



#########################################################
#funçao para ler os arquivos
arquivos = glob.glob(os.path.join(caminhopasta, '*.xlsm'))


print(arquivos)

abas = ['OSP']


colunasarrumadas = {
        'Centro de leitura' : str,
        'Instalação de origem' : str, 
        'Conta de contrato' : str, 
        'Instalação Destino' : str, 
        'Contrato': str,
        'MRU de destino' : str,
        'Nº rota' : str,
        'Fatura entregue' : str,
        'Hora da Impressão' : str,
        'Quant.d.imp.d.fatura' : str,
        'Quant.d.pagina impr.' : str,
        'Motivo de não impr.' : str,
        'Foto' : str,
        'Descrição do motivo da não impressão' : str,
        'Texto da fatura' : str,
        'Cod do leitor' : str,
        'Nome do leiturista' : str,
        'Nº doc.impressão' : str,
        'Rua' : str,
        'Nº' : str,
        'Suplemento' : str,
        'Bairro' : str,
        'Local' : str,
        'Nº sala' : str,
        'Andar' : str,
        'Rua 5' : str,
        'Rua 4' : str,
        'Nº sala_1' : str,
        'Suplemento_2' : str,
        'Sigla edifício' : str,
        'resultados diferidos' : str,
        'Versão do objeto' : str,
        'BASE DE CALCULO' : str,
        'STATUS' : str,
        'CRITERIO DO CALCULO' : str,
        'Latitude localiz.geográfica' : float,
        'Longitude localiz.geográfica' : float
}

for arquivo in arquivos:
    nome_arquivo = os.path.basename(arquivo)  
    df = pd.read_excel(arquivo, engine='openpyxl', dtype=colunasarrumadas, sheet_name="OSP")  
    dfs.append(df)


#funciona por aba de planilha
df = pd.concat(dfs, ignore_index=True)



df = df.fillna('')
df = df.replace(to_replace=['NaT', 'nan', 'NaN'], value=None)
df = df.where(pd.notnull(df), None)
#########################################################



colunas_para_remover = ['TROCAS','CONCAT', 'ATIVOS']

df = df.drop(columns=colunas_para_remover)

# Colunas a esquerda são do excel e direita do banco de dados
mapeamento_colunas = {
 'Data' : 'Data_atual',
 'Centro de leitura' : 'centro_leitura',
 'Instalação de origem' : 'instalacao_origem',
 'Conta de contrato' : 'conta_contrato',
 'Instalação Destino' : 'instalacao_destino',
 'Contrato' : 'contrato',
 'MRU de destino' : 'MRU',
 'Nº rota' : 'n_rota',
 'Data Ent.prev.' : 'data_ent_prev',
 'Data de entrega' : 'data_entrega',
 'Hora da impressão' : 'hora',
 'Fatura entregue' : 'fatura_entregue',
 'Quant.d.imp.d.fatura' : 'quant_imp_fatura',
 'Quant.d.pagina impr.' : 'quant_pag_imp',
 'Motivo de não impr.' : 'motivo_nao_imp',
 'Foto' : 'foto',
 'Descrição do motivo da não impressão' : 'desc_motivo_nao_imp',
 'Texto da fatura' : 'text_fat',
 'Cod do leitor' : 'codigo_leitor',
 'Nome do leiturista' : 'nome_leit',
 'Valor da fatura' : 'valor_fatura',
 'Nº doc.impressão' : 'n_doc_imp',
 'Data de lançamento' : 'data_lancamento',
 'Data de apresentação' : 'data_apresentacao',
 'DT vencimento' : 'data_vencimento',
 'Rua' : 'rua',
 'Nº' : 'numero',
 'Suplemento' : 'suplemento',
 'Bairro' : 'bairro',
 'Local' : 'base',
 'Nº sala' : 'n_sala',
 'Andar' : 'andar',
 'Rua 5' : 'rua_5',
 'Rua 4' : 'rua_4',
 'Nº sala_1' : 'n_sala_1',
 'Suplemento_2' : 'suplemento_2',
 'Sigla edifício' : 'sigla',
 'Latitude localiz.geográfica' : 'latitude',
 'Longitude localiz.geográfica' : 'longitude',
 'resultados diferidos' : 'resultado_diferido',
 'Versão do objeto' : 'vers_objeto',
 'BASE DE CALCULO' : 'base_calculo',
 'STATUS' : 'status',
 'CRITERIO DO CALCULO' : 'criterio_calculo',
 'CONFERENCIA' : 'conferencia'
}



df.rename(columns=mapeamento_colunas, inplace=True)



for col in df.columns:
    if is_datetime64_any_dtype(df[col]) and col.startswith('data'):
        df[col] = df[col].dt.date
        


print(df['hora'].head())


df['hora'] = pd.to_datetime(df['hora'], format='%H:%M:%S', errors='coerce') \
                           .dt.strftime('%H:%M:%S')

print(df['hora'].head())

df['hora'] = df['hora'].where(df['hora'].notna(), None)

df['hora'] = df['hora'].apply(lambda x: datetime.strptime(x, '%H:%M:%S').time() if pd.notnull(x) else None)

df['latitude'] = df['latitude'].astype(str).str.replace(',', '.').astype(float)
df['longitude'] = df['longitude'].astype(str).str.replace(',', '.').astype(float)


df['valor_fatura'] = df['valor_fatura'].astype(float)


def preparar_df_para_postgres(df):
    if df is None:
        print("Erro: O DataFrame recebido é None!")
        return None

    for col in df.columns:
        print(f"Processando coluna: {col}")

        
        if is_datetime64_any_dtype(df[col]) and col.lower().startswith('data'):
            df[col] = df[col].dt.date

        
        elif is_datetime64_any_dtype(df[col]) and col.lower().startswith('hora'):
            
            df[col] = df[col].dt.strftime('%H:%M:%S')

        
        elif df[col].dtype == 'object':
            df[col] = df[col].replace('NaT', None)
            df[col] = df[col].replace(pd.NaT, None)

    
    df = df.where(pd.notnull(df), None)

    return df


df = preparar_df_para_postgres(df)

print((df == 'NaT').sum())
print(df.dtypes)

#########################################################
#Tentar conexão com banco de dados e inserir os dados
try:
    
    with psycopg2.connect(
        dbname=db_name,
        user=db_user,
        password=db_password,
        host=db_host,
        port="5432"
    ) as conn:
        conn.set_client_encoding('UTF8')

        
        cursor = conn.cursor()

        cursor.execute("SET datestyle TO 'ISO, DMY';")
        
    

        data_to_insert = [tuple(row) for row in df.itertuples(index=False, name=None)]
    
    #modificar a tabela que vai ser inserida no BANCO DE DADOS
        query = f"INSERT INTO [table] ({', '.join(df.columns)}) VALUES ({', '.join(['%s']*len(df.columns))})"

        print(f"Quantidade de registros: {len(data_to_insert)}")
        

        extras.execute_batch(cursor, query, data_to_insert, page_size=20000)

        conn.commit()
        print("fez conexao")
       
except Exception as e:
    print(f"Ocorreu um erro: {e}")

finally:
    if cursor:
        cursor.close()

    if conn:
        conn.close()
#########################################################
