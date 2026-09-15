import os
import re
import time
from datetime import datetime
import pandas as pd
from pandas.api.types import is_datetime64_any_dtype

from OSP.src.signals import signals
from OSP.src.database import inserir_dataframe_no_banco

colunasarrumadas = {
    'Centro de leitura': str,
    'Instalação de origem': str,
    'Conta de contrato': str,
    'Instalação Destino': str,
    'Contrato': str,
    'MRU de destino': str,
    'Nº rota': str,
    'Fatura entregue': str,
    'Hora da Impressão': str,
    'Hora da impressão': str,
    'Quant.d.imp.d.fatura': str,
    'Quant.d.pagina impr.': str,
    'Motivo de não impr.': str,
    'Foto': str,
    'Descrição do motivo da não impressão': str,
    'Texto da fatura': str,
    'Cod do leitor': str,
    'Nome do leiturista': str,
    'Nº doc.impressão': str,
    'Rua': str,
    'Nº': str,
    'Suplemento': str,
    'Bairro': str,
    'Local': str,
    'Nº sala': str,
    'Andar': str,
    'Rua 5': str,
    'Rua 4': str,
    'Nº sala_1': str,
    'Suplemento_2': str,
    'Sigla edifício': str,
    'resultados diferidos': str,
    'Versão do objeto': str,
    'BASE DE CALCULO': str,
    'STATUS': str,
    'CRITERIO DO CALCULO': str,
    'Latitude localiz.geográfica': float,
    'Longitude localiz.geográfica': float
}

mapeamento_colunas = {
    'Data': 'data_atual',
    'Nome da Origem.1': 'data_atual',
    'Centro de leitura': 'centro_leitura',
    'Instalação de origem': 'instalacao_origem',
    'Conta de contrato': 'conta_contrato',
    'Instalação Destino': 'instalacao_destino',
    'Contrato': 'contrato',
    'MRU de destino': 'mru',
    'Nº rota': 'n_rota',
    'Data Ent.prev.': 'data_ent_prev',
    'Data de entrega': 'data_entrega',
    'Hora da impressão': 'hora',
    'Hora da Impressão': 'hora',
    'Fatura entregue': 'fatura_entregue',
    'Quant.d.imp.d.fatura': 'quant_imp_fatura',
    'Quant.d.pagina impr.': 'quant_pag_imp',
    'Motivo de não impr.': 'motivo_nao_imp',
    'Foto': 'foto',
    'Descrição do motivo da não impressão': 'desc_motivo_nao_imp',
    'Texto da fatura': 'text_fat',
    'Cod do leitor': 'codigo_leitor',
    'Nome do leiturista': 'nome_leit',
    'Valor da fatura': 'valor_fatura',
    'Nº doc.impressão': 'n_doc_imp',
    'Data de lançamento': 'data_lancamento',
    'Data de apresentação': 'data_apresentacao',
    'DT vencimento': 'data_vencimento',
    'Rua': 'rua',
    'Nº': 'numero',
    'Suplemento': 'suplemento',
    'Bairro': 'bairro',
    'Local': 'base',
    'Nº sala': 'n_sala',
    'Andar': 'andar',
    'Rua 5': 'rua_5',
    'Rua 4': 'rua_4',
    'Nº sala_1': 'n_sala_1',
    'Suplemento_2': 'suplemento_2',
    'Sigla edifício': 'sigla',
    'Latitude localiz.geográfica': 'latitude',
    'Longitude localiz.geográfica': 'longitude',
    'resultados diferidos': 'resultado_diferido',
    'Versão do objeto': 'vers_objeto',
    'BASE DE CALCULO': 'base_calculo',
    'STATUS': 'status',
    'CRITERIO DO CALCULO': 'criterio_calculo',
    'CONFERENCIA': 'conferencia'
}

def listar_arquivos_excel(caminho_pasta: str):
    """
    Retorna uma lista de caminhos absolutos de arquivos .xlsx, .xlsm e .xls na pasta.
    """
    if not caminho_pasta or not os.path.isdir(caminho_pasta):
        return []
    
    extensoes_validas = ('.xlsx', '.xlsm', '.xls')
    arquivos_encontrados = []
    
    for f in os.listdir(caminho_pasta):
        if f.startswith('~$'):
            continue  # ignorar arquivos temporarios do Excel
        if f.lower().endswith(extensoes_validas):
            arquivos_encontrados.append(os.path.join(caminho_pasta, f))
            
    arquivos_encontrados.sort()
    return arquivos_encontrados

def formatar_tempo(segundos: float) -> str:
    minutos = int(segundos // 60)
    segs = int(segundos % 60)
    return f"{minutos:02d}:{segs:02d}"

def preparar_df_para_postgres(df):
    if df is None:
        return None

    for col in df.columns:
        if is_datetime64_any_dtype(df[col]) and col.lower().startswith('data'):
            df[col] = df[col].dt.date
        elif is_datetime64_any_dtype(df[col]) and col.lower().startswith('hora'):
            df[col] = df[col].dt.strftime('%H:%M:%S')
        elif df[col].dtype == 'object':
            df[col] = df[col].replace('NaT', None)
            df[col] = df[col].replace(pd.NaT, None)

    df = df.where(pd.notnull(df), None)
    return df

def processar_pasta_excel(caminho_pasta, tabela_leitura, schema_leitura):
    start_time_global = time.time()

    ano_tabela_box = re.search(r'\d+', tabela_leitura)
    if not ano_tabela_box:
        signals.update_message.emit("Nenhum ano numérico encontrado no nome da tabela selecionada.", "red")
        signals.set_buttons_enabled.emit(True)
        return

    ano_str = ano_tabela_box.group()

    dicionario_meses = {
        "jan_" + ano_str: 1,
        "fev_" + ano_str: 2,
        "mar_" + ano_str: 3,
        "abr_" + ano_str: 4,
        "mai_" + ano_str: 5,
        "jun_" + ano_str: 6,
        "jul_" + ano_str: 7,
        "ago_" + ano_str: 8,
        "set_" + ano_str: 9,
        "out_" + ano_str: 10,
        "nov_" + ano_str: 11,
        "dez_" + ano_str: 12,
        "teste_" + ano_str: 5
    }

    try:
        arquivos = listar_arquivos_excel(caminho_pasta)
        if not arquivos:
            signals.update_message.emit("Nenhum arquivo Excel (.xlsx, .xlsm, .xls) encontrado na pasta selecionada.", "red")
            signals.set_buttons_enabled.emit(True)
            return

        dfs = []
        total_arquivos = len(arquivos)
        
        for idx, arq in enumerate(arquivos, start=1):
            nome_arquivo = os.path.basename(arq)
            
            elapsed = time.time() - start_time_global
            timer_str = formatar_tempo(elapsed)
            progresso_pct = int(((idx - 1) / total_arquivos) * 50)
            
            signals.update_progress.emit(progresso_pct, f"{progresso_pct}%")
            signals.update_metrics.emit(timer_str, "- MB/s")
            signals.update_message.emit(f"Lendo com calamine [{idx}/{total_arquivos}]: {nome_arquivo}...", "blue")

            abas = pd.ExcelFile(arq, engine='calamine')
            # Prioriza a aba 'OSP', se existir; caso contrário lê as abas disponíveis
            abas_para_ler = [s for s in abas.sheet_names if s.strip().upper() == 'OSP']
            if not abas_para_ler:
                abas_para_ler = abas.sheet_names

            for aba in abas_para_ler:
                df_aba = pd.read_excel(arq, sheet_name=aba, engine='calamine', dtype=colunasarrumadas)
                # Se não houver coluna Data na planilha, extrai a data do nome do arquivo (primeiros 10 chars)
                if 'Data' not in df_aba.columns and 'data_atual' not in df_aba.columns:
                    df_aba['Data'] = nome_arquivo[:10]
                dfs.append(df_aba)

            progresso_fim_arq = int((idx / total_arquivos) * 50)
            signals.update_progress.emit(progresso_fim_arq, f"{progresso_fim_arq}%")

        if not dfs:
            signals.update_message.emit("Nenhum dado pôde ser lido das planilhas.", "red")
            signals.set_buttons_enabled.emit(True)
            return

        elapsed = time.time() - start_time_global
        signals.update_metrics.emit(formatar_tempo(elapsed), "- MB/s")
        signals.update_message.emit("Consolidando planilhas e aplicando tratamentos Pandas...", "blue")
        signals.update_progress.emit(50, "50%")

        df = pd.concat(dfs, ignore_index=True)
        df = df.fillna('')
        df = df.replace(to_replace=['NaT', 'nan', 'NaN'], value=None)
        df = df.where(pd.notnull(df), None)

        colunas_para_remover = ['TROCAS', 'CONCAT', 'ATIVOS']
        cols_existentes = [c for c in colunas_para_remover if c in df.columns]
        if cols_existentes:
            df.drop(columns=cols_existentes, inplace=True)

        df.rename(columns=mapeamento_colunas, inplace=True)

        for col in df.columns:
            if is_datetime64_any_dtype(df[col]) and col.lower().startswith('data'):
                df[col] = df[col].dt.date

        if 'hora' in df.columns:
            df['hora'] = pd.to_datetime(df['hora'], format='%H:%M:%S', errors='coerce').dt.strftime('%H:%M:%S')
            df['hora'] = df['hora'].where(df['hora'].notna(), None)
            df['hora'] = df['hora'].apply(lambda x: datetime.strptime(x, '%H:%M:%S').time() if pd.notnull(x) else None)

        if 'latitude' in df.columns:
            df['latitude'] = pd.to_numeric(
                df['latitude'].astype(str).str.replace(',', '.', regex=False),
                errors='coerce'
            )

        if 'longitude' in df.columns:
            df['longitude'] = pd.to_numeric(
                df['longitude'].astype(str).str.replace(',', '.', regex=False),
                errors='coerce'
            )

        if 'valor_fatura' in df.columns:
            df['valor_fatura'] = pd.to_numeric(
                df['valor_fatura'].astype(str).str.replace(',', '.', regex=False),
                errors='coerce'
            )

        df = preparar_df_para_postgres(df)

        datas = []
        if 'data_atual' in df.columns:
            datas_series = pd.to_datetime(df['data_atual'], errors='coerce')
            datas = datas_series.dt.strftime('%Y-%m-%d').dropna().unique().tolist()
            df['data_atual'] = datas_series.dt.strftime('%Y-%m-%d')

        # Converte 100% dos nomes das colunas para minúsculo para compatibilidade com o PostgreSQL
        df.columns = [str(c).lower() for c in df.columns]

        # Validação do mês da tabela selecionada vs dados da planilha
        for sufixo_mes, num_mes in dicionario_meses.items():
            if tabela_leitura.endswith(sufixo_mes) and 'data_atual' in df.columns:
                primeira_data = pd.to_datetime(df['data_atual'].dropna().iloc[0]) if not df['data_atual'].dropna().empty else None
                if primeira_data and primeira_data.month != num_mes:
                    raise SystemExit(
                        f"O mês da tabela selecionada ({tabela_leitura}) não corresponde ao mês das planilhas lidas ({primeira_data.month}). "
                        "Por favor, selecione a tabela correta."
                    )

        signals.update_progress.emit(55, "55%")
        signals.update_message.emit("Tratamento Pandas concluído. Iniciando envio ao PostgreSQL...", "green")

    except SystemExit as e:
        signals.update_message.emit(f"ERRO: {e}", "red")
        signals.set_buttons_enabled.emit(True)
        return

    except Exception as e:
        signals.update_message.emit(f"Erro ao processar planilhas OSP: {e}", "red")
        signals.set_buttons_enabled.emit(True)
        return

    # Inserção no banco com monitoramento em tempo real do stream COPY
    inserir_dataframe_no_banco(df, datas, tabela_leitura, schema_leitura, start_time_global)
