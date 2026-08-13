import os
import re
import time
import pandas as pd

from OSB.src.signals import signals
from OSB.src.database import inserir_dataframe_no_banco

colunasarrumadas = {
    'Nº': str,
    'Nº item da ordem': str,
    'Instal': str,
    'Registrador': str,
    'Nº da casa': str,
    'Sequência': str,
    'Contrato': str,
    'ObjLigacao': str,
    'Nº Poste': str,
    'Nº Serie': str,
    'Unid.leit': str,
    'Cta.contr.': str,
    'Coment.leitura': str
}

mapeamento_colunas = {
    'Nome da Origem.1': 'Data_Atual',
    'Nº': 'N',
    'Nº item da ordem': 'Numero_Item_Ordem',
    'Instal': 'Instalacao',
    'Registrador': 'Registrador',
    'Rua': 'Rua',
    'Nº da casa': 'N_casa',
    'Sequência': 'Sequencia',
    'Contrato': 'Contrato',
    'Latitude localiz.geográfica': 'Latitude',
    'Longitude localiz.geográfica': 'Longitude',
    'Val Fat': 'Valor_fatura',
    'NomeCliente': 'Nome_Cliente',
    'Complemento': 'Complemento',
    'Ponto Ref': 'Ponto_Ref',
    'Local': 'Municipio',
    'Bairro': 'Bairro',
    'Sigla edifício': 'Sigla_Edificio',
    'Nº sala': 'N_sala',
    'Andar': 'Andar',
    'Complemento endereco': 'Complemento_Endereco',
    'ObjLigacao': 'Objeto_Ligacao',
    'Nº Poste': 'N_poste',
    'Nº Serie': 'N_serie',
    'Unid.leit': 'Unidade_Leitura',
    'O. leitura real': 'O_Leitura_Real',
    'O. Sem leit real': 'O_Sem_Leitura_Real',
    'Nota leit.': 'Nota_Leitura',
    'Hora leit.': 'Hora_Leitura',
    'Seq.Mod': 'SeqMod',
    'Cond WOL': 'CondWOL',
    'Leit': 'Codigo_Leitor',
    'Nome leit': 'Nome_Leit',
    'Indic Foto': 'Indicador_Foto',
    'Interv.Leit': 'Intervalo_leitura',
    'Cta.contr.': 'Conta_Contrato',
    'Abaixo lim': 'Abaixo_Lim',
    'Excede lim': 'Excede_Lim',
    'Desvio leit': 'Desvio_Leitura',
    'Fat. Assin': 'Fat_Assin',
    'Coment.leitura': 'Comentario_Leitura',
    'Coment.fatura': 'Comentario_Fatura',
    'Tipo rota': 'Tipo_Rota',
    'Tipo ordem': 'Tipo_Ordem',
    'Impresso': 'Impresso',
    'ResCampo': 'Res_campo',
    'FA CT OK': 'FACT_OK'
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
            data_origem = nome_arquivo[:10]  # Pega os 10 primeiros caracteres do nome do arquivo
            
            elapsed = time.time() - start_time_global
            timer_str = formatar_tempo(elapsed)
            progresso_pct = int(((idx - 1) / total_arquivos) * 50)
            
            signals.update_progress.emit(progresso_pct, f"{progresso_pct}%")
            signals.update_metrics.emit(timer_str, "- MB/s")
            signals.update_message.emit(f"Lendo com calamine [{idx}/{total_arquivos}]: {nome_arquivo}...", "blue")

            # Leitura estrita utilizando calamine sem fallback
            abas = pd.ExcelFile(arq, engine='calamine')
            for aba in abas.sheet_names:
                df_aba = pd.read_excel(arq, sheet_name=aba, engine='calamine', dtype=colunasarrumadas)
                df_aba['Nome da Origem.1'] = data_origem
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

        # Remover colunas da planilha que não existem na tabela do banco de dados
        colunas_para_remover = ['resultados diferidos', 'Versão do objeto', 'TROCAS', 'CONCAT', 'ATIVOS']
        cols_existentes = [c for c in colunas_para_remover if c in df.columns]
        if cols_existentes:
            df.drop(columns=cols_existentes, inplace=True)

        df.rename(columns=mapeamento_colunas, inplace=True)

        df['Hora_Leitura'] = pd.to_datetime(df['Hora_Leitura'], format='%H:%M:%S', errors='coerce').dt.time
        df['Intervalo_leitura'] = pd.to_datetime(df['Intervalo_leitura'], format='%H:%M:%S', errors='coerce').dt.time
        df['Data_Atual'] = pd.to_datetime(df['Data_Atual'], format='%d.%m.%Y', errors='coerce')

        df['Latitude'] = pd.to_numeric(
            df['Latitude'].astype(str).str.replace(',', '.', regex=False),
            errors='coerce'
        )

        df['Longitude'] = pd.to_numeric(
            df['Longitude'].astype(str).str.replace(',', '.', regex=False),
            errors='coerce'
        )

        df['Valor_fatura'] = pd.to_numeric(
            df['Valor_fatura'].astype(str).str.replace(',', '.', regex=False),
            errors='coerce'
        )

        dias = df['Data_Atual'].dt.day.dropna().unique().tolist()
        df['Data_Atual'] = pd.to_datetime(df['Data_Atual'], errors='coerce').dt.strftime('%Y-%m-%d')

        if tabela_leitura in dicionario_meses:
            mes_tabela_box = dicionario_meses[tabela_leitura]
            mes_excel = pd.to_datetime(df['Data_Atual'].iloc[0]).month

            if mes_excel != mes_tabela_box:
                raise SystemExit(f"O mês da tabela selecionada ({tabela_leitura}) não corresponde ao mês das planilhas lidas ({mes_excel}). Por favor, selecione a tabela correta.")

        signals.update_progress.emit(55, "55%")
        signals.update_message.emit("Tratamento Pandas concluído. Iniciando envio ao PostgreSQL...", "green")

    except SystemExit as e:
        signals.update_message.emit(f"ERRO: {e}", "red")
        signals.set_buttons_enabled.emit(True)
        return

    except Exception as e:
        signals.update_message.emit(f"Erro ao processar planilhas com calamine: {e}", "red")
        signals.set_buttons_enabled.emit(True)
        return

    # Inserção no banco com monitoramento em tempo real do stream COPY
    inserir_dataframe_no_banco(df, dias, tabela_leitura, schema_leitura, start_time_global)
