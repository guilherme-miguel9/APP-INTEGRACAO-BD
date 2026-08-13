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
    'Nome da Origem.1': 'data_atual',
    'Nº': 'n',
    'Nº item da ordem': 'numero_item_ordem',
    'Instal': 'instalacao',
    'Registrador': 'registrador',
    'Rua': 'rua',
    'Nº da casa': 'n_casa',
    'Sequência': 'sequencia',
    'Contrato': 'contrato',
    'Latitude localiz.geográfica': 'latitude',
    'Longitude localiz.geográfica': 'longitude',
    'Val Fat': 'valor_fatura',
    'NomeCliente': 'nome_cliente',
    'Complemento': 'complemento',
    'Ponto Ref': 'ponto_ref',
    'Local': 'municipio',
    'Bairro': 'bairro',
    'Sigla edifício': 'sigla_edificio',
    'Nº sala': 'n_sala',
    'Andar': 'andar',
    'Complemento endereco': 'complemento_endereco',
    'ObjLigacao': 'objeto_ligacao',
    'Nº Poste': 'n_poste',
    'Nº Serie': 'n_serie',
    'Unid.leit': 'unidade_leitura',
    'O. leitura real': 'o_leitura_real',
    'O. Sem leit real': 'o_sem_leitura_real',
    'Nota leit.': 'nota_leitura',
    'Hora leit.': 'hora_leitura',
    'Seq.Mod': 'seqmod',
    'Cond WOL': 'condwol',
    'Leit': 'codigo_leitor',
    'Nome leit': 'nome_leit',
    'Indic Foto': 'indicador_foto',
    'Interv.Leit': 'intervalo_leitura',
    'Cta.contr.': 'conta_contrato',
    'Abaixo lim': 'abaixo_lim',
    'Excede lim': 'excede_lim',
    'Desvio leit': 'desvio_leitura',
    'Fat. Assin': 'fat_assin',
    'Coment.leitura': 'comentario_leitura',
    'Coment.fatura': 'comentario_fatura',
    'Tipo rota': 'tipo_rota',
    'Tipo ordem': 'tipo_ordem',
    'Impresso': 'impresso',
    'ResCampo': 'res_campo',
    'FA CT OK': 'fact_ok'
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

        df['hora_leitura'] = pd.to_datetime(df['hora_leitura'], format='%H:%M:%S', errors='coerce').dt.time
        df['intervalo_leitura'] = pd.to_datetime(df['intervalo_leitura'], format='%H:%M:%S', errors='coerce').dt.time
        df['data_atual'] = pd.to_datetime(df['data_atual'], format='%d.%m.%Y', errors='coerce')

        df['latitude'] = pd.to_numeric(
            df['latitude'].astype(str).str.replace(',', '.', regex=False),
            errors='coerce'
        )

        df['longitude'] = pd.to_numeric(
            df['longitude'].astype(str).str.replace(',', '.', regex=False),
            errors='coerce'
        )

        df['valor_fatura'] = pd.to_numeric(
            df['valor_fatura'].astype(str).str.replace(',', '.', regex=False),
            errors='coerce'
        )

        dias = df['data_atual'].dt.day.dropna().unique().tolist()
        df['data_atual'] = pd.to_datetime(df['data_atual'], errors='coerce').dt.strftime('%Y-%m-%d')

        # Garantir que 100% dos nomes das colunas estejam em minúsculo para o PostgreSQL
        df.columns = [str(c).lower() for c in df.columns]

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
