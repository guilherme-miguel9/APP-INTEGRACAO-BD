import pandas as pd
import psycopg2
from psycopg2 import extras, sql
import os
import configparser
from sqlalchemy import create_engine, inspect
from urllib.parse import quote_plus
import threading
import sys
import re
import time
from io import StringIO
from pathlib import Path

from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QLineEdit, QPushButton, QComboBox, QListWidget,
    QProgressBar, QFrame, QStackedWidget, QFileDialog
)
from PySide6.QtCore import Qt, Signal, QObject
from PySide6.QtGui import QIcon, QFont


# ---------------------------------------------------------
# Configurações e caminhos
# ---------------------------------------------------------

def resource_path(filename: str) -> Path:
    if getattr(sys, 'frozen', False):
        base_path = Path(sys._MEIPASS)
    else:
        base_path = Path(__file__).resolve().parents[2]
    
    file_path = base_path / filename
    if not file_path.exists():
        alt_path = Path(__file__).resolve().parent / filename
        if alt_path.exists():
            return alt_path
        alt_exec = Path(sys.executable).parent / filename
        if alt_exec.exists():
            return alt_exec
        alt_root = Path.cwd() / filename
        if alt_root.exists():
            return alt_root
    return file_path


ini_path = resource_path('config.ini')
config = configparser.ConfigParser()
config.read(ini_path, encoding='utf-8')

db_host = config['database']['host'] if config.has_section('database') and 'host' in config['database'] else 'localhost'
db_name = config['database']['dbname'] if config.has_section('database') and 'dbname' in config['database'] else 'postgres'

icon_path = resource_path(os.path.join('Banco de Dados', 'icones', 'icone_aplicativo.ico'))


# ---------------------------------------------------------
# Variáveis Globais de Estado
# ---------------------------------------------------------

dataframe = []
parar_barra_progresso = threading.Event()
tempo_medio_por_linha = 0.05
ponteiro_01 = False

login_usuario = None
login_senha = None

engine = None
insp = None

caminho_arquivo = ""
nome_arquivo = ""
abas = None


# ---------------------------------------------------------
# Mapeamentos de Colunas (Estrutura Mantida Intacta)
# ---------------------------------------------------------

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


# ---------------------------------------------------------
# Sistema de Sinais Qt (Thread-Safety)
# ---------------------------------------------------------

class AppSignals(QObject):
    update_progress = Signal(int, str)
    update_message = Signal(str, str)
    update_quantity = Signal(str)
    set_buttons_enabled = Signal(bool)
    login_result = Signal(bool, str)
    schemas_loaded = Signal(list)
    tables_loaded = Signal(list)


signals = AppSignals()


# ---------------------------------------------------------
# Estilo QSS Minimalista e Arredondado (Dark Theme)
# ---------------------------------------------------------

QSS_STYLE = """
QMainWindow {
    background-color: #0f1117;
}

QStackedWidget {
    background-color: #0f1117;
}

QFrame#cardFrame {
    background-color: #181b24;
    border: 1px solid #262a36;
    border-radius: 16px;
}

QLabel {
    color: #e2e8f0;
    font-family: 'Segoe UI', system-ui, sans-serif;
    font-size: 13px;
}

QLabel#titleLabel {
    font-size: 28px;
    font-weight: bold;
    color: #ffffff;
    letter-spacing: 0.5px;
}

QLabel#subtitleLabel {
    font-size: 13px;
    color: #818cf8;
    font-weight: 500;
}

QLabel#sectionTitle {
    font-size: 15px;
    font-weight: 600;
    color: #f1f5f9;
}

QLineEdit {
    background-color: #222634;
    border: 1px solid #33394b;
    border-radius: 10px;
    padding: 10px 14px;
    color: #f8fafc;
    font-size: 13px;
}

QLineEdit:focus {
    border: 1px solid #6366f1;
    background-color: #262b3a;
}

QPushButton {
    background-color: #6366f1;
    color: #ffffff;
    border: none;
    border-radius: 10px;
    padding: 11px 20px;
    font-size: 13px;
    font-weight: 600;
}

QPushButton:hover {
    background-color: #4f46e5;
}

QPushButton:pressed {
    background-color: #4338ca;
}

QPushButton:disabled {
    background-color: #334155;
    color: #64748b;
}

QPushButton#secondaryButton {
    background-color: #242836;
    color: #cbd5e1;
    border: 1px solid #33394b;
}

QPushButton#secondaryButton:hover {
    background-color: #2e3446;
    color: #ffffff;
    border-color: #475569;
}

QComboBox {
    background-color: #222634;
    border: 1px solid #33394b;
    border-radius: 10px;
    padding: 8px 14px;
    color: #f8fafc;
    font-size: 13px;
}

QComboBox:hover {
    border-color: #475569;
}

QComboBox QAbstractItemView {
    background-color: #1e2230;
    border: 1px solid #33394b;
    border-radius: 8px;
    selection-background-color: #6366f1;
    color: #f8fafc;
    padding: 4px;
}

QListWidget {
    background-color: #222634;
    border: 1px solid #33394b;
    border-radius: 10px;
    color: #f8fafc;
    padding: 6px;
    font-size: 13px;
}

QListWidget::item {
    padding: 6px 10px;
    border-radius: 6px;
}

QProgressBar {
    background-color: #222634;
    border: none;
    border-radius: 8px;
    height: 10px;
    text-align: center;
    color: transparent;
}

QProgressBar::chunk {
    background-color: #6366f1;
    border-radius: 8px;
}
"""


# ---------------------------------------------------------
# Janela Principal PySide6 (Lumi)
# ---------------------------------------------------------

class LumiWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Lumi")
        self.resize(850, 720)

        if os.path.exists(icon_path):
            self.setWindowIcon(QIcon(str(icon_path)))

        self.setStyleSheet(QSS_STYLE)

        self.central_widget = QWidget()
        self.setCentralWidget(self.central_widget)

        self.main_layout = QVBoxLayout(self.central_widget)
        self.main_layout.setContentsMargins(0, 0, 0, 0)

        self.stacked_widget = QStackedWidget()
        self.main_layout.addWidget(self.stacked_widget)

        # Construir telas
        self.init_login_screen()
        self.init_main_screen()

        # Conectar Sinais Qt
        signals.login_result.connect(self.on_login_result)
        signals.update_progress.connect(self.on_update_progress)
        signals.update_message.connect(self.on_update_message)
        signals.update_quantity.connect(self.on_update_quantity)
        signals.set_buttons_enabled.connect(self.on_set_buttons_enabled)
        signals.tables_loaded.connect(self.on_tables_loaded)

    # ---------------------------------------------------------
    # Tela de Login
    # ---------------------------------------------------------
    def init_login_screen(self):
        login_container = QWidget()
        layout_outer = QVBoxLayout(login_container)
        layout_outer.setAlignment(Qt.AlignCenter)

        card = QFrame()
        card.setObjectName("cardFrame")
        card.setFixedWidth(420)
        card_layout = QVBoxLayout(card)
        card_layout.setContentsMargins(36, 36, 36, 36)
        card_layout.setSpacing(16)

        # Header
        lbl_app_name = QLabel("Lumi")
        lbl_app_name.setObjectName("titleLabel")
        lbl_app_name.setAlignment(Qt.AlignCenter)

        lbl_sub = QLabel("Conexão ao Banco de Dados")
        lbl_sub.setObjectName("subtitleLabel")
        lbl_sub.setAlignment(Qt.AlignCenter)

        card_layout.addWidget(lbl_app_name)
        card_layout.addWidget(lbl_sub)
        card_layout.addSpacing(10)

        # Usuário
        lbl_user = QLabel("Usuário")
        self.entry_usuario = QLineEdit()
        self.entry_usuario.setPlaceholderText("Digite seu usuário")

        # Senha
        lbl_pass = QLabel("Senha")
        self.entry_senha = QLineEdit()
        self.entry_senha.setEchoMode(QLineEdit.Password)
        self.entry_senha.setPlaceholderText("Digite sua senha")

        # Mensagem de Login
        self.mensagem_login = QLabel("")
        self.mensagem_login.setWordWrap(True)
        self.mensagem_login.setAlignment(Qt.AlignCenter)

        # Botão Entrar
        self.botao_login = QPushButton("Entrar")
        self.botao_login.setCursor(Qt.PointingHandCursor)
        self.botao_login.clicked.connect(self.realizar_login)

        self.entry_usuario.returnPressed.connect(self.realizar_login)
        self.entry_senha.returnPressed.connect(self.realizar_login)

        card_layout.addWidget(lbl_user)
        card_layout.addWidget(self.entry_usuario)
        card_layout.addWidget(lbl_pass)
        card_layout.addWidget(self.entry_senha)
        card_layout.addWidget(self.mensagem_login)
        card_layout.addSpacing(10)
        card_layout.addWidget(self.botao_login)

        layout_outer.addWidget(card)
        self.stacked_widget.addWidget(login_container)

    def realizar_login(self):
        usuario_digitado = self.entry_usuario.text().strip()
        senha_digitada = self.entry_senha.text()

        if not usuario_digitado or not senha_digitada:
            self.mensagem_login.setText("Por favor, preencha usuário e senha.")
            self.mensagem_login.setStyleSheet("color: #f87171;")
            return

        self.botao_login.setEnabled(False)
        self.mensagem_login.setText("Conectando ao banco de dados...")
        self.mensagem_login.setStyleSheet("color: #60a5fa;")

        threading.Thread(
            target=self._test_login_connection,
            args=(usuario_digitado, senha_digitada),
            daemon=True
        ).start()

    def _test_login_connection(self, usuario_digitado, senha_digitada):
        global login_usuario, login_senha, engine, insp
        try:
            senha_escapada = quote_plus(senha_digitada)
            engine_teste = create_engine(
                f'postgresql://{usuario_digitado}:{senha_escapada}@{db_host}:5432/{db_name}'
            )
            insp_teste = inspect(engine_teste)
            insp_teste.get_schema_names()

            login_usuario = usuario_digitado
            login_senha = senha_digitada
            engine = engine_teste
            insp = insp_teste

            signals.login_result.emit(True, "Login validado com sucesso!")
        except Exception as e:
            signals.login_result.emit(False, f"Falha no login: {e}")

    def on_login_result(self, success, message):
        if success:
            schemas = insp.get_schema_names()
            self.schema_box.blockSignals(True)
            self.schema_box.clear()
            self.schema_box.addItems(schemas)
            self.schema_box.blockSignals(False)

            if schemas:
                self.atualizar_box_tabelas()

            self.stacked_widget.setCurrentIndex(1)
        else:
            self.mensagem_login.setText(message)
            self.mensagem_login.setStyleSheet("color: #f87171;")
            self.botao_login.setEnabled(True)

    # ---------------------------------------------------------
    # Tela Principal
    # ---------------------------------------------------------
    def init_main_screen(self):
        main_container = QWidget()
        layout_outer = QVBoxLayout(main_container)
        layout_outer.setContentsMargins(40, 30, 40, 30)
        layout_outer.setSpacing(20)

        # Top Header Bar
        header_layout = QHBoxLayout()
        lbl_app = QLabel("Lumi")
        lbl_app.setObjectName("titleLabel")

        lbl_desc = QLabel("Integração Excel para PostgreSQL")
        lbl_desc.setObjectName("subtitleLabel")

        header_info = QVBoxLayout()
        header_info.addWidget(lbl_app)
        header_info.addWidget(lbl_desc)

        header_layout.addLayout(header_info)
        header_layout.addStretch()
        layout_outer.addLayout(header_layout)

        # Card Configurações
        card_config = QFrame()
        card_config.setObjectName("cardFrame")
        config_layout = QVBoxLayout(card_config)
        config_layout.setContentsMargins(28, 24, 28, 24)
        config_layout.setSpacing(14)

        lbl_section1 = QLabel("Configurações do Banco de Dados")
        lbl_section1.setObjectName("sectionTitle")
        config_layout.addWidget(lbl_section1)

        # Botão escolher Excel & ListBox
        excel_row = QHBoxLayout()
        self.botao_escolher_excel = QPushButton("Selecionar Arquivo Excel")
        self.botao_escolher_excel.setObjectName("secondaryButton")
        self.botao_escolher_excel.setCursor(Qt.PointingHandCursor)
        self.botao_escolher_excel.clicked.connect(self.selecionar_arquivo_excel)

        self.lista_box = QListWidget()
        self.lista_box.setFixedHeight(42)

        excel_row.addWidget(self.botao_escolher_excel)
        excel_row.addWidget(self.lista_box, stretch=1)
        config_layout.addLayout(excel_row)

        lbl_msg_schema = QLabel("Escolha a pasta e logo após a tabela para inserir os dados do Excel")
        lbl_msg_schema.setStyleSheet("color: #94a3b8;")
        config_layout.addWidget(lbl_msg_schema)

        # Comboboxes Schema & Tabela
        combo_row = QHBoxLayout()

        lbl_s = QLabel("Schema:")
        self.schema_box = QComboBox()
        self.schema_box.currentIndexChanged.connect(self.atualizar_box_tabelas)

        lbl_t = QLabel("Tabela:")
        self.tabela_box = QComboBox()

        combo_row.addWidget(lbl_s)
        combo_row.addWidget(self.schema_box, stretch=1)
        combo_row.addSpacing(20)
        combo_row.addWidget(lbl_t)
        combo_row.addWidget(self.tabela_box, stretch=1)

        config_layout.addLayout(combo_row)

        # Botão Processar Excel
        self.botao_abrir_excel = QPushButton("Processar e Importar Excel")
        self.botao_abrir_excel.setCursor(Qt.PointingHandCursor)
        self.botao_abrir_excel.clicked.connect(self.threading_processar_excel)
        config_layout.addWidget(self.botao_abrir_excel)

        layout_outer.addWidget(card_config)

        # Card Status e Progresso
        card_status = QFrame()
        card_status.setObjectName("cardFrame")
        status_layout = QVBoxLayout(card_status)
        status_layout.setContentsMargins(28, 24, 28, 24)
        status_layout.setSpacing(12)

        lbl_section2 = QLabel("Status do Processamento")
        lbl_section2.setObjectName("sectionTitle")
        status_layout.addWidget(lbl_section2)

        self.mensagem = QLabel("Aguardando seleção do arquivo...")
        self.mensagem.setObjectName("statusLabel")
        self.mensagem.setWordWrap(True)
        status_layout.addWidget(self.mensagem)

        self.mensagem_quantidade = QLabel("Quantidade de registros: -")
        self.mensagem_quantidade.setStyleSheet("color: #c084fc; font-weight: 500;")
        status_layout.addWidget(self.mensagem_quantidade)

        prog_row = QHBoxLayout()
        self.progress = QProgressBar()
        self.progress.setRange(0, 100)
        self.progress.setValue(0)

        self.label_porcentagem = QLabel("0%")
        self.label_porcentagem.setStyleSheet("color: #94a3b8; font-weight: 600; min-width: 40px;")
        self.label_porcentagem.setAlignment(Qt.AlignRight | Qt.AlignVCenter)

        prog_row.addWidget(self.progress, stretch=1)
        prog_row.addWidget(self.label_porcentagem)
        status_layout.addLayout(prog_row)

        layout_outer.addWidget(card_status)
        layout_outer.addStretch()

        self.stacked_widget.addWidget(main_container)

    # ---------------------------------------------------------
    # Métodos de Ação da Tela Principal
    # ---------------------------------------------------------
    def selecionar_arquivo_excel(self):
        global caminho_arquivo, nome_arquivo, abas

        file_path, _ = QFileDialog.getOpenFileName(
            self,
            "Selecione um arquivo Excel",
            "",
            "Arquivos Excel (*.xlsx *.xls)"
        )
        if not file_path:
            return

        caminho_arquivo = file_path
        nome_arquivo = os.path.basename(caminho_arquivo)

        self.lista_box.clear()
        self.lista_box.addItem(nome_arquivo)

        abas = pd.ExcelFile(caminho_arquivo)

    def atualizar_box_tabelas(self):
        if not insp:
            return
        schema_atual = self.schema_box.currentText()
        if not schema_atual:
            return

        threading.Thread(
            target=self._fetch_tables,
            args=(schema_atual,),
            daemon=True
        ).start()

    def _fetch_tables(self, schema_name):
        try:
            todas_tabelas = insp.get_table_names(schema=schema_name)
            signals.tables_loaded.emit(todas_tabelas)
        except Exception:
            signals.tables_loaded.emit([])

    def on_tables_loaded(self, me_tables):
        self.tabela_box.clear()
        self.tabela_box.addItems(me_tables)

    def threading_processar_excel(self):
        if not caminho_arquivo:
            self.mensagem.setText("Por favor, selecione um arquivo Excel primeiro.")
            self.mensagem.setStyleSheet("color: #f87171;")
            return

        if not self.tabela_box.currentText():
            self.mensagem.setText("Por favor, selecione uma tabela de destino.")
            self.mensagem.setStyleSheet("color: #f87171;")
            return

        self.botao_abrir_excel.setEnabled(False)
        self.botao_escolher_excel.setEnabled(False)

        parar_barra_progresso.clear()
        global ponteiro_01
        ponteiro_01 = False

        tabela_sel = self.tabela_box.currentText()
        schema_sel = self.schema_box.currentText()

        threading.Thread(
            target=processar_excel,
            args=(tabela_sel, schema_sel),
            daemon=True
        ).start()

        threading.Thread(
            target=atualizar_porcentagem_progresso,
            daemon=True
        ).start()

    # Handlers dos Sinais Qt
    def on_update_progress(self, val, text):
        self.progress.setValue(val)
        self.label_porcentagem.setText(text)

    def on_update_message(self, text, color_code):
        self.mensagem.setText(text)
        color_map = {
            "blue": "#60a5fa",
            "green": "#4ade80",
            "red": "#f87171",
            "purple": "#c084fc"
        }
        hex_color = color_map.get(color_code, "#e2e8f0")
        self.mensagem.setStyleSheet(f"color: {hex_color};")

    def on_update_quantity(self, text):
        self.mensagem_quantidade.setText(text)

    def on_set_buttons_enabled(self, enabled):
        self.botao_abrir_excel.setEnabled(enabled)
        self.botao_escolher_excel.setEnabled(enabled)


# ---------------------------------------------------------
# Lógica de Processamento de Excel & Banco (Intacta)
# ---------------------------------------------------------

def atualizar_porcentagem_progresso():
    tempo_por_linha = tempo_medio_por_linha * 100000
    inicio = time.time()
    signals.update_progress.emit(0, "0%")

    while not parar_barra_progresso.is_set():
        tempo_decorrido = time.time() - inicio
        progresso_barra = min(int((tempo_decorrido / tempo_por_linha) * 100), 99)
        signals.update_progress.emit(progresso_barra, f"{progresso_barra}%")
        time.sleep(0.1)
        if ponteiro_01:
            break

    signals.update_progress.emit(100, "100%")


def processar_excel(tabela_leitura, schema_leitura):
    global ponteiro_01

    ano_tabela_box = re.search(r'\d+', tabela_leitura)
    if not ano_tabela_box:
        signals.update_message.emit("Nenhum ano numérico encontrado no nome da tabela selecionada.", "red")
        parar_barra_progresso.set()
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
        signals.update_message.emit(f"Processando o arquivo Excel... {nome_arquivo}", "blue")

        df = pd.concat(
            [pd.read_excel(caminho_arquivo, sheet_name=aba, engine='calamine', dtype=colunasarrumadas)
             for aba in abas.sheet_names],
            ignore_index=True
        )

        ponteiro_01 = True

        df = df.fillna('')
        df.rename(columns=mapeamento_colunas, inplace=True)

        df['Hora_Leitura'] = pd.to_datetime(df['Hora_Leitura'], format='%H:%M:%S').dt.time
        df['Intervalo_leitura'] = pd.to_datetime(df['Intervalo_leitura'], format='%H:%M:%S').dt.time
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
                raise SystemExit(f"O mês da tabela selecionada {tabela_leitura} não corresponde ao mês do Excel selecionado {nome_arquivo}. Por favor, selecione a tabela correta.")

        signals.update_message.emit("Arquivo do Excel processado com sucesso!", "green")

    except SystemExit as e:
        signals.update_message.emit(f"ERRO: {e}", "red")
        parar_barra_progresso.set()
        signals.set_buttons_enabled.emit(True)
        return

    except Exception as e:
        signals.update_message.emit(f"Ocorreu um erro ao abrir o Excel: {e}", "red")
        parar_barra_progresso.set()
        signals.set_buttons_enabled.emit(True)
        return

    cursor = None
    conn = None
    try:
        signals.update_message.emit("Conectando ao banco de dados...", "blue")

        with psycopg2.connect(
            dbname=db_name,
            user=login_usuario,
            password=login_senha,
            host=db_host,
            port=5432
        ) as conn:
            conn.set_client_encoding('UTF8')

            with conn.cursor() as cursor:
                cursor.execute("SET datestyle TO 'ISO, DMY';")

                query_dias = sql.SQL(
                    "DELETE FROM {schema_leitura}.{tabela_leitura} WHERE EXTRACT(DAY FROM data_atual) IN ({placeholders})"
                ).format(
                    tabela_leitura=sql.Identifier(tabela_leitura),
                    schema_leitura=sql.Identifier(schema_leitura),
                    placeholders=sql.SQL(',').join(sql.Placeholder() * len(dias))
                )

                cursor.execute(query_dias, dias)
                conn.commit()

                buffer = StringIO()
                df.to_csv(buffer, index=False, header=False, encoding='utf-8')
                buffer.seek(0)

                signals.update_quantity.emit(f"Quantidade de registros: {len(df)}")

                query_copy = f"""
                COPY {schema_leitura}.{tabela_leitura}
                ({', '.join(df.columns)})
                FROM STDIN WITH (FORMAT CSV, NULL '', ENCODING 'UTF8')
                """

                cursor.copy_expert(query_copy, buffer)
                conn.commit()

                signals.update_message.emit("Banco de dados atualizado com sucesso!", "green")

    except Exception as e:
        signals.update_message.emit(f"Ocorreu um erro ao entrar no banco de dados: {e}", "red")
        parar_barra_progresso.set()
        signals.set_buttons_enabled.emit(True)
        return

    finally:
        parar_barra_progresso.set()
        signals.set_buttons_enabled.emit(True)


# ---------------------------------------------------------
# Ponto de Entrada da Aplicação
# ---------------------------------------------------------

def main():
    app = QApplication(sys.argv)
    app.setApplicationName("Lumi")

    if os.path.exists(icon_path):
        app.setWindowIcon(QIcon(str(icon_path)))

    window = LumiWindow()
    window.show()

    sys.exit(app.exec())


if __name__ == "__main__":
    main()