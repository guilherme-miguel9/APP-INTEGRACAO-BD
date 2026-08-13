import os
import threading
from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel, QPushButton, QComboBox,
    QListWidget, QProgressBar, QFrame, QFileDialog
)
from PySide6.QtCore import Qt

from OSB.src.signals import signals
from OSB.src.database import fetch_tables
from OSB.src.excel_processor import listar_arquivos_excel, processar_pasta_excel

class MainScreen(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.caminho_pasta = ""
        self.init_ui()

    def init_ui(self):
        layout_outer = QVBoxLayout(self)
        layout_outer.setContentsMargins(40, 30, 40, 30)
        layout_outer.setSpacing(20)

        # Top Header Bar
        header_layout = QHBoxLayout()
        lbl_app = QLabel("Lumi")
        lbl_app.setObjectName("titleLabel")

        lbl_desc = QLabel("Integração Pasta Excel para PostgreSQL")
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

        lbl_section1 = QLabel("Configurações da Pasta e Banco de Dados")
        lbl_section1.setObjectName("sectionTitle")
        config_layout.addWidget(lbl_section1)

        # Botão escolher Pasta & ListBox
        excel_row = QHBoxLayout()
        self.botao_escolher_pasta = QPushButton("Selecionar Pasta de Planilhas")
        self.botao_escolher_pasta.setObjectName("secondaryButton")
        self.botao_escolher_pasta.setCursor(Qt.PointingHandCursor)
        self.botao_escolher_pasta.clicked.connect(self.selecionar_pasta_excel)

        self.lista_box = QListWidget()
        self.lista_box.setFixedHeight(70)

        excel_row.addWidget(self.botao_escolher_pasta)
        excel_row.addWidget(self.lista_box, stretch=1)
        config_layout.addLayout(excel_row)

        lbl_msg_schema = QLabel("Escolha a pasta de planilhas e selecione o Schema/Tabela de destino")
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

        # Botão Processar Pasta Excel
        self.botao_abrir_excel = QPushButton("Processar e Importar Planilhas")
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

        self.mensagem = QLabel("Aguardando seleção da pasta...")
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

        # Linha de métricas: Tempo decorrido (MM:SS) e Velocidade de Transferência (MB/s)
        metrics_row = QHBoxLayout()
        self.mensagem_tempo = QLabel("Tempo: 00:00")
        self.mensagem_tempo.setStyleSheet("color: #38bdf8; font-weight: 500;")

        self.mensagem_velocidade = QLabel("Velocidade: - MB/s")
        self.mensagem_velocidade.setStyleSheet("color: #34d399; font-weight: 500;")
        self.mensagem_velocidade.setAlignment(Qt.AlignRight | Qt.AlignVCenter)

        metrics_row.addWidget(self.mensagem_tempo)
        metrics_row.addStretch()
        metrics_row.addWidget(self.mensagem_velocidade)
        status_layout.addLayout(metrics_row)

        layout_outer.addWidget(card_status)
        layout_outer.addStretch()

    def selecionar_pasta_excel(self):
        folder_path = QFileDialog.getExistingDirectory(
            self,
            "Selecione a Pasta com Planilhas Excel (.xlsx, .xlsm, .xls)",
            ""
        )
        if not folder_path:
            return

        self.caminho_pasta = folder_path
        arquivos = listar_arquivos_excel(self.caminho_pasta)

        self.lista_box.clear()
        for arq in arquivos:
            self.lista_box.addItem(os.path.basename(arq))

        if arquivos:
            self.mensagem.setText(f"{len(arquivos)} arquivo(s) de planilha (.xlsx, .xlsm, .xls) encontrado(s) na pasta.")
            self.mensagem.setStyleSheet("color: #60a5fa;")
        else:
            self.mensagem.setText("Nenhum arquivo de planilha (.xlsx, .xlsm, .xls) encontrado na pasta selecionada.")
            self.mensagem.setStyleSheet("color: #f87171;")

    def atualizar_box_tabelas(self):
        schema_atual = self.schema_box.currentText()
        if not schema_atual:
            return

        threading.Thread(
            target=fetch_tables,
            args=(schema_atual,),
            daemon=True
        ).start()

    def threading_processar_excel(self):
        if not self.caminho_pasta:
            self.mensagem.setText("Por favor, selecione uma pasta de planilhas primeiro.")
            self.mensagem.setStyleSheet("color: #f87171;")
            return

        arquivos = listar_arquivos_excel(self.caminho_pasta)
        if not arquivos:
            self.mensagem.setText("Nenhum arquivo Excel (.xlsx, .xlsm, .xls) encontrado na pasta selecionada.")
            self.mensagem.setStyleSheet("color: #f87171;")
            return

        if not self.tabela_box.currentText():
            self.mensagem.setText("Por favor, selecione uma tabela de destino.")
            self.mensagem.setStyleSheet("color: #f87171;")
            return

        self.botao_abrir_excel.setEnabled(False)
        self.botao_escolher_pasta.setEnabled(False)

        self.progress.setValue(0)
        self.label_porcentagem.setText("0%")
        self.mensagem_tempo.setText("Tempo: 00:00")
        self.mensagem_velocidade.setText("Velocidade: 0.00 MB/s")

        tabela_sel = self.tabela_box.currentText()
        schema_sel = self.schema_box.currentText()

        threading.Thread(
            target=processar_pasta_excel,
            args=(self.caminho_pasta, tabela_sel, schema_sel),
            daemon=True
        ).start()
