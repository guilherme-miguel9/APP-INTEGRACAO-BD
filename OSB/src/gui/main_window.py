import os
from PySide6.QtWidgets import QMainWindow, QWidget, QVBoxLayout, QStackedWidget
from PySide6.QtGui import QIcon

from OSB.src.config import icon_path
from OSB.src.styles import QSS_STYLE
from OSB.src.signals import signals
import OSB.src.database as db
from OSB.src.gui.login_screen import LoginScreen
from OSB.src.gui.main_screen import MainScreen

class LumiWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Lumi")
        self.resize(850, 750)

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
        self.login_screen = LoginScreen()
        self.main_screen = MainScreen()

        self.stacked_widget.addWidget(self.login_screen)
        self.stacked_widget.addWidget(self.main_screen)

        # Conectar Sinais Qt
        signals.login_result.connect(self.on_login_result)
        signals.update_progress.connect(self.on_update_progress)
        signals.update_message.connect(self.on_update_message)
        signals.update_quantity.connect(self.on_update_quantity)
        signals.update_metrics.connect(self.on_update_metrics)
        signals.set_buttons_enabled.connect(self.on_set_buttons_enabled)
        signals.tables_loaded.connect(self.on_tables_loaded)

    def on_login_result(self, success, message):
        if success:
            schemas = db.insp.get_schema_names()
            self.main_screen.schema_box.blockSignals(True)
            self.main_screen.schema_box.clear()
            self.main_screen.schema_box.addItems(schemas)
            self.main_screen.schema_box.blockSignals(False)

            if schemas:
                self.main_screen.atualizar_box_tabelas()

            self.stacked_widget.setCurrentIndex(1)
        else:
            self.login_screen.set_error_message(message)

    def on_update_progress(self, val, text):
        self.main_screen.progress.setValue(val)
        self.main_screen.label_porcentagem.setText(text)

    def on_update_message(self, text, color_code):
        self.main_screen.mensagem.setText(text)
        color_map = {
            "blue": "#60a5fa",
            "green": "#4ade80",
            "red": "#f87171",
            "purple": "#c084fc"
        }
        hex_color = color_map.get(color_code, "#e2e8f0")
        self.main_screen.mensagem.setStyleSheet(f"color: {hex_color};")

    def on_update_quantity(self, text):
        self.main_screen.mensagem_quantidade.setText(text)

    def on_update_metrics(self, tempo_str, velocidade_str):
        self.main_screen.mensagem_tempo.setText(f"Tempo: {tempo_str}")
        self.main_screen.mensagem_velocidade.setText(f"Velocidade: {velocidade_str}")

    def on_set_buttons_enabled(self, enabled):
        self.main_screen.botao_abrir_excel.setEnabled(enabled)
        self.main_screen.botao_escolher_pasta.setEnabled(enabled)

    def on_tables_loaded(self, me_tables):
        self.main_screen.tabela_box.clear()
        self.main_screen.tabela_box.addItems(me_tables)
