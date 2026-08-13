import threading
from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QFrame, QLabel, QLineEdit, QPushButton
)
from PySide6.QtCore import Qt

from OSB.src.database import test_login_connection

class LoginScreen(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.init_ui()

    def init_ui(self):
        layout_outer = QVBoxLayout(self)
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
            target=test_login_connection,
            args=(usuario_digitado, senha_digitada),
            daemon=True
        ).start()

    def set_error_message(self, message):
        self.mensagem_login.setText(message)
        self.mensagem_login.setStyleSheet("color: #f87171;")
        self.botao_login.setEnabled(True)
