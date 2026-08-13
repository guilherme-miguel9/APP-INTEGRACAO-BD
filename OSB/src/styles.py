"""
Estilo QSS Minimalista e Arredondado (Dark Theme) para o aplicativo Lumi.
"""

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
