import sys
import os
from pathlib import Path

# Adiciona a raiz do repositório ao sys.path para garantir importações relativas
root_path = Path(__file__).resolve().parent.parent
if str(root_path) not in sys.path:
    sys.path.insert(0, str(root_path))

from PySide6.QtWidgets import QApplication
from PySide6.QtGui import QIcon

from OSB.src.config import icon_path
from OSB.src.gui.main_window import LumiWindow

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
