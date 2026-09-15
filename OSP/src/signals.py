from PySide6.QtCore import QObject, Signal

class AppSignals(QObject):
    update_progress = Signal(int, str)
    update_message = Signal(str, str)
    update_quantity = Signal(str)
    update_metrics = Signal(str, str)  # (tempo_formatado, velocidade_mbs)
    set_buttons_enabled = Signal(bool)
    login_result = Signal(bool, str)
    schemas_loaded = Signal(list)
    tables_loaded = Signal(list)

signals = AppSignals()
