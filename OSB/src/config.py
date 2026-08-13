import sys
import os
import configparser
from pathlib import Path

def resource_path(relative_path: str) -> Path:
    """
    Retorna o caminho absoluto do recurso, funcionando em ambiente de dev e PyInstaller.
    """
    if getattr(sys, 'frozen', False):
        base_path = Path(sys._MEIPASS)
    else:
        # Se estamos em OSB/src/config.py, subindo 2 níveis chegamos à raiz do projeto
        base_path = Path(__file__).resolve().parents[2]
    
    file_path = base_path / relative_path
    if not file_path.exists():
        alt_path = Path(__file__).resolve().parents[1] / relative_path
        if alt_path.exists():
            return alt_path
        alt_exec = Path(sys.executable).parent / relative_path
        if alt_exec.exists():
            return alt_exec
        alt_root = Path.cwd() / relative_path
        if alt_root.exists():
            return alt_root
    return file_path

# Leitura do config.ini
ini_path = resource_path('config.ini')
config = configparser.ConfigParser()
config.read(ini_path, encoding='utf-8')

db_host = config['database']['host'] if config.has_section('database') and 'host' in config['database'] else 'localhost'
db_name = config['database']['dbname'] if config.has_section('database') and 'dbname' in config['database'] else 'postgres'

icon_path = resource_path(os.path.join('OSB', 'assets', 'icones', 'icone_aplicativo.ico'))
if not os.path.exists(icon_path):
    # Fallback para caminho de ícone legado se necessário
    icon_path = resource_path(os.path.join('Banco de Dados', 'icones', 'icone_aplicativo.ico'))
