import io
import time
from urllib.parse import quote_plus
from sqlalchemy import create_engine, inspect
import psycopg2
from psycopg2 import sql

from OSB.src.config import db_host, db_name
from OSB.src.signals import signals

login_usuario = None
login_senha = None
engine = None
insp = None

class ProgressBufferStream(io.TextIOBase):
    """
    Stream wrapper de texto para monitoramento em tempo real do envio via COPY para o PostgreSQL.
    """
    def __init__(self, raw_str: str, on_progress_callback=None):
        self._io = io.StringIO(raw_str)
        self.raw_bytes = raw_str.encode('utf-8')
        self.total_bytes = len(self.raw_bytes)
        self.bytes_read = 0
        self.start_time = time.time()
        self.on_progress_callback = on_progress_callback

    def read(self, size=-1):
        chunk = self._io.read(size)
        if chunk:
            chunk_bytes = len(chunk.encode('utf-8'))
            self.bytes_read += chunk_bytes
            if self.on_progress_callback:
                elapsed = max(time.time() - self.start_time, 0.001)
                self.on_progress_callback(self.bytes_read, self.total_bytes, elapsed)
        return chunk

    def readline(self, size=-1):
        line = self._io.readline(size)
        if line:
            line_bytes = len(line.encode('utf-8'))
            self.bytes_read += line_bytes
            if self.on_progress_callback:
                elapsed = max(time.time() - self.start_time, 0.001)
                self.on_progress_callback(self.bytes_read, self.total_bytes, elapsed)
        return line

def test_login_connection(usuario_digitado, senha_digitada):
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

def fetch_tables(schema_name):
    if not insp:
        signals.tables_loaded.emit([])
        return
    try:
        todas_tabelas = insp.get_table_names(schema=schema_name)
        signals.tables_loaded.emit(todas_tabelas)
    except Exception:
        signals.tables_loaded.emit([])

def inserir_dataframe_no_banco(df, dias, tabela_leitura, schema_leitura, start_time_global):
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

                if dias:
                    signals.update_message.emit("Removendo registros existentes para os dias das planilhas...", "blue")
                    query_dias = sql.SQL(
                        "DELETE FROM {schema_leitura}.{tabela_leitura} WHERE EXTRACT(DAY FROM data_atual) IN ({placeholders})"
                    ).format(
                        tabela_leitura=sql.Identifier(tabela_leitura),
                        schema_leitura=sql.Identifier(schema_leitura),
                        placeholders=sql.SQL(',').join(sql.Placeholder() * len(dias))
                    )

                    cursor.execute(query_dias, dias)
                    conn.commit()

                signals.update_message.emit("Gerando buffer de dados para envio...", "blue")
                buffer = io.StringIO()
                df.to_csv(buffer, index=False, header=False, encoding='utf-8')
                buffer_str = buffer.getvalue()
                buffer.close()

                total_bytes = len(buffer_str.encode('utf-8'))
                tamanho_mb = total_bytes / (1024 * 1024)
                signals.update_quantity.emit(f"Quantidade de registros: {len(df):,} | Tamanho: {tamanho_mb:.2f} MB")

                def on_copy_progress(bytes_read, total_b, elapsed_upload):
                    ratio = bytes_read / total_b if total_b > 0 else 1.0
                    pct = 55 + int(ratio * 45)
                    speed_mbs = (bytes_read / (1024 * 1024)) / elapsed_upload if elapsed_upload > 0 else 0.0
                    
                    elapsed_global = time.time() - start_time_global
                    minutos = int(elapsed_global // 60)
                    segundos = int(elapsed_global % 60)
                    timer_str = f"{minutos:02d}:{segundos:02d}"
                    speed_str = f"{speed_mbs:.2f} MB/s"

                    mb_enviados = bytes_read / (1024 * 1024)
                    signals.update_progress.emit(pct, f"{pct}%")
                    signals.update_message.emit(f"Enviando ao banco... ({mb_enviados:.1f}/{tamanho_mb:.1f} MB)", "blue")
                    signals.update_metrics.emit(timer_str, speed_str)

                stream = ProgressBufferStream(buffer_str, on_progress_callback=on_copy_progress)

                signals.update_message.emit("Transmitindo dados para o PostgreSQL via COPY...", "blue")

                colunas_sql = ', '.join([f'"{col}"' for col in df.columns])
                query_copy = f"""
                COPY {schema_leitura}.{tabela_leitura}
                ({colunas_sql})
                FROM STDIN WITH (FORMAT CSV, NULL '', ENCODING 'UTF8')
                """

                cursor.copy_expert(query_copy, stream)
                conn.commit()

                elapsed_final = time.time() - start_time_global
                minutos_f = int(elapsed_final // 60)
                segundos_f = int(elapsed_final % 60)
                timer_final = f"{minutos_f:02d}:{segundos_f:02d}"

                signals.update_progress.emit(100, "100%")
                signals.update_message.emit("Banco de dados atualizado com sucesso!", "green")
                signals.update_metrics.emit(timer_final, "Concluído")

    except Exception as e:
        signals.update_message.emit(f"Ocorreu um erro ao entrar no banco de dados: {e}", "red")
        return

    finally:
        signals.set_buttons_enabled.emit(True)
