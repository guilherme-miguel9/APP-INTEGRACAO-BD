# Lumi - App Integração Banco de Dados (OSB e OSP)

Aplicativo desktop desenvolvido em Python com **PySide6** para automatizar a leitura, tratamento e inserção de dados de planilhas Excel em um banco de dados PostgreSQL.

---

## Visão Geral

O repositório é composto por dois módulos de integração:
* **OSB (Lumi)**: Oferece uma interface gráfica (GUI) moderna que permite selecionar uma **pasta inteira com planilhas Excel** (`.xlsx`, `.xlsm`, `.xls`), ler individualmente cada arquivo atribuindo a coluna `'Nome da Origem.1'` com os 10 primeiros dígitos do nome do arquivo correspondente, realizar a limpeza/formatação com `pandas` e `calamine`, e fazer a carga em lote no PostgreSQL via comando nativo `COPY`.
* **OSP**: Módulo complementar mantido em sua própria pasta isolada (`OSP/`).

---

## Estrutura do Projeto

```
APP-INTEGRACAO-BD/
├── config.ini.example               # Exemplo de configuração de banco de dados para o GitHub
├── Lumi.spec                        # Especificação de compilação PyInstaller
├── README.md                        # Documentação do projeto
├── OSB/                             # Projeto OSB (Lumi) modular
│   ├── main.py                      # Ponto de entrada do aplicativo OSB
│   ├── assets/                      # Recursos visuais (ícones)
│   └── src/                         # Módulos Python (gui, excel, database, etc.)
│       ├── config.py                # Resolução de caminhos e config.ini
│       ├── database.py              # Autenticação e streaming COPY para o PostgreSQL
│       ├── excel_processor.py       # Leitura com calamine, mapeamento e colunas fixas
│       ├── signals.py               # Sinais PySide6 para UI e progresso
│       ├── styles.py                # Tema Dark e estilos QSS
│       └── gui/                     # Componentes da interface PySide6
└── OSP/                             # Projeto OSP isolado
    └── ORM_BD_CONEXAO_OSP.py
```

---

## Configuração Obrigatória (`config.ini`)

> [!IMPORTANT]
> O arquivo `config.ini` **não é enviado ao repositório Git** por questões de segurança (está no `.gitignore`). É **obrigatório** criá-lo na raiz do projeto antes de executar a aplicação.

### Como criar o arquivo de configuração:

1. Na raiz do projeto, faça uma cópia do arquivo de exemplo:
   ```bash
   cp config.ini.example config.ini
   ```

2. Abra o arquivo `config.ini` e configure o servidor e o banco de dados desejados:
   ```ini
   [database]
   host = seu_servidor_postgresql
   dbname = seu_banco_de_dados
   ```

> [!NOTE]
> **Autenticação e Credenciais:** O usuário (`user`) e a senha (`password`) do banco de dados **não ficam gravados em arquivos de configuração**. Eles são informados com total segurança diretamente na tela de login da interface gráfica (PySide6) ao iniciar a aplicação.

---

## Como Personalizar as Colunas do Excel e do Banco de Dados

Para alterar ou adaptar as colunas do Excel para a sua necessidade ou schema de banco de dados diferente, edite o arquivo **[`OSB/src/excel_processor.py`](file:///Users/guilhermemiguel/Documents/APP-INTEGRACAO-BD/OSB/src/excel_processor.py)**.

Nele existem dois dicionários principais:

### 1. `colunasarrumadas` (Tipagem de Leitura do Excel)
Define os tipos de dados forçados na leitura inicial das planilhas para evitar perda de zeros à esquerda ou erros de conversão.

```python
colunasarrumadas = {
    'Nº': str,
    'Instal': str,
    'Contrato': str,
    'Cta.contr.': str
    # Adicione ou remova colunas do seu Excel aqui
}
```

### 2. `mapeamento_colunas` (Excel -> PostgreSQL)
Mapeia o nome exato da coluna no Excel (à esquerda) para o nome da coluna correspondente na tabela do banco de dados PostgreSQL (à direita):

```python
mapeamento_colunas = {
    'Nome da Origem.1': 'Data_Atual',     # Origem gerada automaticamente dos 10 1ºs dígitos do nome do arquivo
    'Nº': 'N',
    'Nº item da ordem': 'Numero_Item_Ordem',
    'Instal': 'Instalacao',
    'NomeCliente': 'Nome_Cliente',
    'Val Fat': 'Valor_fatura'
    # Adicione novos mapeamentos no formato: 'Nome No Excel': 'nome_no_postgres'
}
```

---

## Como Executar

1. Crie e configure o arquivo `config.ini` conforme explicado acima.
2. Instale as dependências necessárias através do arquivo `requirements.txt`:
   ```bash
   pip install -r requirements.txt
   ```
3. Execute o aplicativo OSB (Lumi):
   ```bash
   python3 OSB/main.py
   ```

---

## Como Gerar o Executável (PyInstaller)

Para compilar a aplicação **Lumi (OSB)** em um executável autônomo:

```bash
python3 -m PyInstaller -y Lumi.spec
```

O executável será gerado na pasta `dist/` com o nome **Lumi** (ou **Lumi.app** no macOS).

---

## Autor

Desenvolvido por: Guilherme Miguel