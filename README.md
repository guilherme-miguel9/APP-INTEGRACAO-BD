# Lumi - App Integração Banco de Dados (OSB e OSP)

Aplicativo desktop desenvolvido em Python com **PySide6** para automatizar a leitura, tratamento e inserção de dados de planilhas Excel em um banco de dados PostgreSQL.

## Visão Geral

O repositório é composto por dois módulos de integração:
* **OSB (Lumi)**: Oferece uma interface gráfica (GUI) moderna que permite selecionar uma **pasta inteira com planilhas Excel** (`.xlsx`, `.xlsm`, `.xls`), ler individualmente cada arquivo atribuindo a coluna `'Nome da Origem.1'` com os 10 primeiros dígitos do nome do arquivo correspondente, realizar a limpeza/formatação com `pandas` e `calamine`, e fazer a carga em lote no PostgreSQL via comando nativo `COPY`.
* **OSP**: Módulo complementar mantido em sua própria pasta isolada (`OSP/`).

---

## Estrutura do Projeto

```
APP-INTEGRACAO-BD/
├── config.ini                       # Configuração global de banco de dados
├── Lumi.spec                        # Especificação de compilação PyInstaller
├── README.md                        # Documentação do projeto
├── OSB/                             # Projeto OSB (Lumi) modular
│   ├── main.py                      # Ponto de entrada do aplicativo OSB
│   ├── assets/                      # Recursos visuais (ícones)
│   └── src/                         # Módulos Python (gui, excel, database, etc.)
└── OSP/                             # Projeto OSP isolado
    └── ORM_BD_CONEXAO_OSP.py
```

---

## Funcionalidades Principais (OSB)

*   **Seleção por Pasta de Planilhas:** Seleção de diretório completo com varredura automática de arquivos `.xlsx`, `.xlsm` e `.xls`.
*   **Identificação de Origem (`Nome da Origem.1`):** Extração automática dos 10 primeiros caracteres do nome de cada arquivo lido e gravação direta na coluna de origem antes da consolidação no DataFrame.
*   **Autenticação Segura:** Tela de login para credenciais do banco de dados (usuário e senha informados em tempo de execução).
*   **Interface Gráfica Moderna (PySide6):** Design minimalista escuro (*Dark Mode*), com cantos arredondados e feedback visual em tempo real.
*   **Alta Performance (`calamine` + `COPY`):** Leitura de planilhas otimizada em Rust com `python-calamine` e inserção em massa via buffer CSV em memória com `COPY` do `psycopg2`.
*   **Controle de Duplicidade:** O script apaga os registros referentes aos mesmos dias contidos nas planilhas na tabela de destino antes de realizar a nova inserção.

---

## Configuração Obrigatória (`config.ini`)

> [!IMPORTANT]
> O arquivo `config.ini` **não é enviado ao repositório Git** por questões de segurança (está no `.gitignore`). É **obrigatório** criá-lo na raiz do projeto (`/APP-INTEGRACAO-BD/config.ini`):

```ini
[database]
host = seu_host_aqui
dbname = seu_banco_aqui
```

---

## Como Executar

1. Crie e configure o arquivo `config.ini` na raiz do projeto.
2. Instale as dependências necessárias:
   ```bash
   pip install pandas psycopg2-binary sqlalchemy PySide6 python-calamine openpyxl
   ```
3. Execute o aplicativo OSB:
   ```bash
   python3 OSB/main.py
   ```

---

## Como Gerar o Executável (PyInstaller)

Para compilar a aplicação **Lumi (OSB)** em um executável autônomo:

```bash
python3 -m PyInstaller Lumi.spec
```

O executável será gerado na pasta `dist/` com o nome **Lumi** (ou **Lumi.app** no macOS).

---

## Autor

Desenvolvido por: Guilherme Miguel