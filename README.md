# Lumi - App Integração Banco de Dados

Aplicativo desktop desenvolvido em Python com **PySide6** para automatizar a leitura, tratamento e inserção de dados de planilhas Excel em um banco de dados PostgreSQL.

## Visão Geral

O **Lumi** oferece uma interface gráfica (GUI) moderna, minimalista e intuitiva que permite ao usuário conectar-se a um banco de dados PostgreSQL, selecionar arquivos Excel (`.xlsx` ou `.xls`) e importar os dados em massa. O sistema realiza limpeza, formatação de dados e utiliza o comando nativo `COPY` do PostgreSQL via `psycopg2` para garantir inserções de altíssima performance.

## Funcionalidades

*   **Autenticação Segura:** Tela de login para credenciais do banco de dados (usuário e senha informados em tempo de execução).
*   **Interface Gráfica Moderna (PySide6):** Design minimalista escuro (*Dark Mode*), com cantos arredondados e sem poluição visual.
*   **Tratamento Automático de Dados:** Mapeamento de colunas, conversão de formatos (datas, horas e números) e tratamento de valores nulos utilizando `pandas` e `calamine`.
*   **Controle de Duplicidade:** O script apaga os registros referentes aos mesmos dias contidos na planilha na tabela de destino antes de realizar a nova inserção, evitando registros duplicados.
*   **Alta Performance:** Inserção otimizada de grandes volumes de dados via buffer CSV em memória com `COPY` do `psycopg2`.
*   **Feedback em Tempo Real:** Barra de progresso e contadores de registros atualizados em thread separada (Thread-Safe via `PySide6.QtCore.Signal`).

## Tecnologias Utilizadas

*   **Linguagem:** Python 3
*   **Interface Gráfica (GUI):** PySide6 (Qt 6)
*   **Banco de Dados:** PostgreSQL
*   **Manipulação de Dados:** Pandas, python-calamine, openpyxl
*   **Conexão DB:** psycopg2, SQLAlchemy
*   **Empacotamento:** PyInstaller

## Configuração Obrigatória (`config.ini`)

> [!IMPORTANT]
> O arquivo `config.ini` **não é enviado ao repositório Git** por questões de segurança (está no `.gitignore`). É **obrigatório** criá-lo na raiz do projeto antes de executar o script ou gerar o executável.

Crie um arquivo chamado `config.ini` na raiz do projeto (`/APP-INTEGRACAO-BD/config.ini`) com a seguinte estrutura, alterando as variáveis `host` e `dbname` para os valores correspondentes ao seu ambiente de desenvolvimento/produção:

```ini
[database]
host = seu_host_aqui
dbname = seu_banco_aqui
```

*Obs: O usuário e a senha do banco de dados não ficam no `config.ini`; eles são informados com segurança diretamente na tela de login da aplicação.*

## Como Executar

1. Crie e configure o arquivo `config.ini` na raiz do projeto conforme explicado acima.
2. Instale as dependências necessárias:
   ```bash
   pip install pandas psycopg2-binary sqlalchemy PySide6 python-calamine openpyxl
   ```
3. Execute a aplicação:
   ```bash
   python3 "Banco de Dados/SCRIPTS/ORM_BD_CONEXAO_OSB.py"
   ```

## Como Gerar o Executável (PyInstaller)

Para compilar a aplicação **Lumi** em um executável autônomo com ícone e o `config.ini` embutido:

1. Certifique-se de que o PyInstaller está instalado:
   ```bash
   pip install pyinstaller
   ```
2. Certifique-se de que o arquivo `config.ini` foi criado e configurado com o `host` e `dbname` corretos do seu ambiente.
3. Execute o comando de compilação na raiz do projeto:

   ```bash
   python3 -m PyInstaller -y --noconsole --onefile \
     --icon="Banco de Dados/icones/icone_aplicativo.ico" \
     --name="Lumi" \
     --collect-all python_calamine \
     --add-data="config.ini:." \
     --add-data="Banco de Dados/icones:Banco de Dados/icones" \
     "Banco de Dados/SCRIPTS/ORM_BD_CONEXAO_OSB.py"
   ```

4. O executável será gerado na pasta `dist/` com o nome **Lumi** (ou **Lumi.app** no macOS).

## Autor

Desenvolvido por: Guilherme Miguel