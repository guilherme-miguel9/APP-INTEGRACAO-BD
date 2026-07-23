# App Integração Banco de Dados

Aplicativo desktop desenvolvido em Python para automatizar a leitura, tratamento e inserção de dados de planilhas Excel em um banco de dados PostgreSQL.

## Visão Geral

Este projeto oferece uma interface gráfica (GUI) intuitiva que permite ao usuário conectar-se a um banco de dados PostgreSQL, selecionar arquivos Excel (`.xlsx` ou `.xls`) e importar os dados em massa para o banco. O sistema realiza limpeza, formatação de dados e utiliza a função `COPY` nativa do PostgreSQL para garantir inserções com alta performance.

## Funcionalidades

*   **Autenticação Segura:** Tela de login para credenciais do banco de dados, protegendo o acesso.
*   **Interface Gráfica Amigável:** Construída com `tkinter` e `ttkbootstrap`, guiando o usuário no processo de importação.
*   **Tratamento de Dados:** Mapeamento automático de colunas, conversão de formatos (datas, horas e números) e tratamento de valores nulos utilizando a biblioteca `pandas`.
*   **Controle de Duplicidade:** O script apaga os registros referentes aos mesmos dias contidos na planilha na tabela de destino antes de realizar a inserção, evitando duplicações.
*   **Alta Performance:** Inserção otimizada de grandes volumes de dados convertendo DataFrames para um buffer CSV em memória, utilizando o comando `COPY` via `psycopg2`.
*   **Feedback em Tempo Real:** Barra de progresso e informações de quantidade de registros processados e inseridos.
*   **Processamento Assíncrono:** Uso de `threading` para manter a interface responsiva durante a importação e tratamento de dados.

## Tecnologias Utilizadas

*   **Linguagem:** Python 3
*   **Banco de Dados:** PostgreSQL
*   **Interface Gráfica (GUI):** Tkinter, ttkbootstrap
*   **Manipulação de Dados:** Pandas, openpyxl
*   **Conexão DB:** psycopg2, SQLAlchemy

## Estrutura do Projeto

*   `Banco de Dados/SCRIPTS/`: Contém os scripts principais da aplicação (ex: `ORM_BD_CONEXAO_OSB.py` e `ORM_BD_CONEXAO_OSP.py`), que possuem a lógica da interface e integração.
*   `config.ini`: Arquivo de configuração onde são definidos os parâmetros do banco de dados, como `host` e nome do banco (`dbname`). O acesso sensível, como senha, é realizado na própria interface do sistema.
*   Arquivos `.sql`: Scripts complementares utilizados para consultas ou modelagem dos dados (`LEITURA TABELA.sql` e `LEITURA_SQL.sql`).

## Como Utilizar

1.  Configure o arquivo `config.ini` com as informações do seu `host` e `dbname`. (O usuário e senha devem ser inseridos na interface no momento do uso).
2.  Execute um dos scripts principais localizados na pasta `Banco de Dados/SCRIPTS`.
3.  Preencha o formulário de login com suas credenciais do banco de dados.
4.  Após o login, selecione o arquivo Excel desejado clicando no botão para carregar.
5.  Defina o Schema e a Tabela de destino nas opções da tela.
6.  Clique no botão correspondente para iniciar o processamento e acompanhe a barra de progresso até a conclusão.

## Autor

Desenvolvido por: Guilherme Miguel