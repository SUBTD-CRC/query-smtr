# Query SMTR - ETL de Atributos

Sistema de ETL (Extract, Transform, Load) para extração e carregamento de dados de chamados da SMTR (Secretaria Municipal de Transporte) para Google Sheets com suporte a carga incremental e completa.

## 📋 Tabela de Conteúdos

- [Sobre](#sobre)
- [Funcionalidades](#funcionalidades)
- [Pré-requisitos](#pré-requisitos)
- [Instalação](#instalação)
- [Configuração](#configuração)
- [Como Usar](#como-usar)
- [Estrutura do Projeto](#estrutura-do-projeto)
- [Logs](#logs)
- [Troubleshooting](#troubleshooting)

## 📌 Sobre

Este projeto realiza a extração automática de dados de chamados do banco de dados SQL Server da PCRJ, aplicando filtros específicos de unidades organizacionais e capturando atributos como Consórcio, Linha de Ônibus e Linha. Os dados são então carregados em uma planilha Google Sheets com suporte a sincronização incremental.

## ✨ Funcionalidades

- **Carga Incremental**: Executa a sincronização apenas dos novos registros desde a última execução bem-sucedida
- **Carga Completa**: Possibilidade de realizar carga completa dos dados, substituindo todos os registros existentes
- **Google Sheets Integration**: Integração nativa com Google Sheets usando credenciais de serviço
- **Logging Detalhado**: Sistema de logging completo para rastreamento de execuções e diagnóstico de erros
- **Tratamento de Erros**: Implementação robusta de tratamento de exceções com registros detalhados
- **Registro de Execução**: Manutenção automática de log da última execução bem-sucedida na aba "Log"

## 🔧 Pré-requisitos

- **Python 3.8+**
- **SQL Server 2016+**
- **Conta Google com acesso a Google Sheets API**
- **ODBC Driver 17 for SQL Server** instalado
- **Dependências Python**:
  - pandas
  - pyodbc
  - gspread
  - gspread-dataframe
  - google-auth-oauthlib
  - google-auth-httplib2
  - python-dotenv

## 📦 Instalação

1. **Clone o repositório**
   ```bash
   git clone <seu-repositório>
   cd query-smtr
   ```

2. **Crie um ambiente virtual** (opcional, mas recomendado)
   ```bash
   python -m venv venv
   venv\Scripts\activate
   ```

3. **Instale as dependências**
   ```bash
   pip install pandas pyodbc gspread gspread-dataframe google-auth-oauthlib google-auth-httplib2 python-dotenv
   ```

## ⚙️ Configuração

### 1. Variáveis de Ambiente

Crie um arquivo `.env` na raiz do projeto com as seguintes variáveis:

```env
DB_USER=seu_usuario_sql_server
DB_PASS=sua_senha_sql_server
spreadsheet_id=id_google_sheet
```

### 2. Credenciais do Google

1. Acesse o [Google Cloud Console](https://console.cloud.google.com/)
2. Crie um novo projeto (ou use um existente)
3. Ative a **Google Sheets API**
4. Crie uma **Conta de Serviço** (Service Account)
5. Gere uma chave privada em formato JSON
6. Salve o arquivo JSON na pasta `data/` com o nome `dev-airlock-470113-c4-ce935f97b934.json`
7. Compartilhe a planilha do Google Sheets com o email da conta de serviço

### 3. Configurações do Script

Dentro do arquivo `etl.py`, você pode ajustar:

- **`spreadsheet_id`**: ID da planilha Google Sheets (linha 47)
- **`sheet_name`**: Nome da aba onde os dados serão salvos (padrão: "Base")
- **`data_inicio`**: Data inicial para carga completa (padrão: "2021-01-01")
- **`data_filtro`**: Filtro de data usado na query SQL (preenchido automaticamente)
- **IDs de unidades organizacionais**: Lista de IDs no WHERE da query (linhas 81-119)

## 🚀 Como Usar

### Execução Manual

```bash
python etl.py
```

### Execução Agendada (Windows Task Scheduler)

1. Abra **Task Scheduler**
2. Crie uma nova tarefa
3. Configure o gatilho (frequência desejada)
4. Configure a ação:
   - Programa: `C:\Users\<usuario>\AppData\Local\Microsoft\WindowsApps\python3.12.exe`
   - Argumentos: `etl.py`
   - Iniciar em: `C:\caminho\para\query-smtr`

## 📁 Estrutura do Projeto

```
query-smtr/
├── etl.py                                          # Script principal de ETL
├── README.md                                       # Este arquivo
├── .env                                            # Variáveis de ambiente (não incluído no git)
├── .gitignore                                      # Arquivo de exceções do Git
├── execucao_query.log                              # Arquivo de log das execuções
└── data/
    └── service-account-key.json                    # Credenciais do Google (não incluído no git)
```

## 📝 Logs

### Localização do Log

O arquivo de log é salvo automaticamente como `execucao_query.log` no diretório raiz do projeto.

### Nivelamento do Log

- **INFO**: Informações gerais de execução
- **WARNING**: Avisos de execução (por exemplo, dados existentes não encontrados)
- **ERROR**: Erros críticos de execução

### Exemplo de Saída de Log

```
2026-03-05 10:30:15 - INFO - Iniciando a execução do script.
2026-03-05 10:30:16 - INFO - Conectando ao SQL Server...
2026-03-05 10:30:17 - INFO - Últim execução encontrada: 2026-03-04 10:30:15
2026-03-05 10:30:18 - INFO - Modo de execução: INCREMENTAL. Filtro de data: 2026-03-04 10:30:15.000
2026-03-05 10:30:25 - INFO - Dados extraídos com sucesso. Total de linhas: 150
2026-03-05 10:30:30 - INFO - Dados mesclados. Total após merge: 2500 linhas
2026-03-05 10:30:45 - INFO - Enviando dados para a aba 'Base'...
2026-03-05 10:30:50 - INFO - Processo concluído com sucesso!
```

## 🐛 Troubleshooting

### Erro: "Arquivo de credenciais não encontrado"
**Solução**: Certifique-se de que o arquivo JSON das credenciais está no caminho correto: `data/dev-airlock-470113-c4-ce935f97b934.json`

### Erro: "Falha na conexão com SQL Server"
**Solução**: 
- Verifique se o ODBC Driver 17 está instalado
- Confirme se as credenciais no arquivo `.env` estão corretas
- Verifique se o servidor SQL está acessível na rede

### Erro: "gspread.exceptions.APIError"
**Solução**:
- Verifique se a planilha foi compartilhada com o email da conta de serviço
- Confirme se a Google Sheets API está ativada no Google Cloud Console

### Nenhum dado extraído
**Solução**:
- Verifique se há dados nos registros das unidades organizacionais especificadas
- Revise os IDs das unidades organizacionais na query SQL
- Consulte o arquivo de log para mais detalhes

### Script executado mas dados não aparecem na planilha
**Solução**:
- Confirme se o `spreadsheet_id` está correto
- Verifique se a aba "Base" existe na planilha (será criada automaticamente se não existir)
- Revise as permissões de compartilhamento da planilha

## 📊 Campos Extraídos

A query atual extrai os seguintes campos para cada chamado:

| Campo | Descrição |
|-------|-----------|
| Consórcio | Consórcio de transportes associado ao Chamado |
| Linha de Ônibus | Identificação da linha de ônibus |
| Linha | Número ou identificação da linha |
| id_chamado | ID único do chamado |
| dt_inicio | Data e hora de início do atendimento |
| no_unidade_organizacional | Nome da unidade organizacional responsável |
