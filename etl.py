import pandas as pd
import pyodbc
import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv
import logging
import os
from datetime import datetime, timedelta

load_dotenv()
log_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "execucao_query.log")

logging.basicConfig(
    filename=log_path,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    encoding='utf-8'
)

def obter_data_ultima_execucao(client, spreadsheet_id):
    """Obtém a data da última execução bem-sucedida do Google Sheets"""
    try:
        log_sheet = client.open_by_key(spreadsheet_id).worksheet("Log")
        cell_value = log_sheet.cell(1, 2).value
        
        if cell_value:
            ultima_data = datetime.strptime(cell_value, '%Y-%m-%d %H:%M:%S') - timedelta(hours=1)
            logging.info(f"Última execução encontrada: {cell_value}")
            return ultima_data
    except (gspread.WorksheetNotFound, ValueError, AttributeError):
        logging.info("Nenhuma execução anterior encontrada. Fazendo carga completa.")
    
    return None

def main():
    try:
        logging.info("Iniciando a execução do script.")

        db_user = os.getenv("DB_USER")
        db_pass = os.getenv("DB_PASS")
        spreadsheet_id = os.getenv("spreadsheet_id")
        sheet_name = "Base"

        data_inicio = "2021-01-01"

        logging.info("Conectando ao SQL Server...")
        conn_str = (
            f"Driver={{ODBC Driver 17 for SQL Server}};"
            f"Server=10.6.0.35,1433;"
            f"Database=PCRJ;"
            f"UID={db_user};"
            f"PWD={db_pass};"
        )
        
        logging.info("Obtendo data da última execução...")
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds_path = 'data/service-account-key.json'
        
        if not os.path.exists(creds_path):
            raise FileNotFoundError(f"Arquivo de credenciais não encontrado: {creds_path}")

        creds = Credentials.from_service_account_file(creds_path, scopes=scope)
        client = gspread.authorize(creds)
        
        ultima_execucao = obter_data_ultima_execucao(client, spreadsheet_id)
        
        if ultima_execucao:
            data_filtro = ultima_execucao.strftime('%Y-%m-%d %H:%M:%S.000')
            modo_execucao = "INCREMENTAL"
        else:
            data_filtro = data_inicio + " 00:00:00.000"
            modo_execucao = "CARGA COMPLETA"
        
        logging.info(f"Modo de execução: {modo_execucao}. Filtro de data: {data_filtro}")
        
        conn = pyodbc.connect(conn_str)
        
        sql = f"""
        SELECT
            (SELECT ds_valor FROM tb_chamado_atributo WHERE id_atributo_fk = 518 AND id_chamado_fk = tb_chamado.id_chamado) AS 'Consórcio',			
            (SELECT ds_valor FROM tb_chamado_atributo WHERE id_atributo_fk = 533 AND id_chamado_fk = tb_chamado.id_chamado) AS 'Linha de Ônibus',	
            (SELECT ds_valor FROM tb_chamado_atributo WHERE id_atributo_fk = 230 AND id_chamado_fk = tb_chamado.id_chamado) AS 'Linha',
            tb_chamado.id_chamado,
            tb_chamado.dt_inicio,
        tb_unidade_organizacional.no_unidade_organizacional
        FROM tb_chamado
        LEFT JOIN tb_responsavel_chamado ON tb_chamado.id_responsavel_chamado_fk = tb_responsavel_chamado.id_responsavel_chamado
        LEFT JOIN tb_unidade_organizacional ON tb_responsavel_chamado.id_unidade_organizacional_fk = tb_unidade_organizacional.id_unidade_organizacional
        WHERE 1 = 1 
        AND dt_inicio >= '{data_filtro}'
        AND id_unidade_organizacional IN (
            '34',
            '43',
            '44',
            '45',
            '46',
            '47',
            '48',
            '49',
            '50',
            '51',
            '52',
            '53',
            '54',
            '55',
            '1502',
            '1503',
            '1504',
            '1505',
            '1506',
            '1507',
            '1508',
            '1509',
            '1573',
            '1602',
            '1683',
            '1684',
            '1686',
            '1692',
            '1693',
            '1694',
            '1703',
            '1704',
            '1705',
            '1706',
            '1795',
            '1796',
            '1797',
            '1798',
            '1814',
            '1815',
            '1816',
            '1857'
        )
        """

        df = pd.read_sql(sql, conn)
        logging.info(f"Dados extraídos com sucesso. Total de linhas: {len(df)}")
        
        conn.close()

        if modo_execucao == "INCREMENTAL":
            logging.info("Carregando dados existentes do Google Sheets...")
            try:
                sheet = client.open_by_key(spreadsheet_id).worksheet(sheet_name)
                df_existente = pd.DataFrame(sheet.get_all_records())
                
                if not df_existente.empty and 'id_chamado' in df_existente.columns:
                    df['id_chamado'] = df['id_chamado'].astype(str)
                    df_existente['id_chamado'] = df_existente['id_chamado'].astype(str)
                    
                    ids_novos = set(df['id_chamado'].unique())
                    df_existente = df_existente[~df_existente['id_chamado'].isin(ids_novos)]
                    
                    df = pd.concat([df_existente, df], ignore_index=True)
                    logging.info(f"Dados mesclados. Total após merge: {len(df)} linhas")
                else:
                    logging.warning("Não foi possível carregar dados existentes. Usando apenas dados novos.")
            except Exception as e:
                logging.warning(f"Erro ao carregar dados existentes. Usando apenas dados novos. Erro: {e}")
        else:
            logging.info("Modo carga completa: dados existentes serão substituídos.")
        
        sheet = client.open_by_key(spreadsheet_id).worksheet(sheet_name)

        sheet.clear()

        logging.info(f"Enviando dados para a aba '{sheet_name}'...")
        set_with_dataframe(sheet, df)

        logging.info("Processo concluído com sucesso!")

        logging.info("Atualizando aba de log...")
        try:
            log_sheet = client.open_by_key(spreadsheet_id).worksheet("Log")
        except gspread.WorksheetNotFound:
            log_sheet = client.open_by_key(spreadsheet_id).add_worksheet("Log", rows=1, cols=2)
        log_sheet.clear()
        log_sheet.update('A1:B1', [['Última execução', datetime.now().strftime('%Y-%m-%d %H:%M:%S')]])
        logging.info("Aba de log atualizada.")

    except Exception as e:
        logging.error(f"Erro durante a execução: {str(e)}", exc_info=True)
        print(f"Erro: {e}")

if __name__ == "__main__":
    main()
