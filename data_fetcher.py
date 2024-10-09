import pandas as pd
from auth import get_credentials
from googleapiclient.discovery import build
import sqlite3
import traceback
from logger import logger  # Импорт логгера

async def get_sheet_data(spreadsheet_id, range_name):
    """Получает данные из Google Sheets и возвращает их в виде pandas DataFrame"""
    creds = await get_credentials()
    service = build('sheets', 'v4', credentials=creds)

    try:
        result = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id,
                                                    range=range_name).execute()
        values = result.get('values', [])
    except Exception as e:
        logger.error("google_sheets_error", error=str(e))
        return None

    df = pd.DataFrame(values[1:], columns=values[0])
    return df

async def save_to_database(df, db_name, product_data_table='product_data_ozon1', primary_key_cols=None):
    """Записывает данные из DataFrame в таблицу базы данных, обновляя и удаляя существующие записи"""
    conn = None
    try:
        logger.info("database_update_start", table=product_data_table, dataframe_size=len(df))

        conn = sqlite3.connect(db_name)
        c = conn.cursor()

        logger.info("creating_table_if_not_exists")
        columns = ", ".join([f'"{col}"' for col in df.columns])
        c.execute(f"CREATE TABLE IF NOT EXISTS '{product_data_table}' ({columns})")

        if primary_key_cols is None:
            primary_key_cols = [df.columns[0]]
        logger.info("primary_keys", keys=primary_key_cols)

        logger.info("fetching_existing_records")
        c.execute(f"SELECT * FROM '{product_data_table}'")
        existing_data = {tuple(row[:len(primary_key_cols)]): row for row in c.fetchall()}
        logger.info("existing_records_count", count=len(existing_data))

        updates = inserts = unchanged = deleted = 0

        logger.info("processing_records_start")
        for index, row in df.iterrows():
            key_values = tuple(str(row[col]) for col in primary_key_cols)
            values = tuple(str(v) for v in row.tolist())

            if key_values in existing_data:
                if values != existing_data[key_values]:
                    placeholders = ", ".join([f'"{col}"=?' for col in df.columns])
                    c.execute(
                        f"UPDATE '{product_data_table}' SET {placeholders} WHERE {' AND '.join([f'"{col}"=?' for col in primary_key_cols])}",
                        values + key_values)
                    updates += 1
                else:
                    unchanged += 1
                del existing_data[key_values]
            else:
                placeholders = ", ".join(["?"] * len(values))
                c.execute(f"INSERT INTO '{product_data_table}' VALUES ({placeholders})", values)
                inserts += 1

            if index % 1000 == 0:
                logger.info("processing_progress", records_processed=index + 1)

        logger.info("deleting_missing_records")
        for key in existing_data.keys():
            c.execute(
                f"DELETE FROM '{product_data_table}' WHERE {' AND '.join([f'"{col}"=?' for col in primary_key_cols])}",
                key)
            deleted += 1

        conn.commit()
        logger.info("database_changes_committed")

        total_records = max(len(df) + deleted, 1)
        change_percentage = (updates + inserts + deleted) / total_records * 100

        logger.info("update_complete",
                    inserted=inserts,
                    updated=updates,
                    unchanged=unchanged,
                    deleted=deleted,
                    total_records=total_records,
                    change_percentage=f"{change_percentage:.2f}%")

    except Exception as e:
        logger.error("database_update_error",
                     error=str(e),
                     traceback=traceback.format_exc())
    finally:
        if conn:
            conn.close()
            logger.info("database_connection_closed")


