import pandas as pd  
from auth import get_credentials
from googleapiclient.discovery import build
import sqlite3
import logging
from config import LOG_FILE_NAME

logging.basicConfig(
    filename=LOG_FILE_NAME,
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

async def get_sheet_data(spreadsheet_id, range_name):
    """Получает данные из Google Sheets и возвращает их в виде pandas DataFrame"""
    creds = await get_credentials()
    service = build('sheets', 'v4', credentials=creds)

    # Получение данных из Google Sheets
    try:
        result = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id,
                                                    range=range_name).execute()
        values = result.get('values', [])
    except Exception as e:
        logging.error(f"Ошибка при получении данных из Google Sheets: {e}")
        return None

    # Создание DataFrame из полученных данных
    df = pd.DataFrame(values[1:], columns=values[0])

    return df


async def save_to_database(df, db_name, product_data_table='product_data_ozon1', primary_key_cols=None):
    """Записывает данные из DataFrame в таблицу базы данных, обновляя и удаляя существующие записи"""
    try:
        conn = sqlite3.connect(db_name)
        c = conn.cursor()

        # Создаем таблицу product_data_table, если она не существует
        columns = ", ".join([f'"{col}"' for col in df.columns])
        c.execute(f"CREATE TABLE IF NOT EXISTS '{product_data_table}' ({columns})")

        # Определяем столбцы первичного ключа, если они не указаны
        if primary_key_cols is None:
            primary_key_cols = [df.columns[0]]  # Используем первый столбец в качестве первичного ключа

        # Получаем существующие записи из таблицы
        c.execute(f"SELECT {', '.join([f'"{col}"' for col in primary_key_cols])} FROM '{product_data_table}'")
        existing_keys = [row for row in c.fetchall()]

        # Записываем данные из DataFrame в таблицу, обновляя и удаляя существующие записи
        for _, row in df.iterrows():
            key_values = [str(row[col]) for col in primary_key_cols]
            key_placeholders = " AND ".join([f'"{col}"=?' for col in primary_key_cols])
            placeholders = ", ".join(["?"] * len(row))
            values = [str(v) for v in row.tolist()]

            if tuple(key_values) in existing_keys:
                update_values = values + key_values
                c.execute(f"UPDATE '{product_data_table}' SET {', '.join([f'"{col}"=?' for col in df.columns])} WHERE {key_placeholders}", update_values)
                logging.info(f"Обновлена строка в таблице '{product_data_table}': {', '.join(values)}")
                existing_keys.remove(tuple(key_values))
            else:
                c.execute(f"INSERT INTO '{product_data_table}' VALUES ({placeholders})", values)
                logging.info(f"Добавлена новая строка в таблицу '{product_data_table}': {', '.join(values)}")

        # Удаляем записи, которые отсутствуют в DataFrame
        for key in existing_keys:
            key_placeholders = " AND ".join([f'"{col}"=?' for col in primary_key_cols])
            c.execute(f"DELETE FROM '{product_data_table}' WHERE {key_placeholders}", key)
            logging.info(f"Удалена строка из таблицы '{product_data_table}': {', '.join(map(str, key))}")

        conn.commit()
        logging.info(f"Данные успешно сохранены в таблицу '{product_data_table}'")
    except sqlite3.Error as e:
        logging.error(f"Ошибка при работе с базой данных: {e}")
    finally:
        conn.close()