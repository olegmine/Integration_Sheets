import asyncio
import pandas as pd
import logging
from pathlib import Path
import sqlite3
from config import LOG_FILE_NAME, SQLITE_DB_NAME
import sqlite3
from datetime import datetime

ID_COL = 'id'
PRODUCT_ID_COL = 'product_id'
PRICE_COL = 't_price'
OLD_PRICE_COL = 'price'
PRIM_COL = 'prim'

log_file = Path(LOG_FILE_NAME)
log_file.parent.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    filename=LOG_FILE_NAME,
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

async def update_price(df, id_col=ID_COL, product_id_col=PRODUCT_ID_COL, price_col=PRICE_COL, old_price_col=OLD_PRICE_COL, prim_col=PRIM_COL, sqlite_db_name=SQLITE_DB_NAME, price_change_log_table='price_change_log'):
    """Определяет необходимость обновления цен, записывает информацию об изменениях в таблицу price_change_log и обновляет цены в DataFrame"""
    df = df.iloc[1:]  # Пропускаем первую строку (заголовки)
    changes = []
    updated_df = df.copy()

    try:
        conn = sqlite3.connect(sqlite_db_name)
        c = conn.cursor()

        # Создаем таблицу для логов, если она не существует
        c.execute(f'''CREATE TABLE IF NOT EXISTS '{price_change_log_table}'  
                     (timestamp TEXT, id TEXT, product_id TEXT, old_price REAL, new_price REAL, prim TEXT)''')

        for _, row in df.iterrows():
            try:
                old_price = int(row[old_price_col])
                new_price = int(row[price_col])
            except ValueError:
                logging.warning(f"Некорректное значение цены для товара с ID {row[id_col]} и артикулом {row[product_id_col]}")
                c.execute(f"INSERT INTO '{price_change_log_table}' (timestamp, id, product_id, old_price, new_price, prim) VALUES (?, ?, ?, ?, ?, ?)",
                         (datetime.now().strftime('%Y-%m-%d %H:%M:%S'), row[id_col], row[product_id_col], 0, 0, "Ошибка формата данных"))
                continue

            if abs(new_price - old_price) / old_price > 0.5:
                updated_df.at[_, prim_col] = f"Изменение цены с {old_price} на {new_price} превышает 50%, цена не изменена"
                change_info = row.to_dict()
                change_info[prim_col] = f"Изменение цены с {old_price} на {new_price} превышает 50%, цена не изменена"
                logging.info(
                    f"Для товара с ID {row[id_col]} и артикулом {row[product_id_col]} изменение цены с {old_price} на {new_price} превышает 50%, цена не изменена")
                c.execute(f"INSERT INTO '{price_change_log_table}' (timestamp, id, product_id, old_price, new_price, prim) VALUES (?, ?, ?, ?, ?, ?)",
                         (datetime.now().strftime('%Y-%m-%d %H:%M:%S'), row[id_col], row[product_id_col], old_price, new_price, f"Изменение цены с {old_price} на {new_price} превышает 50%, цена не изменена"))
            elif new_price != old_price:
                updated_df.at[_, old_price_col] = new_price  # Обновляем цену в DataFrame
                updated_df.at[_, prim_col] = f"Изменена цена с {old_price} на {new_price}"
                change_info = row.to_dict()
                change_info[price_col] = new_price
                change_info[prim_col] = f"Изменена цена с {old_price} на {new_price}"
                logging.info(
                    f"Для товара с ID {row[id_col]} и артикулом {row[product_id_col]} изменена цена с {old_price} на {new_price}")
                c.execute(f"INSERT INTO '{price_change_log_table}' (timestamp, id, product_id, old_price, new_price, prim) VALUES (?, ?, ?, ?, ?, ?)",
                         (datetime.now().strftime('%Y-%m-%d %H:%M:%S'), row[id_col], row[product_id_col], old_price, new_price, f"Изменена цена с {old_price} на {new_price}"))
                changes.append(change_info)
            else:
                continue

        conn.commit()
    except sqlite3.Error as e:
        logging.error(f"Ошибка при работе с базой данных: {e}")
        return None, None

    conn.close()

    # Создаем новый DataFrame, содержащий только строки с измененными ценами
    price_changed_df = pd.DataFrame(changes)

    return updated_df, price_changed_df