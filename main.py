import asyncio
import logging
import sqlite3
import pandas as pd
from data_fetcher import get_sheet_data, save_to_database
from data_updater import update_price
from data_writer import write_sheet_data
from config import (
    SAMPLE_SPREADSHEET_ID, SAMPLE_RANGE_NAME, UPDATE_INTERVAL_MINUTES,
    SQLITE_DB_NAME, RANGE_NAME_OZON2, RANGE_NAME_OZON3
)
from Ozon.update_ozon import update_prices_ozon
from WB.update_wb import update_prices_wb
from YM.update_ym import update_price_ym
from MM.update_mm import update_prices_mm

DEBUG = False

# Установите максимальное количество строк и столбцов для отображения
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)

# Настройка логирования
logging.basicConfig(
    filename='app.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

async def delete_table(db_name, table_name):
    """Удаляет таблицу из базы данных"""
    try:
        conn = sqlite3.connect(db_name)
        c = conn.cursor()
        c.execute(f"DROP TABLE IF EXISTS '{table_name}'")
        conn.commit()
    except sqlite3.Error as e:
        logging.error(f"Ошибка при работе с базой данных: {e}")
    finally:
        conn.close()

async def update_loop():
    while True:
        try:
            await update_data_ozon()
            await update_data_wb()
            await update_data_ym()
            await update_data_mm()
        except Exception as e:
            logging.error(f"Ошибка при обновлении данных: {e}")
        await asyncio.sleep(UPDATE_INTERVAL_MINUTES * 60)

async def update_data_ozon():
    df = await get_sheet_data(SAMPLE_SPREADSHEET_ID, SAMPLE_RANGE_NAME)
    await save_to_database(df, SQLITE_DB_NAME, 'product_data_ozon1', primary_key_cols=['offer_id'])
    updated_df, price_changed_df = await update_price(df, product_id_col='offer_id')
    await write_sheet_data(updated_df, SAMPLE_SPREADSHEET_ID, 'Ozon!A3:H')
    if not price_changed_df.empty:
        await update_prices_ozon(price_changed_df, "t_price", "old_price", "offer_id", "min_price", 'client_id', 'api_key', debug=DEBUG)

    df = await get_sheet_data(SAMPLE_SPREADSHEET_ID, RANGE_NAME_OZON2)
    await save_to_database(df, SQLITE_DB_NAME, 'product_data_ozon2', primary_key_cols=['offer_id'])
    updated_df, price_changed_df = await update_price(df, product_id_col='offer_id')
    await write_sheet_data(updated_df, SAMPLE_SPREADSHEET_ID, 'Ozon!K3:R')
    if not price_changed_df.empty:
        await update_prices_ozon(price_changed_df, "t_price", "old_price", "offer_id", "min_price", 'client_id', 'api_key', debug=DEBUG)

    df = await get_sheet_data(SAMPLE_SPREADSHEET_ID, RANGE_NAME_OZON3)
    await save_to_database(df, SQLITE_DB_NAME, 'product_data_ozon3', primary_key_cols=['offer_id'])
    updated_df, price_changed_df = await update_price(df, product_id_col='offer_id')
    await write_sheet_data(updated_df, SAMPLE_SPREADSHEET_ID, 'Ozon!W3:AD')
    if not price_changed_df.empty:
        await update_prices_ozon(price_changed_df, "t_price", "old_price", "offer_id", "min_price", 'client_id', 'api_key', debug=DEBUG)

async def update_data_wb():
    df = await get_sheet_data(SAMPLE_SPREADSHEET_ID, 'WB!A1:H')
    await save_to_database(df, SQLITE_DB_NAME, 'product_data_wb1', primary_key_cols=['nmID'])
    updated_df, price_changed_df = await update_price(df, product_id_col='nmID')
    await write_sheet_data(updated_df, SAMPLE_SPREADSHEET_ID, 'WB!A3:H')
    if not price_changed_df.empty:
        await update_prices_wb(price_changed_df, "nmID", "t_price", "discount", 'api_key', debug=DEBUG)

    df = await get_sheet_data(SAMPLE_SPREADSHEET_ID, 'WB!K1:R')
    await save_to_database(df, SQLITE_DB_NAME, 'product_data_wb2', primary_key_cols=['nmID'])
    updated_df, price_changed_df = await update_price(df, product_id_col='nmID')
    await write_sheet_data(updated_df, SAMPLE_SPREADSHEET_ID, 'WB!K3:R')
    if not price_changed_df.empty:
        await update_prices_wb(price_changed_df, "nmID", "t_price", "discount", 'api_key', debug=DEBUG)

    df = await get_sheet_data(SAMPLE_SPREADSHEET_ID, 'WB!U1:AB')
    await save_to_database(df, SQLITE_DB_NAME, 'product_data_wb3', primary_key_cols=['nmID'])
    updated_df, price_changed_df = await update_price(df, product_id_col='nmID')
    await write_sheet_data(updated_df, SAMPLE_SPREADSHEET_ID, 'WB!U3:AB')
    if not price_changed_df.empty:
        await update_prices_wb(price_changed_df, "nmID", "t_price", "discount", 'api_key', debug=DEBUG)

async def update_data_ym():
    df = await get_sheet_data(SAMPLE_SPREADSHEET_ID, 'YM!A1:H')
    await save_to_database(df, SQLITE_DB_NAME, 'product_data_ym1', primary_key_cols=['offer_id'])
    updated_df, price_changed_df = await update_price(df, product_id_col='offer_id')
    await write_sheet_data(updated_df, SAMPLE_SPREADSHEET_ID, 'YM!A3:H')
    if not price_changed_df.empty:
        await update_price_ym(price_changed_df, 'access_token', 'campaign_id', "offer_id", "t_price", "discount_base", debug=DEBUG)

    df = await get_sheet_data(SAMPLE_SPREADSHEET_ID, 'YM!K1:R')
    await save_to_database(df, SQLITE_DB_NAME, 'product_data_ym2', primary_key_cols=['offer_id'])
    updated_df, price_changed_df = await update_price(df, product_id_col='offer_id')
    await write_sheet_data(updated_df, SAMPLE_SPREADSHEET_ID, 'YM!K3:R')
    if not price_changed_df.empty:
        await update_price_ym(price_changed_df, 'access_token', 'campaign_id', "offer_id", "t_price", "discount_base", debug=DEBUG)

    df = await get_sheet_data(SAMPLE_SPREADSHEET_ID, 'YM!U1:AB')
    await save_to_database(df, SQLITE_DB_NAME, 'product_data_ym3', primary_key_cols=['offer_id'])
    updated_df, price_changed_df = await update_price(df, product_id_col='offer_id')
    await write_sheet_data(updated_df, SAMPLE_SPREADSHEET_ID, 'YM!U3:AB')
    if not price_changed_df.empty:
        await update_price_ym(price_changed_df, 'access_token', 'campaign_id', "offer_id", "t_price", "discount_base", debug=DEBUG)

async def update_data_mm():
    df = await get_sheet_data(SAMPLE_SPREADSHEET_ID, 'MM!A1:H')
    await save_to_database(df, SQLITE_DB_NAME, 'product_data_mm1', primary_key_cols=['offerId'])
    updated_df, price_changed_df = await update_price(df, product_id_col='offerId')
    await write_sheet_data(updated_df, SAMPLE_SPREADSHEET_ID, 'MM!A3:H')
    if not price_changed_df.empty:
        await update_prices_mm(price_changed_df, 'token', "offerId", "t_price", "isDeleted", debug=DEBUG)

    df = await get_sheet_data(SAMPLE_SPREADSHEET_ID, 'MM!K1:R')
    await save_to_database(df, SQLITE_DB_NAME, 'product_data_mm2', primary_key_cols=['offerId'])
    updated_df, price_changed_df = await update_price(df, product_id_col='offerId')
    await write_sheet_data(updated_df, SAMPLE_SPREADSHEET_ID, 'MM!K3:R')
    if not price_changed_df.empty:
        await update_prices_mm(price_changed_df, 'token', "offerId", "t_price", "isDeleted", debug=DEBUG)

    df = await get_sheet_data(SAMPLE_SPREADSHEET_ID, 'MM!U1:AB')
    await save_to_database(df, SQLITE_DB_NAME, 'product_data_mm3', primary_key_cols=['offerId'])
    updated_df, price_changed_df = await update_price(df, product_id_col='offerId')
    await write_sheet_data(updated_df, SAMPLE_SPREADSHEET_ID, 'MM!U3:AB')
    if not price_changed_df.empty:
        await update_prices_mm(price_changed_df, 'token', "offerId", "t_price", "isDeleted", debug=DEBUG)

async def main():
    await update_loop()

if __name__ == "__main__":
    asyncio.run(main())