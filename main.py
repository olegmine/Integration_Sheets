import asyncio
import logging
import sqlite3
import pandas as pd
from data_fetcher import get_sheet_data, save_to_database
from data_updater import update_price
from data_writer import write_sheet_data
from config import (
    SAMPLE_SPREADSHEET_ID, UPDATE_INTERVAL_MINUTES,
    SQLITE_DB_NAME
)
from Ozon.update_ozon import update_prices_ozon
from WB.update_wb import update_prices_wb
from YM.update_ym import update_price_ym
from MM.update_mm import update_prices_mm
from logger import logger
from config import (Tech_PC_Components_OZON, Client_Id_Tech_PC_Components_OZON, Smart_Shop_OZON,
                    Client_Id_Smart_Shop_OZON, ByMarket_OZON, Client_Id_ByMarket_OZON, Tech_PC_Components_YM,
                    B_id_Tech_PC_Components_YM, SSmart_shop_YM, B_id_SSmart_shop_YM, ByMarket_YM, B_id_ByMarket_YM,
                    Tech_PC_Components_WB, ByMarket_WB, Smart_shop_WB)

DEBUG = True

# Установите максимальное количество строк и столбцов для отображения
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)


async def delete_table(db_name, table_name):
    try:
        conn = sqlite3.connect(db_name)
        c = conn.cursor()
        c.execute(f"DROP TABLE IF EXISTS '{table_name}'")
        conn.commit()
        logger.info(f"Таблица {table_name} успешно удалена из базы данных {db_name}", db_name=db_name, table_name=table_name)
    except sqlite3.Error as e:
        logger.error(f"Ошибка при удалении таблицы {table_name} из базы данных {db_name}", db_name=db_name, table_name=table_name, error=str(e))
    finally:
        conn.close()

async def update_loop():
    while True:
        try:
            logger.info("Начало цикла обновления данных для всех маркетплейсов")
            await asyncio.gather(
                update_data_ozon(),
                # update_data_wb(),
                # update_data_ym(),
                # update_data_mm() # Раскомментируйте для обновления данных Megamarket
            )
            logger.info("Цикл обновления данных для всех маркетплейсов успешно завершен")
        except Exception as e:
            logger.warning("Критическая ошибка в цикле обновления данных", error=str(e))
        logger.warning(f"Ожидание {UPDATE_INTERVAL_MINUTES} минут до следующего обновления")
        await asyncio.sleep(UPDATE_INTERVAL_MINUTES * 60)

async def update_data_ozon():
    ozon_logger = logger.bind(marketplace="Ozon")
    try:
        ozon_logger.warning("Начало обновления данных Ozon")
        # Определяем итерируемый объект с дополнительными данными
        ozon_ranges = [
            ('ByMarket', 'Ozon!A1:K', Client_Id_ByMarket_OZON,ByMarket_OZON),
            ('Smart Shop', 'Ozon!N1:X', Client_Id_Smart_Shop_OZON, Smart_Shop_OZON),
            ('Tech PC Components', 'Ozon!AA1:AK', Client_Id_Tech_PC_Components_OZON, Tech_PC_Components_OZON)
        ]
        for range_name, sheet_range, client_id, api_key in ozon_ranges:
            ozon_logger.info(f"Обработка диапазона {range_name}")
            df = await get_sheet_data(SAMPLE_SPREADSHEET_ID, sheet_range)
            ozon_logger.info(f"Получены данные из Google Sheets для диапазона {range_name}")
            await save_to_database(df, SQLITE_DB_NAME, f'product_data_ozon_{range_name}', primary_key_cols=['product_id'])
            ozon_logger.info(f"Данные сохранены в базу данных для диапазона {range_name}")
            updated_df, price_changed_df = await update_price(df, product_id_col='product_id',
                                                              old_disc_in_base_col='price_old',
                                                              old_disc_manual_col='old_price')
            ozon_logger.info(f"Обновление цен выполнено для диапазона {range_name}")
            await write_sheet_data(updated_df, SAMPLE_SPREADSHEET_ID, sheet_range.replace('1', '3'))
            ozon_logger.info(f"Обновленные данные записаны в Google Sheets для диапазона {range_name}")
            await save_to_database(updated_df, SQLITE_DB_NAME, f'product_data_ozon_{range_name}',
                                   primary_key_cols=['product_id'])
            print(price_changed_df.head())
            if not price_changed_df.empty:
                ozon_logger.warning(f"Начало обновления цен через API Ozon для диапазона {range_name}", importance="high")
                await update_prices_ozon(price_changed_df,"t_price", 'price_old',
                                         "old_price", "product_id", 'offer_id',
                                         "min_price" , client_id,api_key,debug=DEBUG)
                ozon_logger.warning(f"Завершено обновление цен через API Ozon для диапазона {range_name}")
            ozon_logger.info(f"Обработка диапазона {range_name} завершена", rows_updated=len(price_changed_df))
        ozon_logger.info("Обновление данных Ozon успешно завершено")
    except Exception as e:
        ozon_logger.error("Критическая ошибка при обновлении данных Ozon", error=str(e))

async def update_data_wb():
    wb_logger = logger.bind(marketplace="Wildberries")
    try:
        wb_logger.warning("Начало обновления данных Wildberries")
        wb_ranges = [
            ('Tech PC Components', 'WB!A1:I', Tech_PC_Components_WB),
            ('ByMarket', 'WB!L1:T',ByMarket_WB ),
            ('Smart Shop', 'WB!W1:AE', Smart_shop_WB )
        ]
        for range_name, sheet_range, api_key in wb_ranges:
            wb_logger.info(f"Обработка диапазона {range_name}")
            df = await get_sheet_data(SAMPLE_SPREADSHEET_ID, sheet_range)
            wb_logger.info(f"Получены данные из Google Sheets для диапазона {range_name}")
            await save_to_database(df, SQLITE_DB_NAME, f'product_data_wb_{range_name}', primary_key_cols=['nmID'])
            wb_logger.info(f"Данные сохранены в базу данных для диапазона {range_name}")
            updated_df, price_changed_df = await update_price(df, product_id_col='nmID',
                                                              old_disc_in_base_col='disc_old',
                                                              old_disc_manual_col='discount')
            wb_logger.info(f"Обновление цен выполнено для диапазона {range_name}")
            await write_sheet_data(updated_df, SAMPLE_SPREADSHEET_ID, sheet_range.replace('1', '3'))
            wb_logger.info(f"Обновленные данные записаны в Google Sheets для диапазона {range_name}")
            if not price_changed_df.empty:
                wb_logger.warning(f"Начало обновления цен через API Wildberries для диапазона {range_name}", importance="high")
                await update_prices_wb(price_changed_df, "nmID", "t_price",
                                       "discount", 'disc_old', api_key, debug=DEBUG)
                wb_logger.warning(f"Завершено обновление цен через API Wildberries для диапазона {range_name}")
            wb_logger.info(f"Обработка диапазона {range_name} завершена", rows_updated=len(price_changed_df))
        wb_logger.info("Обновление данных Wildberries успешно завершено")
    except Exception as e:
        wb_logger.error("Критическая ошибка при обновлении данных Wildberries", error=str(e))

async def update_data_ym():
    ym_logger = logger.bind(marketplace="YandexMarket")
    try:
        ym_logger.warning("Начало обновления данных Yandex Market")
        ym_ranges = [
            ('Tech PC Components', 'YM!A1:I', Tech_PC_Components_YM, B_id_Tech_PC_Components_YM),
            ('ByMarket', 'YM!L1:T',  ByMarket_YM, B_id_ByMarket_YM),
            ('Smart Shop', 'YM!W1:AE',SSmart_shop_YM, B_id_SSmart_shop_YM )
        ]
        for range_name, sheet_range, api_key, business_id in ym_ranges:
            ym_logger.info(f"Обработка диапазона {range_name}")
            df = await get_sheet_data(SAMPLE_SPREADSHEET_ID, sheet_range)
            ym_logger.info(f"Получены данные из Google Sheets для диапазона {range_name}")
            await save_to_database(df, SQLITE_DB_NAME, f'product_data_ym_{range_name}', primary_key_cols=['offer_id'])
            ym_logger.info(f"Данные сохранены в базу данных для диапазона {range_name}")
            updated_df, price_changed_df = await update_price(df, product_id_col='offer_id',
                                                              old_disc_in_base_col='price_old',
                                                              old_disc_manual_col='discount_base')
            ym_logger.info(f"Обновление цен выполнено для диапазона {range_name}")
            await write_sheet_data(updated_df, SAMPLE_SPREADSHEET_ID, sheet_range.replace('1', '3'))
            ym_logger.info(f"Обновленные данные записаны в Google Sheets для диапазона {range_name}")
            if not price_changed_df.empty:
                ym_logger.warning(f"Начало обновления цен через API Yandex Market для диапазона {range_name}", importance="high")
                await update_price_ym(price_changed_df, api_key, business_id,"offer_id", "price_old",
                                      "t_price", "discount_base", debug=DEBUG)
                ym_logger.warning(f"Завершено обновление цен через API Yandex Market для диапазона {range_name}")
            ym_logger.info(f"Обработка диапазона {range_name} завершена", rows_updated=len(price_changed_df))
        ym_logger.info("Обновление данных Yandex Market успешно завершено")
    except Exception as e:
        ym_logger.error("Критическая ошибка при обновлении данных Yandex Market", error=str(e))

async def update_data_mm():
    mm_logger = logger.bind(marketplace="Megamarket")
    try:
        mm_logger.info("Начало обновления данных Megamarket")
        for range_name, sheet_range in [('MM1', 'MM!A1:H'), ('MM2', 'MM!K1:R'), ('MM3', 'MM!U1:AB')]:
            mm_logger.info(f"Обработка диапазона {range_name}")
            df = await get_sheet_data(SAMPLE_SPREADSHEET_ID, sheet_range)
            mm_logger.info(f"Получены данные из Google Sheets для диапазона {range_name}")
            await save_to_database(df, SQLITE_DB_NAME, f'product_data_mm_{range_name}', primary_key_cols=['offerId'])
            mm_logger.info(f"Данные сохранены в базу данных для диапазона {range_name}")
            updated_df, price_changed_df = await update_price(df, product_id_col='offerId')
            mm_logger.info(f"Обновление цен выполнено для диапазона {range_name}")
            await write_sheet_data(updated_df, SAMPLE_SPREADSHEET_ID, sheet_range.replace('1', '3'))
            mm_logger.info(f"Обновленные данные записаны в Google Sheets для диапазона {range_name}")
            if not price_changed_df.empty:
                mm_logger.info(f"Начало обновления цен через API Megamarket для диапазона {range_name}", importance="high")
                await update_prices_mm(price_changed_df, 'token', "offerId", "t_price", "isDeleted", debug=DEBUG)
                mm_logger.info(f"Завершено обновление цен через API Megamarket для диапазона {range_name}")
            mm_logger.info(f"Обработка диапазона {range_name} завершена", rows_updated=len(price_changed_df))
        mm_logger.info("Обновление данных Megamarket успешно завершено")
    except Exception as e:
        mm_logger.error("Критическая ошибка при обновлении данных Megamarket", error=str(e))

async def main():
    logger.info("Запуск основного цикла обновления данных для всех маркетплейсов")
    await update_loop()

if __name__ == "__main__":
    asyncio.run(main())

