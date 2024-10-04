import asyncio
import aiohttp
import pandas as pd
import logging

logging.basicConfig(
    filename="update_prices_ozon.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

async def update_prices_ozon(df: pd.DataFrame, new_price_col: str, old_price_col: str, offer_id_col: str, min_price_col: str, client_id: str, api_key: str, debug: bool = False):
    """
    Обновляет цены товаров в системе Ozon.

    Args:
        df (pd.DataFrame): DataFrame с информацией о товарах.
        new_price_col (str): Название столбца с новыми ценами.
        old_price_col (str): Название столбца со старыми ценами.
        offer_id_col (str): Название столбца с артикулами товаров.
        min_price_col (str): Название столбца с минимальными ценами.
        client_id (str): Идентификатор клиента.
        api_key (str): API-ключ.
        debug (bool, optional): Флаг отладочного режима. Defaults to False.
    """
    async with aiohttp.ClientSession() as session:
        for _, row in df.iterrows():
            offer_id = row[offer_id_col]
            old_price = row[old_price_col]
            new_price = row[new_price_col]
            min_price = 0

            payload = {
                "prices": [
                    {
                        "auto_action_enabled": "UNKNOWN",
                        "currency_code": "RUB",
                        "min_price": str(min_price),
                        "offer_id": offer_id,
                        "old_price": str(old_price),
                        "price": str(new_price),
                        "price_strategy_enabled": "UNKNOWN",
                        "product_id": 0  # Не используется, но обязательно должен быть указан
                    }
                ]
            }

            headers = {
                "Client-Id": client_id,
                "Api-Key": api_key,
                "Content-Type": "application/json"
            }

            if debug:
                logging.info("Отладочный режим для Ozon включен. Запрос не будет отправлен.")
                logging.info("Отправляемые данные:")
                logging.info(payload)
            else:
                async with session.post("https://api-seller.ozon.ru/v1/product/import/prices", json=payload, headers=headers) as response:
                    if response.status == 200:
                        logging.info(f"Цена товара с артикулом {offer_id} успешно обновлена.")
                    else:
                        logging.error(f"Ошибка при обновлении цены товара с артикулом {offer_id}: {await response.text()}")

# Пример использования
df = pd.DataFrame({
    "offer_id": ["10002179", "10002180", "10002181"],
    "old_price": [1000, 1500, 2000],
    "new_price": [1100, 1600, 2100],
    "min_price": [900, 1400, 1900]
})

client_id = "your_client_id"
api_key = "your_api_key"

asyncio.run(update_prices_ozon(df, "new_price", "old_price", "offer_id", "min_price", client_id, api_key, debug=True))