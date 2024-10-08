
import asyncio
import aiohttp
import pandas as pd
import logging
import json

logging.basicConfig(
    filename="update_prices_ozon.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

def prepare_dataframe_for_json(df):
    for col in df.select_dtypes(include=['int64', 'float64']).columns:
        if col != 'product_id':
            df[col] = df[col].astype(str)
    return df

async def update_prices_ozon(df: pd.DataFrame, new_price_col: str, old_price_col: str, product_id_col: str,offer_id_col: str,
                             min_price_col: str, client_id: str, api_key: str, debug: bool = False):
    df = prepare_dataframe_for_json(df)

    async with aiohttp.ClientSession() as session:
        for _, row in df.iterrows():
            product_id = int(row[product_id_col])
            offer_id = str(row[offer_id_col])
            old_price = row[old_price_col]
            new_price = row[new_price_col]
            min_price = row.get(min_price_col, '0')

            payload = {
                "prices": [
                    {
                        "auto_action_enabled": "UNKNOWN",
                        "currency_code": "RUB",
                        "min_price": min_price,
                        "offer_id": offer_id,
                        "old_price": old_price,
                        "price": new_price,
                        "price_strategy_enabled": "UNKNOWN",
                        "product_id": product_id
                    }
                ]
            }

            headers = {
                "Client-Id": client_id,
                "Api-Key": api_key,
                "Content-Type": "application/json"
            }

            logging.info(f"Отправляемые данные: {json.dumps(payload, ensure_ascii=False)}")

            if debug:
                logging.info(f"DEBUG MODE: Запрос не отправлен. Payload: {json.dumps(payload, ensure_ascii=False)}")
            else:
                async with session.post("https://api-seller.ozon.ru/v1/product/import/prices", json=payload,
                                        headers=headers) as response:
                    response_text = await response.text()
                    logging.info(f"Статус ответа: {response.status}")
                    logging.info(f"Текст ответа: {response_text}")
                    logging.info(f"Заголовки ответа: {response.headers}")

                    if response.status == 200:
                        try:
                            response_data = json.loads(response_text)
                            if response_data.get("result"):
                                logging.info(f"Цена товара с product_id {product_id} успешно обновлена.")
                            else:
                                error_message = response_data.get("error", {}).get("message", "Неизвестная ошибка")
                                logging.error(
                                    f"Ошибка при обновлении цены товара с product_id {product_id}: {error_message}")
                        except json.JSONDecodeError:
                            logging.error(f"Ошибка при декодировании JSON ответа для товара с product_id {product_id}")
                    else:
                        logging.error(f"Ошибка при отправке в OZON цены товара с product_id {product_id}: {response_text}")

# Пример использования
df = pd.DataFrame({
    "offer_id": ['Moroccanoil-100ml'],
    "old_price": [2900],
    "new_price": [2670],
    'product_id': [1706152388]
})

client_id = "1336645"
api_key = "e6640a3f-d177-4b08-9487-59be840f8a8c"

# Для отправки реальных запросов
# asyncio.run(update_prices_ozon(df, "new_price", "old_price",
                               # "offer_id", "min_price", client_id, api_key))

# # Для тестового режима (debug mode)
# asyncio.run(update_prices_ozon(df, "new_price", "old_price",
#                                "product_id", 'offer_id',"min_price", client_id, api_key, debug=False))
