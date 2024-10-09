import asyncio
import aiohttp
import pandas as pd
import json
import os,sys


# Добавляем корневую директорию проекта в sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from logger import logger  # Импортируем настроенный логгер

def prepare_dataframe_for_json(df):
    # Подготовка DataFrame путем преобразования числовых столбцов в строки (кроме product_id)
    for col in df.select_dtypes(include=['int64', 'float64']).columns:
        if col != 'product_id':
            df[col] = df[col].astype(str)
    return df


async def update_prices_ozon(df: pd.DataFrame, new_price_col: str, base_old_price_col: str, old_price_col: str,
                             product_id_col: str, offer_id_col: str, min_price_col: str,
                             client_id: str, api_key: str, debug: bool = False):
    df = prepare_dataframe_for_json(df)  # Подготовка данных для JSON

    async with aiohttp.ClientSession() as session:  # Создаем асинхронную сессию для HTTP-запросов
        for _, row in df.iterrows():
            product_id = int(row[product_id_col])  # Получаем ID продукта
            offer_id = str(row[offer_id_col])  # Получаем ID предложения

            new_price = int(round(float(row[new_price_col])))  # Получаем новую цену
            min_price = row.get(min_price_col, '0')  # Получаем минимальную цену, по умолчанию '0'

            # Пытаемся преобразовать base_old_price в целое число
            try:
                old_price = int(round(float(row[old_price_col])))  # Получаем старую цену
            except ValueError:
                logger.warning(
                    f"Недопустимое значение базы скидки для offer_id: {offer_id}, discount_base: {row[old_price_col]}")
                try:

                    old_price = int(round(float(row[base_old_price_col])))
                except :
                    old_price = 0  # Значение по умолчанию, если преобразование не удалось
                    logger.warning(f"Установлено значение по умолчанию для старой цены : {0}")

                # Подготовка полезной нагрузки для запроса
            payload = {
                "prices": [
                    {
                        "auto_action_enabled": "UNKNOWN",
                        "currency_code": "RUB",
                        "min_price": "0",
                        "offer_id": str(offer_id),
                        "old_price": str(old_price),
                        "price": str(new_price),
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

            logger.info(f"Отправляемые данные: {json.dumps(payload, ensure_ascii=False)}")  # Логируем полезную нагрузку

            if debug:
                logger.info(f"Режим отладки: Запрос не отправлен. Данные: {json.dumps(payload, ensure_ascii=False)}")
            else:
                async with session.post("https://api-seller.ozon.ru/v1/product/import/prices", json=payload,
                                        headers=headers) as response:
                    response_text = await response.text()  # Получаем текст ответа
                    logger.info(f"Статус ответа: {response.status}")  # Логируем статус ответа
                    logger.info(f"Текст ответа: {response_text}")  # Логируем текст ответа
                    logger.info(f"Заголовки ответа: {response.headers}")  # Логируем заголовки ответа

                    if response.status == 200:
                        try:
                            response_data = json.loads(response_text)  # Разбираем JSON-ответ
                            if response_data.get("result"):
                                logger.info(f"Цена товара с product_id {product_id} успешно обновлена.")
                            else:
                                error_message = response_data.get("error", {}).get("message", "Неизвестная ошибка")
                                logger.error(f"Ошибка при обновлении цены товара с product_id {product_id}: {error_message}")
                        except json.JSONDecodeError:
                            logger.error(f"Ошибка при декодировании JSON ответа для товара с product_id {product_id}")
                    else:
                        logger.error(f"Ошибка при отправке в OZON цены товара с product_id {product_id}: {response_text}")

# Пример использования
df = pd.DataFrame({
    "offer_id": ['Moroccanoil-100ml'],
    "old_price": ['hhh'],
    "new_price": [2670],
    'product_id': [1706152388],
    'disc_old_col':["l"]
})

client_id = "1336645"
api_key = "e6640a3f-d177-4b08-9487-59be840f8a8c"

# Для отправки реальных запросов
# asyncio.run(update_prices_ozon(df, "new_price", "old_price", "product_id", 'offer_id', "min_price", client_id, api_key))

# Для тестового режима (режим отладки)
# asyncio.run(update_prices_ozon(df, "new_price", 'disc_old_col',"old_price", "product_id", 'offer_id', "min_price", client_id, api_key, debug=True))
