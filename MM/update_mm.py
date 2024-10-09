import asyncio
import aiohttp
import pandas as pd
import json
import logging

logging.basicConfig(
    filename="update_prices_mm.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

async def update_prices_mm(df, token, offer_id_col, price_col, is_deleted_col, debug=False):
    async with aiohttp.ClientSession() as session:
        url = "https://api.megamarket.tech/api/merchantIntegration/v1/offerService/manualPrice/save"

        prices = []
        for _, row in df.iterrows():
            offer_id = row[offer_id_col]
            if row[is_deleted_col] == '':
                isDeleted = False
            else:
                isDeleted = row[is_deleted_col]
            prices.append({
                "offerId": str(offer_id),
                "price": int(row[price_col]),
                "isDeleted": bool(isDeleted)
            })

        data = {
            "meta": {},
            "data": {
                "token": token,
                "prices": prices
            }
        }

        if debug == True:
            logging.info("Отладочный режим  для MM включен. Запрос не будет отправлен.")
            logging.info("Отправляемые данные:")
            logging.info(json.dumps(data, indent=2))
        else:
            async with session.post(url, headers={"Content-Type": "application/json"}, data=json.dumps(data)) as response:
                if response.status == 200:
                    try:
                        response_data = await response.json()
                        logging.info(f"Цены для товара с артикулом {offer_id} успешно обновлены!")
                        logging.info(f"Ответ сервера: {response_data}")
                    except aiohttp.client_exceptions.ContentTypeError:
                        response_text = await response.text()
                        logging.error(f"Ошибка при обновлении цен для товара с артикулом {offer_id}: {response_text}")
                        logging.info(f"Статус ответа: {response.status}")
                        logging.info(f"Заголовки ответа: {response.headers}")
                else:
                    response_text = await response.text()
                    logging.error(f"Ошибка при отправке в МегаМаркет цен для товара с артикулом {offer_id}: {response_text}")
                    logging.info(f"Статус ответа: {response.status}")
                    logging.info(f"Заголовки ответа: {response.headers}")

# Пример использования
df = pd.DataFrame({
    "offer_id": ["118763"],
    "price": [10200],
    "is_deleted": [False]
})

token = "E20E64D6-DB3B-48C3-A22D-94963381F3F7"
# asyncio.run(update_prices_mm(df, token, "offer_id", "price", "is_deleted", debug=False))