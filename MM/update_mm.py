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
            if row[is_deleted_col] == '':
                isDeleted = False
            else:
                isDeleted = row[is_deleted_col]
            prices.append({
                "offerId": str(row[offer_id_col]),
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

        if debug:
            logging.info("Отладочный режим  для MM включен. Запрос не будет отправлен.")
            logging.info("Отправляемые данные:")
            logging.info(json.dumps(data, indent=2))
        else:
            async with session.post(url, headers={"Content-Type": "application/json"}, data=json.dumps(data)) as response:
                if response.status == 200:
                    logging.info("Цены успешно обновлены!")
                else:
                    logging.error(f"Ошибка при обновлении цен: {response.status} - {await response.text()}")

# Пример использования
df = pd.DataFrame({
    "offer_id": ["10002179"],
    "price": [2000],
    "is_deleted": [False]
})

token = "********-****-****-****-************"
asyncio.run(update_prices_mm(df, token, "offer_id", "price", "is_deleted", debug=True))