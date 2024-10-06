import asyncio
import aiohttp
import pandas as pd
import logging

logging.basicConfig(
    filename="update_prices_wb.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

async def update_prices_wb(df, nmID_col, price_col, discount_col, api_key: str, debug: bool = False):
    url = f"https://openapi.wildberries.ru/prices/api/ru/api/v2/upload/task"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    goods = [
        {
            "nmID": int(row[nmID_col]),
            "price": int(row[price_col]),
            "discount": int(row[discount_col])
        }
        for _, row in df.iterrows()
    ]
    payload = {
        "data": goods
    }

    if debug == True:
        logging.info("Отладочный режим для WB включен. Запрос не будет отправлен.")
        logging.info("Отправляемые данные:")
        logging.info(payload)
        return {"error": False, "errorText": ""}
    else:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, json=payload) as response:
                if response.status == 200:
                    try:
                        response_data = await response.json()
                        if response_data.get("error", False):
                            error_text = response_data.get("errorText", "Неизвестная ошибка")
                            for _, row in df.iterrows():
                                nmID = row[nmID_col]
                                logging.error(f"Ошибка при обновлении цен и скидок для товара с номером {nmID}: {error_text}")
                            logging.info(f"Ответ сервера: {response_data}")
                        else:
                            for _, row in df.iterrows():
                                nmID = row[nmID_col]
                                logging.info(f"Цены и скидки для товара с номером {nmID} успешно обновлены")
                            logging.info(f"Ответ сервера: {response_data}")
                        return response_data
                    except aiohttp.client_exceptions.ContentTypeError:
                        response_text = await response.text()
                        for _, row in df.iterrows():
                            nmID = row[nmID_col]
                            logging.error(f"Ошибка при обновлении цен и скидок для товара с номером {nmID}: {response_text}")
                        logging.info(f"Статус ответа: {response.status}")
                        logging.info(f"Заголовки ответа: {response.headers}")
                        return {"error": True, "errorText": response_text}
                else:
                    response_text = await response.text()
                    for _, row in df.iterrows():
                        nmID = row[nmID_col]
                        logging.error(f"Ошибка при отправке в Вайлдбериз цен и скидок для товара с номером {nmID}: {response_text}")
                    logging.info(f"Статус ответа: {response.status}")
                    logging.info(f"Заголовки ответа: {response.headers}")
                    return {"error": True, "errorText": response_text}

if __name__ == "__main__":
    # Пример DataFrame
    df = pd.DataFrame({
        "nmID": [123, 456, 789],
        "price": [999, 1499, 799],
        "discount": [30, 20, 40]
    })

    api_key = "your_api_key_here"
    asyncio.run(update_prices_wb(df, "nmID", "price", "discount", api_key, debug=False))