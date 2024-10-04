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
            "nmID": row[nmID_col],
            "price": row[price_col],
            "discount": row[discount_col]
        }
        for _, row in df.iterrows()
    ]
    payload = {
        "data": goods
    }

    if debug:
        logging.info("Отладочный режим для WB включен. Запрос не будет отправлен.")
        logging.info("Отправляемые данные:")
        logging.info(payload)
        return {"error": False, "errorText": ""}
    else:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, json=payload) as response:
                response_data = await response.json()
                if response.status == 200:
                    logging.info("Цены и скидки успешно обновлены")
                    return response_data
                else:
                    logging.error(f"Ошибка при обновлении цен и скидок: {response_data['errorText']}")
                    return response_data

if __name__ == "__main__":
    # Пример DataFrame
    df = pd.DataFrame({
        "nmID": [123, 456, 789],
        "price": [999, 1499, 799],
        "discount": [30, 20, 40]
    })

    api_key = "your_api_key_here"
    asyncio.run(update_prices_wb(df, "nmID", "price", "discount", api_key, debug=True))