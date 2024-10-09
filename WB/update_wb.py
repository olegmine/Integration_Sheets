import asyncio
import aiohttp
import pandas as pd
import sys
import os

# Добавляем корневую директорию проекта в sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from logger import logger


async def update_prices_wb(df, nmID_col, price_col, discount_col, disc_old_col, api_key: str, debug: bool = False):
    url = f"https://discounts-prices-api.wildberries.ru/api/v2/upload/task"
    headers = {
        "Authorization": api_key,
        "Content-Type": "application/json"
    }
    goods = []
    logger.info("Начало обработки данных для обновления цен и скидок")
    for index, row in df.iterrows():
        logger.info(f"Обработка строки {index + 1}/{len(df)}")
        try:
            nmID = int(row[nmID_col])
            price = int(row[price_col])
            logger.info(f"Успешно получены nmID: {nmID} и цена: {price}")
        except (ValueError, TypeError):
            logger.error(f"Ошибка при преобразовании nmID или цены в строке {index + 1}. Пропуск строки.")
            continue

        try:
            discount = int(row[discount_col])
            logger.info(f"Успешно получена скидка из основной колонки: {discount}")
        except (ValueError, TypeError):
            logger.warning(f"Не удалось получить скидку из основной колонки '{discount_col}'. Попытка использовать резервную колонку '{disc_old_col}'.")
            try:
                discount = int(row[disc_old_col])
                logger.info(f"Успешно получена скидка из резервной колонки: {discount}")
            except (ValueError, TypeError):
                discount = 0
                logger.warning(f"Не удалось получить корректное значение скидки. Установлено значение по умолчанию: {discount}")

        goods.append({
            "nmID": nmID,
            "price": price,
            "discount": discount
        })
        logger.info(f"Добавлен товар: nmID={nmID}, price={price}, discount={discount}")

    payload = {
        "data": goods
    }

    if debug:
        logger.warning("Включен режим отладки для WB. Запрос к API не будет отправлен.",
                       payload=payload)
        return {"error": False, "errorText": ""}
    else:
        logger.info("Подготовка к отправке запроса API в Wildberries",
                    url=url, payload_size=len(goods))
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, json=payload) as response:
                if response.status == 200:
                    try:
                        response_data = await response.json()
                        if response_data.get("error", False):
                            error_text = response_data.get("errorText", "Неизвестная ошибка")
                            logger.warning("Ошибка при обновлении цен и скидок",
                                           error=error_text,
                                           response=response_data)
                        else:
                            logger.info("Цены и скидки успешно обновлены",
                                        response=response_data)
                        return response_data
                    except aiohttp.client_exceptions.ContentTypeError:
                        response_text = await response.text()
                        logger.warning("Ошибка при разборе ответа от API",
                                       status=response.status,
                                       headers=dict(response.headers),
                                       response_text=response_text)
                        return {"error": True, "errorText": response_text}
                else:
                    response_text = await response.text()
                    logger.warning("Ошибка при отправке запроса в Wildberries",
                                   status=response.status,
                                   headers=dict(response.headers),
                                   response_text=response_text)
                    return {"error": True, "errorText": response_text}

if __name__ == "__main__":
    # Пример DataFrame
    df = pd.DataFrame({
        "nmID": [234732552, 239118382, 237350405, 237350406],
        "price": [6000, 14800, 11490, 10000],
        "discount": [29, "не число", 44, "не число"],
        "disc_old": [25, 63, 40, "тоже не число"]
    })

    api_key = "your_api_key_here"  # Замените на ваш реальный API ключ

    logger.info("Начало процесса обновления цен")
    asyncio.run(update_prices_wb(df, "nmID", "price", "discount", "disc_old", api_key, debug=True))
    logger.info("Процесс обновления цен завершен")



