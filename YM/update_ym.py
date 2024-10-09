import asyncio
import aiohttp
import pandas as pd
import json
import os,sys


# Добавляем корневую директорию проекта в sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from logger import logger  # Импортируем настроенный логгер


async def update_price_ym(df, access_token, campaign_id, offer_id_col, disc_old_col, new_price_col, discount_base_col, debug=False):
    async with aiohttp.ClientSession() as session:  # Создаем асинхронную сессию для HTTP-запросов
        tasks = []  # Список для хранения задач
        for _, row in df.iterrows():
            offer_id = row[offer_id_col]  # Получаем идентификатор предложения
            new_price = row[new_price_col]  # Получаем новую цену
            discount_base = row[discount_base_col]  # Получаем базу скидки

            currency_id = "RUR"  # Устанавливаем валюту

            # Пытаемся преобразовать discount_base в целое число
            try:
                discount_base = int(discount_base)
            except ValueError:
                logger.warning("Недопустимое значение базы скидки", offer_id=offer_id, discount_base=discount_base)
                try:
                    discount = int(row[disc_old_col])
                    logger.info(f"Успешно получена скидка из резервной колонки: {discount}")
                except (ValueError, TypeError):
                    discount = 0  # Значение по умолчанию, если преобразование не удалось
                    logger.warning(f"Не удалось получить корректное значение скидки. Установлено значение по умолчанию: {discount}")

            # Подготовка данных для запроса
            data = {
                "offers": [
                    {
                        "offerId": offer_id,
                        "price": {
                            "value": new_price,
                            "currencyId": currency_id,
                            "discountBase": discount_base
                        }
                    }
                ]
            }

            url = f"https://api.partner.market.yandex.ru/businesses/{campaign_id}/offer-prices/updates"  # URL для обновления цен
            headers = {
                "Content-Type": "application/json",
                "Api-Key": access_token  # Заголовок с токеном доступа
            }

            if debug:
                logger.info("Режим отладки включен. Запрос не будет отправлен.")
                logger.info("Отправляемые данные:")
                logger.info(json.dumps(data, ensure_ascii=False, indent=2))  # Предотвращаем экранирование Unicode в логах
            else:
                task = asyncio.create_task(session.post(url, headers=headers, data=json.dumps(data, ensure_ascii=False)))  # Отправляем POST-запрос
                tasks.append((offer_id, task))  # Добавляем задачу в список

        if not debug:
            for offer_id, task in tasks:
                response = await task  # Ждем завершения задачи
                response_text = await response.text()  # Получаем текст ответа

                logger.info(f"Полный ответ сервера для товара с ID {offer_id}:")
                logger.info(response_text)

                if response.status == 200:
                    try:
                        response_data = json.loads(response_text)  # Разбираем JSON-ответ
                        if response_data.get('success') == 0:
                            error_message = response_data.get('error', {}).get('message', 'Неизвестная ошибка')
                            logger.error(f"Ошибка при обновлении цены для товара с ID {offer_id}: {error_message}")
                        else:
                            logger.info(f"Цена для товара с ID {offer_id} успешно обновлена!")
                    except json.JSONDecodeError as e:
                        logger.error(f"Ошибка при разборе JSON для товара с ID {offer_id}: {str(e)}")
                else:
                    logger.error(f"Ошибка при отправке в Яндекс.Маркет цены для товара с ID {offer_id}")
                    logger.info(f"Статус ответа: {response.status}")
                    logger.info(f"Заголовки ответа: {response.headers}")

# Основная функция
async def main():
    access_token = "sample"  # Токен доступа
    campaign_id = "sample"  # Идентификатор кампании
    df = pd.DataFrame({
        "offer_id": ["Blackshark-учебник"],
        "new_price": [3380],
        "discount_base": ["не число"],
        "discount_base_col": [4000]  # Пример значения
    })

    await update_price_ym(df, access_token, campaign_id, "offer_id", "discount_base_col", "new_price", "discount_base", debug=True)

if __name__ == "__main__":
    asyncio.run(main())  # Запуск основной функции