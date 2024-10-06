import asyncio
import aiohttp
import pandas as pd
import json
import logging

logging.basicConfig(
    filename="update_price_ym.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

async def update_price_ym(df, access_token, campaign_id, offer_id_col, new_price_col, discount_base_col, debug=False):
    async with aiohttp.ClientSession() as session:
        tasks = []
        for _, row in df.iterrows():
            offer_id = row[offer_id_col]
            new_price = row[new_price_col]
            discount_base = row[discount_base_col]
            currency_id = "RUR"

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

            url = f"https://api.partner.market.yandex.ru/v2/campaigns/{campaign_id}/offers/prices/update.json"
            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {access_token}"
            }

            if debug:
                logging.info("Отладочный режим для YM включен. Запрос не будет отправлен.")
                logging.info("Отправляемые данные:")
                logging.info(json.dumps(data, indent=2))
            else:
                task = asyncio.create_task(
                    session.post(url, headers=headers, data=json.dumps(data))
                )
                tasks.append(task)

        if not debug:
            responses = await asyncio.gather(*tasks)
            for response in responses:
                response_text = await response.text()  # Fetch response text for all cases early
                if response.status == 200:
                    try:
                        response_data = await response.json()
                        if response_data.get('success') == 0:
                            error_message = response_data.get('error', {}).get('message', 'Unknown error')
                            logging.error(f"Ошибка при обновлении цены для товара с ID {offer_id}: {error_message}")
                        else:
                            logging.info(f"Цена для товара с ID {offer_id} успешно обновлена!")
                            logging.info(f"Ответ сервера: {response_data}")
                    except aiohttp.client_exceptions.ContentTypeError as e:
                        logging.error(f"Ошибка при обновлении цены для товара с ID {offer_id}: {str(e)}")
                        logging.error(f"Ответ сервера: {response_text}")
                else:
                    logging.error(f"Ошибка при отправке в ЯндексМаркет цены для товара с ID {offer_id}: {response_text}")
                    logging.info(f"Статус ответа: {response.status}")
                    logging.info(f"Заголовки ответа: {response.headers}")

async def main():
    access_token = "{access_token}"
    campaign_id = "{campaign_id}"
    df = pd.DataFrame({
        "offer_id": ["123456", "789012", "345678"],
        "new_price": [1999.99, 2499.99, 1799.99],
        "discount_base": [2499.99, 3499.99, 2299.99]
    })

    await update_price_ym(df, access_token, campaign_id, "offer_id", "new_price", "discount_base", debug=False)

if __name__ == "__main__":
    asyncio.run(main())
