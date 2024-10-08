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

            url = f"https://api.partner.market.yandex.ru/businesses/{campaign_id}/offer-prices/updates"
            headers = {
                "Content-Type": "application/json",
                "Api-Key": access_token
            }

            if debug:
                logging.info("Отладочный режим для YM включен. Запрос не будет отправлен.")
                logging.info("Отправляемые данные:")
                logging.info(json.dumps(data, indent=2))
            else:
                task = asyncio.create_task(
                    session.post(url, headers=headers, data=json.dumps(data))
                )
                tasks.append((offer_id, task))

        if not debug:
            for offer_id, task in tasks:
                response = await task
                response_text = await response.text()  # Fetch response text for all cases

                logging.info(f"Полный ответ сервера для товара с ID {offer_id}:")
                logging.info(response_text)

                if response.status == 200:
                    try:
                        response_data = json.loads(response_text)
                        if response_data.get('success') == 0:
                            error_message = response_data.get('error', {}).get('message', 'Unknown error')
                            logging.error(f"Ошибка при обновлении цены для товара с ID {offer_id}: {error_message}")
                        else:
                            logging.info(f"Цена для товара с ID {offer_id} успешно обновлена!")
                    except json.JSONDecodeError as e:
                        logging.error(f"Ошибка при разборе JSON для товара с ID {offer_id}: {str(e)}")
                else:
                    logging.error(f"Ошибка при отправке в ЯндексМаркет цены для товара с ID {offer_id}")
                    logging.info(f"Статус ответа: {response.status}")
                    logging.info(f"Заголовки ответа: {response.headers}")


async def main():
    access_token = "ACMA:D4a5OExH6Hvtcx8BxgTqv2gfIpc2E7KmTPlekqDE:43a81531"
    campaign_id = "76443469"
    business_id = '76443469'
    df = pd.DataFrame({
        "offer_id": ["Blackshark-ucenka"],
        "new_price": [3380],
        "discount_base": [5600]
    })

    await update_price_ym(df, access_token, campaign_id, "offer_id", "new_price", "discount_base", debug=False)

if __name__ == "__main__":
    asyncio.run(main())
