
import asyncio
import pandas as pd
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from auth import get_credentials
from logger import logger  # Импорт логгера


async def write_sheet_data(df, spreadsheet_id, range_name):
    logger.info("Запуск функции write_sheet_data",
                spreadsheet_id=spreadsheet_id,
                range_name=range_name)

    creds = await get_credentials()

    try:
        service = build("sheets", "v4", credentials=creds)

        # Заполняем пустые значения в DataFrame
        df = df.fillna('')
        logger.debug("DataFrame подготовлен для записи", rows=len(df), columns=len(df.columns))

        # Преобразуем DataFrame в список списков для записи в Google Sheets
        data = df.values.tolist()

        body = {
            "values": data
        }

        # Выполняем синхронный вызов в потоке
        request = service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=range_name,
            valueInputOption="USER_ENTERED",
            body=body
        )

        logger.info("Выполнение запроса к API Google Sheets")
        # Запускаем запрос и получаем ответ
        response = await asyncio.to_thread(request.execute)

        # Теперь response содержит фактический ответ
        logger.info("Данные успешно обновлены в Google Sheets",
                    updated_cells=response.get('updatedCells', 'Неизвестно'),
                    updated_rows=response.get('updatedRows', 'Неизвестно'),
                    updated_columns=response.get('updatedColumns', 'Неизвестно'))

    except HttpError as err:
        # Пример обработки различных ошибок
        if err.resp.status == 403:
            logger.error("Ошибка HTTP 403: Недостаточно прав для обновления таблицы",
                         spreadsheet_id=spreadsheet_id,
                         range_name=range_name,
                         error=str(err))
        else:
            logger.error("Произошла ошибка при записи данных в Google Sheets",
                         spreadsheet_id=spreadsheet_id,
                         range_name=range_name,
                         error=str(err))
    except Exception as e:
        logger.exception("Непредвиденная ошибка в write_sheet_data",
                         spreadsheet_id=spreadsheet_id,
                         range_name=range_name,
                         error=str(e))


