import asyncio
import pandas as pd
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from auth import get_credentials

async def write_sheet_data(df, spreadsheet_id, range_name):
    creds = await get_credentials()

    try:
        service = build("sheets", "v4", credentials=creds)

        # Заполняем пустые значения в DataFrame
        df = df.fillna('')

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

        # Запускаем запрос и получаем ответ
        response = await asyncio.to_thread(request.execute)

        # Теперь response содержит фактический ответ
        print("Данные успешно обновлены в Google Sheets.")
        print("Ответ от Google Sheets:", response)

        # Проверяем обновленные ячейки
        print("Обновлено ячеек:", response.get('updatedCells', 'Неизвестно'))
        print("Обновлено строк:", response.get('updatedRows', 'Неизвестно'))
        print("Обновлено столбцов:", response.get('updatedColumns', 'Неизвестно'))

    except HttpError as err:
        # Пример обработки различных ошибок
        if err.resp.status == 403:
            print(
                "Ошибка 403: У вас недостаточно прав для обновления данной таблицы. Проверьте разрешения API и доступы.")
        else:
            print(f"Произошла ошибка при записи данных: {err}")