
import os.path
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from logger import logger  # Импорт логгера

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

async def get_credentials():
    logger.info("Запуск функции get_credentials")
    creds = None
    if os.path.exists("token.json"):
        logger.debug("Найден существующий файл token.json")
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)
        logger.info("Загружены учетные данные из token.json")

    if not creds or not creds.valid:
        logger.info("Учетные данные отсутствуют или недействительны")
        if creds and creds.expired and creds.refresh_token:
            logger.info("Обновление просроченных учетных данных")
            try:
                creds.refresh(Request())
                logger.info("Учетные данные успешно обновлены")
            except Exception as e:
                logger.error("Не удалось обновить учетные данные", error=str(e))
        else:
            logger.info("Инициация нового процесса аутентификации")
            try:
                flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
                creds = flow.run_local_server(port=0)
                logger.info("Получены новые учетные данные через локальный сервер")
            except Exception as e:
                logger.error("Не удалось получить новые учетные данные", error=str(e))
                raise

        logger.debug("Сохранение новых учетных данных в token.json")
        try:
            with open("token.json", "w") as token:
                token.write(creds.to_json())
            logger.info("Новые учетные данные сохранены в token.json")
        except Exception as e:
            logger.error("Не удалось сохранить учетные данные в token.json", error=str(e))

    logger.info("Учетные данные успешно получены")
    return creds

