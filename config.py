import os
from dotenv import load_dotenv


SAMPLE_SPREADSHEET_ID = '1k_1W6IL1AN9hJ8ZOsrZux1yDxuB0ugew20UdtNzWneM' #id таблицы на гугл драйв
SQLITE_DB_NAME = 'data.db'
# LOG_FILE_NAME = 'app.log'
UPDATE_INTERVAL_MINUTES = 5

  # Загрузка переменных из .env файла
load_dotenv()

# Озон
Tech_PC_Components_OZON = os.getenv('Tech_PC_Components_OZON')
Client_Id_Tech_PC_Components_OZON = "1336645"

Smart_Shop_OZON = os.getenv('Smart_Shop_OZON')
Client_Id_Smart_Shop_OZON = "1921962"

ByMarket_OZON = os.getenv('ByMarket_OZON')
Client_Id_ByMarket_OZON = "1515458"

# Яндекс маркет
Tech_PC_Components_YM = os.getenv('Tech_PC_Components_YM')
B_id_Tech_PC_Components_YM = "76443469"

SSmart_shop_YM = os.getenv('SSmart_shop_YM')
B_id_SSmart_shop_YM ="121883700"

ByMarket_YM = os.getenv('ByMarket_YM')
B_id_ByMarket_YM = "95137059"

# Вайлдберриз
Tech_PC_Components_WB = os.getenv('Tech_PC_Components_WB')
ByMarket_WB = os.getenv('ByMarket_WB')
Smart_shop_WB = os.getenv('Smart_shop_WB')