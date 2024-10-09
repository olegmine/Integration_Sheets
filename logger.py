import structlog
import logging
from logging.handlers import TimedRotatingFileHandler
import json
import os
from datetime import datetime, timedelta


class NonEscapingJsonEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, str):
            return obj
        return super().default(obj)

    def encode(self, obj):
        return json.dumps(obj, ensure_ascii=False, separators=(',', ':'))


def json_serializer(event_dict, *args, **kwargs):
    return json.dumps(event_dict, ensure_ascii=False, cls=NonEscapingJsonEncoder)


class ColorCodes:
    RESET = "\033[0m"
    RED = "\033[31m"
    GREEN = "\033[32m"
    YELLOW = "\033[33m"
    BLUE = "\033[34m"
    MAGENTA = "\033[35m"
    CYAN = "\033[36m"
    WHITE = "\033[37m"
    BOLD = "\033[1m"


def add_color_and_importance(logger, method_name, event_dict):
    level = event_dict.get('level', '')
    importance = event_dict.pop('importance', 'normal')

    if 'message' not in event_dict:
        event_dict['message'] = "Сообщение не предоставлено"

    if importance == 'high':
        event_dict['message'] = f"[ВАЖНО] {event_dict['message']}"

    if method_name == 'info':
        color = ColorCodes.GREEN
    elif method_name in ['warning', 'warn']:
        color = ColorCodes.YELLOW
    elif method_name in ['error', 'exception', 'critical']:
        color = ColorCodes.RED
    else:
        color = ColorCodes.WHITE

    if importance == 'high':
        color += ColorCodes.BOLD

    event_dict['colored_message'] = f"{color}{event_dict['message']}{ColorCodes.RESET}"
    return event_dict


def filter_important_logs(logger, method_name, event_dict):
    if event_dict and '[ВАЖНО]' in event_dict.get('message', ''):
        return event_dict
    return event_dict  # Оставляем лог пустым, но возвращаем в консоль


def reorder_event_dict(logger, method_name, event_dict):
    if 'marketplace' in event_dict:
        marketplace_value = event_dict.pop('marketplace')
        event_dict = {'marketplace': marketplace_value, **event_dict}
    return event_dict


def remove_empty_values(logger, method_name, event_dict):
    return {k: v for k, v in event_dict.items() if v is not None and v != ''}


def cleanup_old_logs(log_directory, days_to_keep=10):
    current_date = datetime.now()
    cutoff_date = current_date - timedelta(days=days_to_keep)

    for filename in os.listdir(log_directory):
        file_path = os.path.join(log_directory, filename)
        if os.path.isfile(file_path) and filename.startswith('app.log.'):
            file_modification_time = os.path.getmtime(file_path)
            file_date = datetime.fromtimestamp(file_modification_time)
            if file_date < cutoff_date:
                os.remove(file_path)
                print(f"Удален старый лог-файл: {filename}")


def configure_logging(log_directory='logs', log_level=logging.INFO):
    os.makedirs(log_directory, exist_ok=True)
    log_file_path = os.path.join(log_directory, 'app.log')

    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    # Настройка обработчика для файлов логов
    file_handler = TimedRotatingFileHandler(
        log_file_path,
        when="midnight",
        interval=1,
        backupCount=10,
        encoding='utf-8'
    )
    file_handler.setLevel(log_level)

    # Настройка обработчика для вывода в консоль
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)

    # Конфигурация structlog
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            reorder_event_dict,  # Перемещение 'marketplace' в начало
            add_color_and_importance,
            remove_empty_values,  # Удаляем пустые значения для консоли
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    # Форматирование для файлов
    file_processor = structlog.processors.JSONRenderer(serializer=json_serializer)
    file_formatter = structlog.stdlib.ProcessorFormatter(processor=file_processor)
    file_handler.setFormatter(file_formatter)

    # Форматирование для консоли
    console_processor = structlog.dev.ConsoleRenderer(colors=True)
    console_formatter = structlog.stdlib.ProcessorFormatter(processor=console_processor)
    console_handler.setFormatter(console_formatter)

    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)

    # Очистка старых логов
    cleanup_old_logs(log_directory)

    return structlog.get_logger()


# Создание глобального логгера
logger = configure_logging()

