import telebot
import logging
from datetime import datetime
import os

def create_telegram_logger(bot_token: str, chat_id: str):
    try:
        bot = telebot.TeleBot(bot_token)
    except Exception as e:
        print(f"[Telegram Logger] Не удалось инициализировать бота: {e}")
        return logging.getLogger("dummy")  # заглушка

    class TelegramHandler(logging.Handler):
        def emit(self, record):
            try:
                log_entry = self.format(record)
                script_name = os.path.basename(record.pathname)
                if script_name == 'main.py':
                    script_name = 'Проверка заказов'
                else:
                    script_name = ''
                formatted_message = f"({script_name}) [{datetime.now().strftime('%d.%m.%Y %H:%M')}] \n{log_entry}"

                max_message_length = 4096
                for i in range(0, len(formatted_message), max_message_length):
                    chunk = formatted_message[i:i + max_message_length]
                    bot.send_message(chat_id, chunk)

            except Exception as e:
                print(f"[Telegram Logger] Ошибка при отправке сообщения: {e}")

    telegram_logger = logging.getLogger(f"telegram_{chat_id}")
    telegram_logger.setLevel(logging.INFO)

    handler = TelegramHandler()
    handler.setFormatter(logging.Formatter('%(message)s'))
    telegram_logger.addHandler(handler)

    return telegram_logger
