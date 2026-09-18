# bot_callback.py
import asyncio
import logging
from bot_lib import BotClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

TOKEN = "bHHefLfZz4TfqbFI0qD3CJ5iC0eDCdC_7tFeC9VVcRc="
BOT_ID = 2  # ID бота

bot = BotClient(TOKEN, BOT_ID)


@bot.on_message
async def handle_message(event):
    content = event.get("content")
    entity_id = event.get("entity_id")
    if not content or not entity_id:
        return

    if content.strip().lower() == "/start":
        reply_markup = {
            "inline_keyboard": [
                [
                    {"text": "Кнопка 1", "callback_data": "button1"},
                    {"text": "Кнопка 2", "callback_data": "button2"},
                ]
            ]
        }
        await bot.send_message(
            entity_id,
            "Привет! Нажмите одну из кнопок:",
            reply_markup=reply_markup,
        )
        logger.info(f"Отправлена клавиатура в чат {entity_id}")


@bot.on_callback_query
async def handle_callback(event):
    # Генерируем callback_id, если сервер его не прислал
    callback_id = event.get("id") or f"{event.get('entity_id')}_{event.get('message_id')}"
    data = event.get("callback_data")
    entity_id = event.get("entity_id")
    message_id = event.get("message_id")

    logger.info(f"Обработка callback: id={callback_id}, data={data}, entity={entity_id}, msg={message_id}")

    if not data or not entity_id or not message_id:
        logger.warning("Недостаточно данных в callback-событии")
        return

    # Подтверждаем получение callback (убираем «часики»)
    await bot.answer_callback_query(
        callback_id,
        text=f"Вы выбрали {data}",
        show_alert=False,
    )

    # Редактируем исходное сообщение: новый текст и убираем клавиатуру
    new_text = f"✅ Вы нажали кнопку «{data}». Клавиатура удалена."
    await bot.edit_message(
        entity_id,
        message_id,
        new_text,
        reply_markup=None,
    )
    logger.info(f"Сообщение {message_id} отредактировано")


if __name__ == "__main__":
    asyncio.run(bot.run())