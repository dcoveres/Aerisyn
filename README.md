#Aerisyn

Aerisyn — это защищённый мессенджер с сквозным шифрованием, поддержкой личных чатов, групп, каналов, ботов, файлов, стикеров, опросов, реакций и многого другого. Проект включает серверную часть на Python, веб-интерфейс и консольный клиент.

✨ Возможности

· Аутентификация по email: вход с помощью одноразового кода, отправляемого на почту.
· Сквозное шифрование: X25519 для обмена ключами, EdDSA для подписей, ChaCha20-Poly1305 для симметричного шифрования, HKDF для деривации ключей.
· Сущности:
  · Личные чаты (1-на-1)
  · Группы
  · Каналы
· Роли и права: владелец, администраторы, участники, бан-листы.
· Сообщения:
  · Текстовые сообщения
  · Файлы (изображения, видео, аудио, документы)
  · Стикеры
  · Опросы (с голосованием и закрытием)
  · Реакции (эмодзи)
  · Ответы на сообщения (reply)
  · Редактирование сообщений (для ботов)
· Инлайн-кнопки для ботов (callback-запросы).
· Боты: создание, управление токенами, приглашение в сущности.
· Аналитика для групп и каналов (количество участников, сообщений, реакций, график активности).
· Чёрный список: блокировка пользователей и ботов.
· Анти-спам: ограничение на первое сообщение незнакомым пользователям (аналог Telegram).
· Управление сессиями: просмотр активных сессий и их завершение.
· Приватные сущности: заявки на вступление и их одобрение.
· Поиск по пользователям, ботам и публичным сущностям.
· Веб-клиент (адаптивный интерфейс) и CLI-клиент.

🏗 Архитектура

Проект состоит из нескольких компонентов:

· gateway.py — WebSocket-сервер (WSS) с SSL, обрабатывает подключения, аутентификацию, команды и файловые потоки.
· main.py — веб-сервер на Quart, предоставляет веб-интерфейс и REST API для браузера.
· cli.py — консольный клиент для взаимодействия с сервером.
· Сервисы (services/) — бизнес-логика:
  · auth_service — аутентификация, handshake, проверка кодов.
  · user_service — профили пользователей.
  · entity_service — управление чатами, группами, каналами.
  · message_service — отправка и получение сообщений.
  · file_service — загрузка и скачивание файлов.
  · bot_service — создание и управление ботами.
  · reaction_service — реакции на сообщения.
  · sticker_service — стикеры.
  · poll_service — опросы.
  · analytics_service — сбор статистики.
  · spam_service — анти-спам ограничения.
  · command_service — маршрутизация команд от клиентов.
· Хранилища:
  · PostgreSQL / SQLite (через SQLAlchemy) — основное хранилище данных.
  · Redis — сессии, счётчики, временные данные, кэш.
· Протокол (protocol.py) — константы, функции шифрования, сериализации, валидации.

📦 Требования

· Python 3.8+
· Redis
· PostgreSQL (опционально, можно использовать SQLite для разработки)
· SMTP-сервер для отправки кодов подтверждения

🚀 Установка и запуск

1. Клонирование репозитория

```bash
git clone https://github.com/yourusername/aerisyn.git
cd aerisyn
```

2. Установка зависимостей

```bash
python -m venv venv
source venv/bin/activate  # Linux/macOS
# или
venv\Scripts\activate  # Windows
pip install -r requirements.txt
```

3. Настройка окружения

Создайте файл .env в корне проекта:

```env
# База данных (пример для PostgreSQL)
DATABASE_URL=postgresql+asyncpg://user:password@localhost/aerisyn
# Для SQLite:
# DATABASE_URL=sqlite+aiosqlite:///./messenger.db

# Redis
REDIS_URL=redis://localhost:6379/0

# SMTP для отправки кодов
SMTP_SERVER=smtp.gmail.com
SMTP_PORT=587
SMTP_EMAIL=your_email@gmail.com
SMTP_PASSWORD=your_app_password
```

4. Миграции базы данных

```bash
alembic upgrade head
```

5. SSL-сертификаты

Для работы WebSocket-сервера требуется SSL. Сгенерируйте самоподписанный сертификат для разработки:

```bash
mkdir certs
openssl req -x509 -newkey rsa:4096 -keyout certs/key.pem -out certs/cert.pem -days 365 -nodes
```

6. Запуск серверов

Запустите WebSocket-сервер (gateway):

```bash
python gateway.py
```

В другом терминале запустите веб-сервер:

```bash
python main.py
```

Веб-интерфейс будет доступен по адресу http://localhost:5000.

7. CLI-клиент (опционально)

```bash
python cli.py
```

Введите email, запросите код, введите его и общайтесь.

🖥 Использование

Веб-интерфейс

1. Откройте http://localhost:5000.
2. Введите email и запросите код.
3. Введите полученный код для входа.
4. Создавайте чаты, группы, каналы, приглашайте ботов, отправляйте файлы и стикеры.

CLI-клиент

После запуска cli.py доступны команды:

· /create <type> <name> [target] — создать сущность (chat, group, channel)
· /send <entity_id> <text> — отправить сообщение
· /sendfile <entity_id> <filepath> — отправить файл
· /get <entity_id> [limit] — получить историю
· /setusername <username> — установить юзернейм
· /create_bot <name> [@username] — создать бота
· /invite_bot <entity_id> <bot_identifier> — пригласить бота
· и другие (см. /help внутри клиента).

🔐 Безопасность

· Все управляющие сообщения шифруются с использованием сессионного ключа, полученного через X25519 + HKDF.
· Каждая команда подписывается EdDSA (для пользователей) или проверяется токен (для ботов).
· Защита от replay-атак (уникальные request_id).
· Ограничение частоты команд (rate limit).
· Анти-спам: пользователь, которого заблокировали без ответа, временно не может первым писать незнакомцам.
· Коды подтверждения деактивируются при отправке в чат (защита от утечки).

📄 Лицензия

Проект распространяется под лицензией Apache License 2.0. Подробности в файле LICENSE.

```
Copyright 2025 Aerisyn

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```

🤝 Вклад

Приветствуются пул-реквесты. Для крупных изменений сначала откройте issue для обсуждения.

---

Aerisyn — безопасное общение без компромиссов.
