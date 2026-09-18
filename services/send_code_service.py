# services/send_code_service.py
import smtplib
import asyncio
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Optional
from protocol import EMAIL_REGEX
logger = logging.getLogger(__name__)

class SendCodeService:
    def __init__(self, smtp_server: str, smtp_port: int, sender_email: str, sender_password: str):
        self.smtp_server = smtp_server
        self.smtp_port = smtp_port
        self.sender_email = sender_email
        self.sender_password = sender_password

    async def send_code(self, recipient_email: str, code: str) -> bool:
        if not EMAIL_REGEX.fullmatch(recipient_email):
            logger.warning(f"Некорректный email: {recipient_email}")
            return False
            
        """Отправляет код подтверждения на указанный email. Возвращает True при успехе."""
        subject = "Код подтверждения для входа в Aerisyn"
        body = f"Ваш код подтверждения: {code}\nКод действителен в течение 5 минут. Не делитесь им!"

        msg = MIMEMultipart()
        msg['From'] = self.sender_email
        msg['To'] = recipient_email
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'plain'))

        try:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, self._send_sync, msg)
            logger.info(f"Код отправлен на {recipient_email}")
            return True
        except Exception as e:
            logger.error(f"Ошибка отправки письма на {recipient_email}: {e}")
            return False

    def _send_sync(self, msg):
        with smtplib.SMTP(self.smtp_server, self.smtp_port, timeout=10) as server:
            server.starttls()
            server.login(self.sender_email, self.sender_password)
            server.send_message(msg)