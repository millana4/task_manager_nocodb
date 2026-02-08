import os
import logging
from datetime import datetime, timedelta
from typing import List, Dict
import pytz
from dotenv import load_dotenv
from telegram import Bot
from telegram.error import TelegramError
import requests
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import asyncio
import json

# Загрузка переменных окружения
load_dotenv()

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)


class NocoDBClient:
    def __init__(self):
        self.api_token = os.getenv('NOCODB_API_TOKEN')
        self.table_id = os.getenv('NOCODB_TABLE_ID')
        self.base_url = os.getenv('NOCODB_BASE_URL')
        self.view_id = os.getenv('NOCODB_VIEW_ID')

        if not all([self.api_token, self.table_id, self.base_url]):
            raise ValueError("Missing NocoDB configuration in .env file")

        self.headers = {
            'xc-token': self.api_token,
            'Content-Type': 'application/json'
        }

        # Для отладки
        logger.info(f"NocoDB config: table_id={self.table_id}, view_id={self.view_id}")

    def get_all_tasks(self) -> List[Dict]:
        try:
            url = f"{self.base_url}/tables/{self.table_id}/records"

            logger.info(f"Fetching tasks from URL: {url}")
            logger.info(f"Table ID: {self.table_id}, View ID: {self.view_id}")

            params = {
                'offset': 0,
                'limit': 1000,
                'where': '',
                'viewId': self.view_id,
            }

            response = requests.get(url, headers=self.headers, params=params)
            logger.info(f"Response status: {response.status_code}")

            if response.status_code == 200:
                data = response.json()
                logger.info(f"Successfully fetched {len(data.get('list', []))} tasks")

                # Преобразуем данные в нужный формат
                tasks = []
                for task in data.get('list', []):
                    # Извлекаем значения из полей
                    task_data = {}
                    for key, value in task.items():
                        if key not in ['Id', 'created_at', 'updated_at'] and not key.startswith('_'):
                            # Проверяем, является ли значение словарем с полем 'value'
                            if isinstance(value, dict) and 'value' in value:
                                task_data[key] = value['value']
                            else:
                                task_data[key] = value
                    tasks.append(task_data)

                logger.info(f"Processed {len(tasks)} tasks")
                if tasks:
                    logger.info(f"Sample task: {json.dumps(tasks[0], indent=2, ensure_ascii=False)}")

                return tasks
            else:
                logger.error(f"Failed to get tasks. Status: {response.status_code}, Response: {response.text}")
                return []

        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching tasks from NocoDB: {e}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error in get_all_tasks: {e}")
            return []


class TaskManager:
    """Менеджер для работы с задачами"""

    def __init__(self):
        self.nocodb = NocoDBClient()
        self.moscow_tz = pytz.timezone('Europe/Moscow')

    def get_tasks_for_date(self, target_date: datetime) -> List[Dict]:
        """Получить задачи на указанную дату"""
        all_tasks = self.nocodb.get_all_tasks()
        today_str = target_date.strftime('%Y-%m-%d')

        logger.info(f"Total tasks fetched: {len(all_tasks)}")
        logger.info(f"Looking for tasks with Date: {today_str}")

        tasks_for_today = []
        for task in all_tasks:
            # Преобразуем данные задачи
            task_done = self._parse_bool(task.get('Done'))
            task_date = task.get('Date')

            # Обрабатываем дату
            if task_date and isinstance(task_date, str):
                task_date = task_date[:10]  # Берем только дату

            logger.debug(f"Checking task: {task.get('Task')}, Date: {task_date}, Done: {task_done}")

            if not task_done and task_date and task_date == today_str:
                logger.info(f"Found task for today: {task.get('Task')}")
                tasks_for_today.append(task)

        logger.info(f"Found {len(tasks_for_today)} tasks for today")
        return tasks_for_today

    def get_upcoming_deadlines(self) -> List[Dict]:
        """Получить задачи с дедлайнами на ближайшие 3 дня"""
        all_tasks = self.nocodb.get_all_tasks()
        today = datetime.now(self.moscow_tz).date()

        logger.info(f"Looking for deadlines in next 3 days from {today}")

        upcoming_tasks = []
        for i in range(3):  # Сегодня, завтра, послезавтра
            target_date = today + timedelta(days=i)
            date_str = target_date.strftime('%Y-%m-%d')

            for task in all_tasks:
                task_done = self._parse_bool(task.get('Done'))
                if not task_done:
                    deadline = task.get('Deadline')
                    if deadline and isinstance(deadline, str):
                        deadline = deadline[:10]  # Берем только дату
                        if deadline == date_str:
                            logger.info(f"Found deadline task: {task.get('Task')} due {date_str}")
                            task['_days_until'] = i
                            upcoming_tasks.append(task)

        logger.info(f"Found {len(upcoming_tasks)} upcoming deadline tasks")
        return upcoming_tasks

    @staticmethod
    def _parse_bool(value) -> bool:
        """Парсим булево значение из разных форматов"""
        if isinstance(value, bool):
            return value
        elif isinstance(value, str):
            return value.lower() in ['true', '1', 'yes', 'y', 't']
        elif isinstance(value, (int, float)):
            return bool(value)
        return False

    @staticmethod
    def format_task(task: Dict) -> str:
        """Форматировать задачу для отображения"""
        parts = []

        # Название задачи
        if task_name := task.get('Task'):
            parts.append(f"• {task_name}")
        else:
            parts.append("• Без названия")

        # Курс
        if course := task.get('Course'):
            parts.append(f"  Курс: {course}")

        # Тип задачи
        if task_type := task.get('Type_task'):
            parts.append(f"  Тип: {task_type}")

        # Дедлайн
        if deadline := task.get('Deadline'):
            if isinstance(deadline, str):
                # Обрезаем время, если оно есть
                deadline_date = deadline[:10] if len(deadline) >= 10 else deadline
                parts.append(f"  Дедлайн: {deadline_date}")

        # Длительность
        if duration := task.get('Duration'):
            parts.append(f"  Длительность: {duration/3600} ч.")

        # Дней до дедлайна (только для уведомлений о дедлайнах)
        if days_until := task.get('_days_until'):
            days_text = {
                0: "сегодня",
                1: "завтра",
                2: "послезавтра"
            }.get(days_until, f"через {days_until} дней")
            parts.append(f"  Срок: {days_text}")

        return "\n".join(parts)


class TelegramNotifier:
    """Класс для отправки уведомлений в Telegram"""

    def __init__(self):
        self.bot_token = os.getenv('BOT_TOKEN')
        self.user_id = os.getenv('TELEGRAM_USER_ID')

        if not self.bot_token or not self.user_id:
            raise ValueError("Missing Telegram configuration in .env file")

        self.bot = Bot(token=self.bot_token)
        self.task_manager = TaskManager()
        self.loop = None

    def set_loop(self, loop):
        """Установить event loop для асинхронных операций"""
        self.loop = loop

    async def _send_message_async(self, text: str) -> bool:
        """Асинхронная отправка сообщения"""
        try:
            await self.bot.send_message(
                chat_id=self.user_id,
                text=text,
                parse_mode='HTML'
            )
            return True
        except TelegramError as e:
            logger.error(f"Error sending Telegram message: {e}")
            return False

    def send_message_sync(self, text: str) -> bool:
        """Синхронная обертка для отправки сообщения"""
        if not self.loop:
            logger.error("No event loop available")
            return False

        try:
            # Запускаем асинхронную функцию в синхронном контексте
            future = asyncio.run_coroutine_threadsafe(self._send_message_async(text), self.loop)
            future.result(timeout=30)  # Ждем результат 30 секунд
            return True
        except Exception as e:
            logger.error(f"Error in send_message_sync: {e}")
            return False

    def send_daily_tasks_sync(self):
        """Синхронная версия отправки ежедневных задач"""
        logger.info("Sending daily tasks notification")

        try:
            today = datetime.now(self.task_manager.moscow_tz)
            tasks = self.task_manager.get_tasks_for_date(today)

            if tasks:
                message_lines = ["<b>Анна, приветствую!</b>\nНа сегодня запланированы следующие задачи:\n"]

                for task in tasks:
                    formatted_task = self.task_manager.format_task(task)
                    message_lines.append(formatted_task)
                    message_lines.append("")  # Пустая строка между задачами

                message = "\n".join(message_lines).strip()
            else:
                message = "<b>Анна, приветствую!</b>\nНа сегодня запланированных задач нет."

            return self.send_message_sync(message)
        except Exception as e:
            logger.error(f"Error in send_daily_tasks_sync: {e}")
            return False

    def send_deadline_notification_sync(self):
        """Синхронная версия отправки уведомлений о дедлайнах"""
        logger.info("Sending deadline notification")

        try:
            upcoming_tasks = self.task_manager.get_upcoming_deadlines()

            if not upcoming_tasks:
                logger.info("No upcoming deadlines found")
                return True  # Ничего не отправляем, это нормально

            # Группируем задачи по дням
            tasks_by_day = {}
            for task in upcoming_tasks:
                days = task.get('_days_until', 0)
                if days not in tasks_by_day:
                    tasks_by_day[days] = []
                tasks_by_day[days].append(task)

            message_lines = ["<b>Внимание! Приближаются дедлайны:</b>\n"]

            for days in sorted(tasks_by_day.keys()):
                day_text = {
                    0: "<b>Сегодня:</b>",
                    1: "<b>Завтра:</b>",
                    2: "<b>Послезавтра:</b>"
                }.get(days, f"<b>Через {days} дней:</b>")

                message_lines.append(day_text)

                for task in tasks_by_day[days]:
                    # Убираем поле _days_until для форматирования
                    task_copy = task.copy()
                    task_copy.pop('_days_until', None)
                    formatted_task = self.task_manager.format_task(task_copy)
                    message_lines.append(formatted_task)

                message_lines.append("")  # Пустая строка между группами

            message = "\n".join(message_lines).strip()
            return self.send_message_sync(message)
        except Exception as e:
            logger.error(f"Error in send_deadline_notification_sync: {e}")
            return False


def create_scheduler(notifier: TelegramNotifier) -> BackgroundScheduler:
    """Создать и настроить планировщик"""
    scheduler = BackgroundScheduler(timezone=pytz.timezone('Europe/Moscow'))

    # Ежедневное уведомление в 9:00
    scheduler.add_job(
        notifier.send_daily_tasks_sync,
        trigger=CronTrigger(hour=9, minute=0),
        id='daily_tasks',
        name='Send daily tasks notification',
        replace_existing=True
    )

    # Уведомление о дедлайнах в 9:05
    scheduler.add_job(
        notifier.send_deadline_notification_sync,
        trigger=CronTrigger(hour=9, minute=5),
        id='deadlines',
        name='Send deadline notification',
        replace_existing=True
    )

    # Для тестирования - запуск сразу
    scheduler.add_job(
        notifier.send_daily_tasks_sync,
        trigger='date',
        run_date=datetime.now() + timedelta(seconds=5),
        id='test_daily',
        name='Test daily tasks'
    )

    scheduler.add_job(
        notifier.send_deadline_notification_sync,
        trigger='date',
        run_date=datetime.now() + timedelta(seconds=10),
        id='test_deadlines',
        name='Test deadlines'
    )

    return scheduler


async def main_async():
    """Основная асинхронная функция"""
    try:
        # Проверка конфигурации
        required_vars = [
            'BOT_TOKEN', 'TELEGRAM_USER_ID',
            'NOCODB_API_TOKEN', 'NOCODB_TABLE_ID'
        ]

        missing_vars = [var for var in required_vars if not os.getenv(var)]
        if missing_vars:
            raise ValueError(f"Missing environment variables: {', '.join(missing_vars)}")

        # Создаем notifier и устанавливаем event loop
        notifier = TelegramNotifier()
        notifier.set_loop(asyncio.get_running_loop())

        # Тестируем соединение с NocoDB
        logger.info("Testing NocoDB connection...")
        test_tasks = notifier.task_manager.nocodb.get_all_tasks()
        logger.info(f"Test successful! Found {len(test_tasks)} tasks")

        # Создаем и запускаем планировщик
        scheduler = create_scheduler(notifier)
        scheduler.start()
        logger.info("Bot scheduler started. Press Ctrl+C to exit.")

        # Бесконечный цикл
        try:
            while True:
                await asyncio.sleep(3600)  # Спим час
        except (KeyboardInterrupt, SystemExit):
            scheduler.shutdown()
            logger.info("Scheduler stopped")

    except Exception as e:
        logger.error(f"Failed to start bot: {e}")
        raise


def main():
    """Точка входа"""
    try:
        # Запускаем асинхронную основную функцию
        asyncio.run(main_async())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}")


if __name__ == '__main__':
    main()