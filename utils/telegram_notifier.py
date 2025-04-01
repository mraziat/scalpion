"""
Модуль для отправки уведомлений в Telegram
"""
import logging
import asyncio
import json
import traceback
from typing import Dict, Callable, Optional, List
from aiogram import Bot, Dispatcher
from aiogram.enums import ParseMode
import traceback
import os
from dotenv import load_dotenv
from datetime import timedelta
from aiohttp import ClientTimeout

logger = logging.getLogger(__name__)

class TelegramNotifier:
    def __init__(self, config: dict):
        self.token = config.get('telegram_token')
        self.chat_id = config.get('telegram_chat_id')
        self.bot: Optional[Bot] = None
        self.max_retries = 5  # Увеличиваем количество попыток
        self.retry_delay = 5  # Увеличиваем задержку между попытками
        self.message_delay = 2  # Увеличиваем задержку между сообщениями
        self.timeout = ClientTimeout(total=30)  # Таймаут для запросов
        self.dp = Dispatcher()
        self.message_queue = asyncio.Queue()
        self.is_running = False
        self.trades_history: List[Dict] = []
        self.last_report_time = None
        self.report_interval = timedelta(hours=4)
        self.last_message_time = 0  # время последнего отправленного сообщения
        self._init_bot()
        
        logger.info("Telegram уведомления инициализированы")
        
    def _init_bot(self):
        """Инициализация бота"""
        try:
            if not self.token or not self.chat_id:
                logger.error("Отсутствуют настройки Telegram")
                return
            self.bot = Bot(token=self.token, timeout=self.timeout)
        except Exception as e:
            logger.error(f"Ошибка инициализации Telegram бота: {str(e)}")

    async def start(self) -> None:
        """Запуск обработчика сообщений"""
        try:
            self.is_running = True
            asyncio.create_task(self._process_message_queue())
            await self.notify_start()
            logger.info("Обработчик Telegram сообщений запущен")
            
        except Exception as e:
            logger.error(f"Ошибка при запуске обработчика Telegram: {e}")
            logger.error(traceback.format_exc())
            raise
            
    async def stop(self) -> None:
        """Остановка обработчика сообщений"""
        try:
            self.is_running = False
            await self.notify_stop()
            
            # Очищаем очередь сообщений
            while not self.message_queue.empty():
                try:
                    self.message_queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                except Exception as e:
                    logger.error(f"Ошибка при очистке очереди: {e}")
            
            # Ждем завершения обработки оставшихся сообщений
            if not self.message_queue.empty():
                logger.info("Ожидание обработки оставшихся сообщений...")
                await asyncio.sleep(5)
                
            await self.bot.session.close()
            logger.info("Обработчик Telegram сообщений остановлен")
            
        except Exception as e:
            logger.error(f"Ошибка при остановке обработчика Telegram: {e}")
            logger.error(traceback.format_exc())
            
    async def _process_message_queue(self) -> None:
        """Обработка очереди сообщений"""
        while self.is_running:
            try:
                if not self.message_queue.empty():
                    try:
                        message = await self.message_queue.get()
                        await self._send_message_with_retry(message)
                    except asyncio.CancelledError:
                        raise
                    except Exception as e:
                        logger.error(f"Ошибка при обработке сообщения из очереди: {e}")
                        logger.error(traceback.format_exc())
                await asyncio.sleep(0.1)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Ошибка в цикле обработки очереди сообщений: {e}")
                logger.error(traceback.format_exc())
                await asyncio.sleep(self.retry_delay)
                
    async def _send_message_with_retry(self, message: str, retries: int = 0) -> None:
        """
        Отправка сообщения с повторными попытками
        
        Args:
            message: Текст сообщения
            retries: Текущее количество попыток
        """
        try:
            # Проверяем, прошло ли достаточно времени с последнего сообщения
            current_time = asyncio.get_event_loop().time()
            if current_time - self.last_message_time < self.message_delay:
                await asyncio.sleep(self.message_delay)
            
            await self.bot.send_message(
                chat_id=self.chat_id,
                text=message,
                parse_mode=ParseMode.HTML
            )
            self.last_message_time = asyncio.get_event_loop().time()
            
        except asyncio.TimeoutError:
            logger.error("Таймаут при отправке сообщения в Telegram")
            if retries < self.max_retries:
                await asyncio.sleep(self.retry_delay * (retries + 1))
                await self._send_message_with_retry(message, retries + 1)
            else:
                logger.error(f"Не удалось отправить сообщение после {self.max_retries} попыток")
        except Exception as e:
            logger.error(f"Ошибка при отправке сообщения в Telegram: {e}")
            if retries < self.max_retries:
                await asyncio.sleep(self.retry_delay * (retries + 1))
                await self._send_message_with_retry(message, retries + 1)
            else:
                logger.error(f"Не удалось отправить сообщение после {self.max_retries} попыток")
                logger.error(traceback.format_exc())
                
    async def notify(self, message: str, max_retries: Optional[int] = None) -> bool:
        """
        Отправляет уведомление в Telegram с повторными попытками
        
        Args:
            message: Текст сообщения
            max_retries: Максимальное количество попыток (по умолчанию self.max_retries)
            
        Returns:
            bool: True если сообщение отправлено успешно, False в противном случае
        """
        if not self.bot or not self.chat_id:
            logger.error("Telegram бот не инициализирован")
            return False

        try:
            await self.message_queue.put(message)
            return True
        except Exception as e:
            logger.error(f"Ошибка при добавлении сообщения в очередь: {e}")
            return False
        
    async def notify_start(self) -> None:
        """Уведомление о запуске бота"""
        await self.notify("🟢 Торговый бот запущен")
        
    async def notify_stop(self) -> None:
        """Уведомление об остановке бота"""
        await self.notify("🔴 Торговый бот остановлен")
        
    async def notify_error(self, error: str) -> None:
        """
        Уведомление об ошибке
        
        Args:
            error: Текст ошибки
        """
        message = f"❌ Ошибка: {error}"
        await self.notify(message)
        
    async def notify_trade(self, symbol: str, side: str, quantity: float, price: float) -> None:
        """
        Уведомление о сделке
        
        Args:
            symbol: Торговая пара
            side: Сторона сделки (buy/sell)
            quantity: Количество
            price: Цена
        """
        emoji = "🟢" if side.lower() == "buy" else "🔴"
        message = f"{emoji} {symbol}: {side.upper()} {quantity:.8f} @ {price:.8f}"
        await self.notify(message)
        
    def _format_volume(self, volume: float) -> str:
        """
        Форматирует объем в читаемый вид (К, М)
        
        Args:
            volume: Объем в USDT
            
        Returns:
            str: Отформатированный объем
        """
        if volume >= 1_000_000:
            return f"{volume/1_000_000:.2f}M"
        elif volume >= 1_000:
            return f"{volume/1_000:.2f}K"
        return f"{volume:.2f}"
        
    async def notify_liquidity_cluster(self, symbol: str, price: float, volume: float) -> None:
        """
        Уведомление о кластере ликвидности
        
        Args:
            symbol: Торговая пара
            price: Цена кластера
            volume: Объем в кластере
        """
        formatted_volume = self._format_volume(volume)
        message = f"💧 {symbol}: Обнаружен кластер ликвидности\nЦена: {price:.8f}\nОбъем: {formatted_volume} USDT"
        await self.notify(message)
        
    async def notify_position_update(self, symbol: str, position_size: float, pnl: float) -> None:
        """
        Уведомление об обновлении позиции
        
        Args:
            symbol: Торговая пара
            position_size: Размер позиции
            pnl: Прибыль/убыток
        """
        emoji = "📈" if pnl >= 0 else "📉"
        formatted_pnl = self._format_volume(pnl)
        message = f"{emoji} {symbol}: Позиция {position_size:.8f}\nP&L: {formatted_pnl} USDT"
        await self.notify(message) 