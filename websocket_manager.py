"""
Модуль для управления WebSocket подключениями
"""
import logging
import asyncio
import json
import traceback
from typing import Dict, Callable, Optional, List, Set
import websockets
from websockets.exceptions import ConnectionClosed, InvalidStatusCode
from datetime import datetime, timedelta
import time

logger = logging.getLogger(__name__)

class WebSocketManager:
    def __init__(self, client=None, cache_timeout: float = 1.0) -> None:
        """
        Инициализация менеджера WebSocket
        
        Args:
            client: Клиент Binance API (опционально)
            cache_timeout: Время кэширования данных стакана в секундах
        """
        self.client = client
        self.connections: Dict[str, websockets.WebSocketClientProtocol] = {}
        self.callbacks: Dict[str, Callable] = {}
        self.is_running = False
        self.reconnect_delay = 2  # Уменьшаем задержку перед повторным подключением
        self.max_reconnect_attempts = 5  # Увеличиваем количество попыток
        self.ping_interval = 15  # Уменьшаем интервал отправки ping
        self.last_message_time: Dict[str, datetime] = {}
        self.message_timeout = 30  # Уменьшаем таймаут получения сообщений
        self.ws_url = "wss://stream.binance.com:9443/ws"
        self.orderbook_cache: Dict[str, Dict] = {}
        self.cache_timeout = cache_timeout
        self.last_update: Dict[str, float] = {}
        self.subscribed_symbols: Set[str] = set()
        self.reconnect_tasks: Dict[str, asyncio.Task] = {}
        
    async def start(self) -> None:
        """Запуск WebSocket менеджера"""
        try:
            self.is_running = True
            asyncio.create_task(self._check_connections())
            logger.info("WebSocket менеджер запущен")
            
        except Exception as e:
            logger.error(f"Ошибка при запуске WebSocket менеджера: {e}")
            logger.error(traceback.format_exc())
            raise
            
    async def stop(self) -> None:
        """Остановка WebSocket менеджера"""
        try:
            self.is_running = False
            
            # Закрываем все соединения
            for stream_name, ws in self.connections.items():
                try:
                    await ws.close()
                    logger.info(f"Соединение {stream_name} закрыто")
                except Exception as e:
                    logger.error(f"Ошибка при закрытии соединения {stream_name}: {e}")
                    
            self.connections.clear()
            self.callbacks.clear()
            logger.info("WebSocket менеджер остановлен")
            
        except Exception as e:
            logger.error(f"Ошибка при остановке WebSocket менеджера: {e}")
            logger.error(traceback.format_exc())
            
    async def _check_connections(self) -> None:
        """Проверка состояния соединений"""
        while self.is_running:
            try:
                current_time = datetime.now()
                
                # Проверяем каждое соединение
                for stream_name in list(self.connections.keys()):
                    ws = self.connections.get(stream_name)
                    if not ws or ws.closed:
                        await self._resubscribe(stream_name)
                        continue
                        
                    # Проверяем таймаут сообщений
                    last_msg_time = self.last_message_time.get(stream_name)
                    if last_msg_time and (current_time - last_msg_time).seconds > self.message_timeout:
                        logger.warning(f"Таймаут сообщений для {stream_name}")
                        await self._resubscribe(stream_name)
                        continue
                        
                    # Отправляем ping
                    try:
                        await ws.ping()
                    except Exception as e:
                        logger.error(f"Ошибка ping для {stream_name}: {e}")
                        await self._resubscribe(stream_name)
                        
                await asyncio.sleep(self.ping_interval)
                
            except Exception as e:
                logger.error(f"Ошибка при проверке соединений: {e}")
                logger.error(traceback.format_exc())
                await asyncio.sleep(self.reconnect_delay)
                
    async def _resubscribe(self, stream_name: str, attempt: int = 0) -> None:
        """
        Переподключение к потоку
        
        Args:
            stream_name: Имя потока
            attempt: Номер попытки
        """
        try:
            if attempt >= self.max_reconnect_attempts:
                logger.error(f"Достигнуто максимальное количество попыток переподключения к {stream_name}")
                return
                
            logger.info(f"Переподключение к {stream_name} (попытка {attempt + 1})")
            
            # Отменяем предыдущую задачу переподключения, если она существует
            if stream_name in self.reconnect_tasks:
                self.reconnect_tasks[stream_name].cancel()
                try:
                    await self.reconnect_tasks[stream_name]
                except asyncio.CancelledError:
                    pass
            
            # Закрываем старое соединение
            if stream_name in self.connections:
                try:
                    await self.connections[stream_name].close()
                except Exception as e:
                    logger.error(f"Ошибка при закрытии соединения {stream_name}: {e}")
                    
            # Создаем новое соединение
            await self._subscribe(stream_name)
            
        except Exception as e:
            logger.error(f"Ошибка при переподключении к {stream_name}: {e}")
            logger.error(traceback.format_exc())
            # Создаем новую задачу для повторной попытки
            self.reconnect_tasks[stream_name] = asyncio.create_task(
                self._resubscribe(stream_name, attempt + 1)
            )
            
    async def _subscribe(self, stream_name: str) -> None:
        """
        Подписка на поток
        
        Args:
            stream_name: Имя потока
        """
        try:
            ws = await websockets.connect(f"{self.ws_url}/{stream_name}")
            
            self.connections[stream_name] = ws
            self.last_message_time[stream_name] = datetime.now()
            
            # Запускаем обработку сообщений
            asyncio.create_task(self._handle_messages(stream_name, ws))
            logger.info(f"Подписка на {stream_name} установлена")
            
        except Exception as e:
            logger.error(f"Ошибка при подписке на {stream_name}: {e}")
            logger.error(traceback.format_exc())
            raise
            
    async def _handle_messages(self, stream_name: str, ws: websockets.WebSocketClientProtocol) -> None:
        """
        Обработка сообщений из потока
        
        Args:
            stream_name: Имя потока
            ws: WebSocket соединение
        """
        try:
            while self.is_running and not ws.closed:
                try:
                    message = await ws.recv()
                    self.last_message_time[stream_name] = datetime.now()
                    
                    # Обработка сообщения
                    if stream_name in self.callbacks:
                        data = json.loads(message)
                        await self.callbacks[stream_name](data)
                        
                    # Обновляем кэш
                    symbol = stream_name.split('@')[0].upper()
                    self.orderbook_cache[symbol] = data
                    self.last_update[symbol] = time.time()
                        
                except ConnectionClosed:
                    logger.warning(f"Соединение {stream_name} закрыто")
                    break
                except json.JSONDecodeError as e:
                    logger.error(f"Ошибка декодирования JSON для {stream_name}: {e}")
                    continue
                except Exception as e:
                    logger.error(f"Ошибка при обработке сообщения из {stream_name}: {e}")
                    logger.error(traceback.format_exc())
                    continue
                    
        except Exception as e:
            logger.error(f"Ошибка в обработчике сообщений {stream_name}: {e}")
            logger.error(traceback.format_exc())
        finally:
            # Переподключаемся при разрыве соединения
            if self.is_running:
                await self._resubscribe(stream_name)
                
    async def subscribe_depth(self, symbol: str, callback: Callable) -> None:
        """
        Подписка на обновления стакана
        
        Args:
            symbol: Торговая пара
            callback: Функция обработки данных
        """
        stream_name = f"{symbol.lower()}@depth@100ms"
        self.callbacks[stream_name] = callback
        await self._subscribe(stream_name)
        
    async def subscribe_trades(self, symbol: str, callback: Callable) -> None:
        """
        Подписка на обновления сделок
        
        Args:
            symbol: Торговая пара
            callback: Функция обработки данных
        """
        stream_name = f"{symbol.lower()}@trade"
        self.callbacks[stream_name] = callback
        await self._subscribe(stream_name)
        
    async def subscribe_kline(self, symbol: str, interval: str, callback: Callable) -> None:
        """
        Подписка на обновления свечей
        
        Args:
            symbol: Торговая пара
            interval: Интервал свечей
            callback: Функция обработки данных
        """
        stream_name = f"{symbol.lower()}@kline_{interval}"
        self.callbacks[stream_name] = callback
        await self._subscribe(stream_name)
        
    async def subscribe_orderbook(self, symbol: str) -> None:
        """
        Подписка на обновления стакана для символа
        
        Args:
            symbol: Торговая пара
        """
        if symbol in self.subscribed_symbols:
            return

        try:
            stream_name = f"{symbol.lower()}@depth@100ms"
            ws_url = f"{self.ws_url}/{stream_name}"
            
            # Отменяем предыдущую задачу переподключения, если она существует
            if symbol in self.reconnect_tasks:
                self.reconnect_tasks[symbol].cancel()
                try:
                    await self.reconnect_tasks[symbol]
                except asyncio.CancelledError:
                    pass
            
            ws = await websockets.connect(ws_url)
            self.connections[symbol] = ws
            self.subscribed_symbols.add(symbol)
            
            # Запускаем обработку сообщений
            asyncio.create_task(self._handle_messages(stream_name, ws))
            logger.info(f"Подписка на стакан для {symbol} успешно создана")
            
        except Exception as e:
            logger.error(f"Ошибка при подписке на стакан для {symbol}: {e}")
            logger.error(traceback.format_exc())
            raise

    async def get_orderbook(self, symbol: str) -> Optional[Dict]:
        """
        Получение данных стакана
        
        Args:
            symbol: Торговая пара
            
        Returns:
            Dict: Данные стакана или None
        """
        try:
            # Проверяем кэш
            if symbol in self.orderbook_cache:
                last_update = self.last_update.get(symbol, 0)
                if time.time() - last_update < self.cache_timeout:
                    return self.orderbook_cache[symbol]
            
            # Если WebSocket недоступен, используем REST API
            if not self.client:
                logger.warning("REST клиент не инициализирован")
                return None
                
            # Получаем данные через REST API
            orderbook = await self.client.get_orderbook_ticker(symbol=symbol)
            
            # Обновляем кэш
            if orderbook:
                self.orderbook_cache[symbol] = orderbook
                self.last_update[symbol] = time.time()
            
            return orderbook
            
        except Exception as e:
            logger.error(f"Ошибка при получении стакана для {symbol}: {e}")
            logger.error(traceback.format_exc())
            return None

    async def _unsubscribe(self, stream_name: str) -> None:
        """
        Отписка от потока
        
        Args:
            stream_name: Имя потока
        """
        try:
            if stream_name in self.connections:
                ws = self.connections[stream_name]
                await ws.close()
                del self.connections[stream_name]
                
            if stream_name in self.callbacks:
                del self.callbacks[stream_name]
                
            if stream_name in self.last_message_time:
                del self.last_message_time[stream_name]
                
            logger.info(f"Отписка от {stream_name} выполнена")
            
        except Exception as e:
            logger.error(f"Ошибка при отписке от {stream_name}: {e}")
            logger.error(traceback.format_exc()) 