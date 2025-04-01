"""
Модуль для анализа рынка
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional, Any
import logging
from datetime import datetime, timedelta
from binance.client import Client
from utils.telegram_notifier import TelegramNotifier
import asyncio
import aiohttp
import json
import traceback
import time
from decimal import Decimal
import decimal

logger = logging.getLogger(__name__)

class MarketAnalyzer:
    def __init__(self, config: Dict[str, Any], binance_client: Client, websocket_manager=None) -> None:
        """
        Инициализация анализатора рынка
        
        Args:
            config: Конфигурация
            binance_client: Клиент Binance
            websocket_manager: Менеджер WebSocket
        """
        self.config = config
        self.binance_client = binance_client
        self.websocket_manager = websocket_manager
        self.orderbooks: Dict[str, pd.DataFrame] = {}
        self.metrics: Dict[str, Dict] = {}
        self.liquidity_clusters: Dict[str, List[Dict]] = {}
        self.volume_profiles: Dict[str, pd.DataFrame] = {}
        
        # Параметры для анализа стакана
        self.min_order_volume = float(config.get('min_order_volume', 1000))  # Минимальный объем ордера в USDT
        self.min_cluster_volume = float(config.get('min_cluster_volume', 150000))  # Минимальный объем кластера в USDT
        self.max_cluster_volume = float(config.get('max_cluster_volume', 1000000))  # Максимальный объем кластера в USDT
        self.price_std_threshold = float(config.get('price_std_threshold', 0.0005))  # Порог стандартного отклонения цены
        self.spread_threshold = float(config.get('spread_threshold', 0.001))  # Порог спреда
        self.min_cluster_size = int(config.get('min_cluster_size', 3))  # Минимальное количество ордеров в кластере
        self.max_cluster_size = int(config.get('max_cluster_size', 50))  # Максимальное количество ордеров в кластере
        self.update_interval = 1  # Интервал обновления в секундах
        self.last_notification_time = {}  # Время последнего уведомления для каждой пары
        self.notification_cooldown = 60  # Задержка между уведомлениями в секундах
        
        # Параметры для анализа стакана
        self.orderbook_metrics: Dict[str, Dict] = {}
        self.liquidity_clusters: Dict[str, List[Dict]] = {}
        self.last_update: Dict[str, datetime] = {}
        self.last_cluster_notification: Dict[str, Dict] = {}  # Кэш последних уведомлений
        
        # Параметры для анализа уровней
        self.telegram_token = config.get('telegram_token')
        self.telegram_chat_id = config.get('telegram_chat_id')
        self.telegram_notifier = None  # Бот будет инициализирован асинхронно
        
        # Параметры анализа
        self.min_level_touches = 2  # Минимальное количество касаний уровня
        self.level_tolerance = 0.002  # Допустимое отклонение от уровня (0.2%)
        self.volume_ma_period = config.get('volume_ma_period', 20)
        self.min_level_strength = config.get('min_level_strength', 2.0)
        self.price_precision = config.get('price_precision', 8)
        self.quantity_precision = config.get('quantity_precision', 8)
        
        # Кэш для оптимизации
        self.cache = {}
        self.cache_timeout = 60  # секунды
        
        self.price_step = float(config.get('price_step', 0.1))  # Шаг цены для группировки ордеров
        self.volume_threshold = float(config.get('volume_threshold', 1.0))  # Порог объема для значимого кластера
        self.price_threshold = float(config.get('price_threshold', 0.1))  # Порог цены для значимого кластера
        self.last_metrics_update = datetime.now()
        self.metrics_update_interval = timedelta(seconds=1)
        self.last_cluster_save = datetime.now()
        self.cluster_save_interval = timedelta(minutes=5)
        self.clusters = []
        self.previous_clusters = {}  # Для отслеживания изменений кластеров
        self.sent_notifications = {}
        self.max_notifications = 1000  # Ограничение размера кэша
        self.performance_threshold = 1.0  # Порог для предупреждений о производительности
        self.orderbook_cache = {}
        self.rate_limiter = RateLimiter(max_requests=1200, period=60)  # 1200 запросов в минуту
        
        # Инициализируем Telegram бота
        if self.telegram_token and self.telegram_chat_id:
            try:
                self.telegram_notifier = TelegramNotifier(config)
                logger.info("Telegram бот успешно инициализирован")
            except Exception as e:
                logger.error(f"Ошибка при инициализации Telegram бота: {str(e)}")
                logger.error(traceback.format_exc())
                self.telegram_notifier = None
        else:
            logger.warning("Не указаны токен или chat_id для Telegram бота")
        
    async def initialize(self):
        """Асинхронная инициализация компонентов"""
        try:
            if self.telegram_token and self.telegram_chat_id:
                self.telegram_notifier = TelegramNotifier(self.config)
                logger.info("Telegram бот успешно инициализирован")
            else:
                logger.warning("Не указаны токен или chat_id для Telegram бота")
        except Exception as e:
            logger.error(f"Ошибка при инициализации Telegram бота: {e}")
            logger.error(traceback.format_exc())
        
    async def get_orderbook(self, symbol: str, limit: int = 5000) -> Optional[Dict]:
        """Получение данных стакана с использованием WebSocket и кэширования"""
        try:
            current_time = time.time()
            
            # Проверяем кэш
            if symbol in self.orderbook_cache:
                last_update = self.last_update.get(symbol, 0)
                if current_time - last_update < self.cache_timeout:
                    return self.orderbook_cache[symbol]
            
            # Пробуем получить данные через WebSocket
            if self.websocket_manager:
                orderbook_data = await self.websocket_manager.get_orderbook(symbol)
                if orderbook_data:
                    self.orderbook_cache[symbol] = orderbook_data
                    self.last_update[symbol] = current_time
                    return orderbook_data
            
            # Если WebSocket недоступен, используем REST API с rate limiting
            try:
                depth = self.binance_client.get_order_book(symbol=symbol, limit=limit)
                
                if not depth or 'bids' not in depth or 'asks' not in depth:
                    logger.warning(f"Получены некорректные данные стакана для {symbol}")
                    return None
                
                # Преобразуем данные в DataFrame
                bids_df = pd.DataFrame(depth['bids'], columns=['price', 'quantity'], dtype=float)
                asks_df = pd.DataFrame(depth['asks'], columns=['price', 'quantity'], dtype=float)
                
                if bids_df.empty or asks_df.empty:
                    logger.warning(f"Пустой стакан для {symbol}")
                    return None
                
                # Валидация данных
                bids_df = self._validate_orderbook_data(bids_df)
                asks_df = self._validate_orderbook_data(asks_df)
                
                if bids_df.empty or asks_df.empty:
                    logger.warning(f"Некорректные данные в стакане для {symbol}")
                    return None
                
                # Обновляем кэш
                self.orderbook_cache[symbol] = {
                    'bids': bids_df.values.tolist(),
                    'asks': asks_df.values.tolist()
                }
                self.last_update[symbol] = current_time
                
                return self.orderbook_cache[symbol]
                
            except Exception as e:
                logger.error(f"Ошибка при получении данных через REST API для {symbol}: {str(e)}")
                return None
            
        except Exception as e:
            logger.error(f"Ошибка при получении стакана для {symbol}: {str(e)}")
            return None

    def _validate_orderbook_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Валидация данных стакана"""
        # Удаляем строки с некорректными значениями
        df = df[
            (df['price'] > 0) & 
            (df['quantity'] > 0) & 
            (df['price'].notna()) & 
            (df['quantity'].notna())
        ]
        return df

    def _update_metrics(self, symbol: str) -> None:
        """
        Обновление метрик стакана
        
        Args:
            symbol: Торговая пара
        """
        try:
            if symbol not in self.orderbooks:
                return
                
            orderbook = self.orderbooks[symbol]
            
            # Рассчитываем дисбаланс
            bids = orderbook[orderbook['side'] == 'bid']
            asks = orderbook[orderbook['side'] == 'ask']
            
            bid_volume = bids['quantity'].sum()
            ask_volume = asks['quantity'].sum()
            imbalance = (bid_volume - ask_volume) / (bid_volume + ask_volume)
            
            # Рассчитываем спред
            best_bid = bids['price'].max()
            best_ask = asks['price'].min()
            spread = (best_ask - best_bid) / best_bid
            
            # Рассчитываем VWAP
            vwap = (orderbook['price'] * orderbook['quantity']).sum() / orderbook['quantity'].sum()
            
            self.orderbook_metrics[symbol] = {
                'imbalance': imbalance,
                'spread': spread,
                'bid_volume': bid_volume,
                'ask_volume': ask_volume,
                'best_bid': best_bid,
                'best_ask': best_ask,
                'vwap': vwap,
                'timestamp': datetime.now()
            }
            
        except Exception as e:
            logger.error(f"Ошибка при обновлении метрик стакана для {symbol}: {e}")
            
    async def _send_telegram_message(self, message: str) -> bool:
        """
        Отправка сообщения в Telegram
        
        Args:
            message: Текст сообщения
            
        Returns:
            bool: Успешность отправки
        """
        if self.telegram_notifier:
            return await self.telegram_notifier.notify(message)
        return False

    async def _update_liquidity_clusters(self, symbol: str) -> List[Dict]:
        """Обновление кластеров ликвидности"""
        try:
            if symbol not in self.orderbooks:
                return []
                
            orderbook = self.orderbooks[symbol]
            if orderbook.empty:
                return []
                
            # Получаем текущую цену
            try:
                # Сначала пробуем получить через REST API
                ticker = self.binance_client.get_symbol_ticker(symbol=symbol)
                current_price = Decimal(str(ticker['price']))
                
                # Проверяем, что цена находится в разумных пределах
                if symbol == 'BTCUSDT':
                    if current_price < 1000 or current_price > 100000:
                        raise ValueError(f"Некорректная цена BTC: {current_price}")
                elif symbol == 'ETHUSDT':
                    if current_price < 100 or current_price > 10000:
                        raise ValueError(f"Некорректная цена ETH: {current_price}")
                elif symbol == 'BNBUSDT':
                    if current_price < 10 or current_price > 1000:
                        raise ValueError(f"Некорректная цена BNB: {current_price}")
                elif symbol == 'SOLUSDT':
                    if current_price < 1 or current_price > 1000:
                        raise ValueError(f"Некорректная цена SOL: {current_price}")
                elif symbol == 'XRPUSDT':
                    if current_price < 0.1 or current_price > 10:
                        raise ValueError(f"Некорректная цена XRP: {current_price}")
                        
            except Exception as e:
                logger.error(f"Ошибка при получении текущей цены через REST API: {e}")
                # Если не удалось получить через REST API, используем стакан
                bids = orderbook[orderbook['side'] == 'bid']
                asks = orderbook[orderbook['side'] == 'ask']
                if not bids.empty and not asks.empty:
                    best_bid = float(bids['price'].max())
                    best_ask = float(asks['price'].min())
                    current_price = Decimal(str((best_bid + best_ask) / 2))
                else:
                    logger.warning(f"Пустой стакан для {symbol}")
                    return []
            
            # Подготавливаем данные для анализа
            orderbook_data = {
                'current_price': current_price,
                'bids': orderbook[orderbook['side'] == 'bid'][['price', 'quantity']].values.tolist(),
                'asks': orderbook[orderbook['side'] == 'ask'][['price', 'quantity']].values.tolist()
            }

            # Анализируем кластеры
            clusters = self._analyze_clusters(symbol)

            # Проверяем изменения в кластерах
            if symbol in self.previous_clusters:
                changes = self._detect_cluster_changes(self.previous_clusters[symbol], clusters)
                if changes:
                    message = f"🔔 Изменения в кластерах {symbol}:\n\n"
                    for change in changes:
                        message += f"{change}\n"
                    await self._send_telegram_message(message)
            else:
                # Первое обнаружение кластеров
                if clusters:
                    message = f"🔔 Обнаружены кластеры {symbol}:\n\n"
                    
                    # Группируем кластеры по стороне
                    bid_clusters = [c for c in clusters if c['side'] == 'bid']
                    ask_clusters = [c for c in clusters if c['side'] == 'ask']
                    
                    # Добавляем информацию о бидах
                    if bid_clusters:
                        message += "📈 Кластеры на покупку:\n"
                        for cluster in bid_clusters:
                            message += f"Цена: {cluster['price']:.8f}\n"
                            message += f"Объем: {cluster['volume_usdt']:.2f} USDT\n"
                            message += f"Ордера: {cluster['orders']}\n"
                            message += f"Диапазон: [{cluster['min_price']:.8f}, {cluster['max_price']:.8f}]\n\n"
                    
                    # Добавляем информацию об асках
                    if ask_clusters:
                        message += "📉 Кластеры на продажу:\n"
                        for cluster in ask_clusters:
                            message += f"Цена: {cluster['price']:.8f}\n"
                            message += f"Объем: {cluster['volume_usdt']:.2f} USDT\n"
                            message += f"Ордера: {cluster['orders']}\n"
                            message += f"Диапазон: [{cluster['min_price']:.8f}, {cluster['max_price']:.8f}]\n\n"
                    
                    # Добавляем общую статистику
                    total_bid_volume = sum(c['volume_usdt'] for c in bid_clusters)
                    total_ask_volume = sum(c['volume_usdt'] for c in ask_clusters)
                    message += f"📊 Общая статистика:\n"
                    message += f"Объем на покупку: {total_bid_volume:.2f} USDT\n"
                    message += f"Объем на продажу: {total_ask_volume:.2f} USDT\n"
                    message += f"Дисбаланс: {((total_bid_volume - total_ask_volume) / (total_bid_volume + total_ask_volume) * 100):.2f}%"
                    
                    await self._send_telegram_message(message)

            # Сохраняем текущие кластеры
            self.previous_clusters[symbol] = clusters

            return clusters

        except Exception as e:
            logger.error(f"Ошибка при обновлении кластеров: {e}")
            return []

    def _detect_cluster_changes(self, old_clusters: List[Dict], new_clusters: List[Dict]) -> List[str]:
        """Определение изменений в кластерах"""
        changes = []
        
        # Создаем словари для быстрого поиска
        old_dict = {f"{c['side']}_{c.get('price', c.get('price_range', '0'))}": c for c in old_clusters}
        new_dict = {f"{c['side']}_{c.get('price', c.get('price_range', '0'))}": c for c in new_clusters}
        
        # Проверяем новые кластеры
        for key, new_cluster in new_dict.items():
            if key not in old_dict:
                changes.append(
                    f"🆕 Новый кластер {new_cluster['side']}:\n"
                    f"Цена: {new_cluster.get('price', new_cluster.get('price_range', '0'))}\n"
                    f"Объем: {new_cluster.get('volume_usdt', 0):.2f} USDT\n"
                    f"Ордера: {new_cluster.get('orders', 0)}\n"
                    f"Диапазон: [{new_cluster.get('min_price', 0):.8f}, {new_cluster.get('max_price', 0):.8f}]"
                )
            else:
                old_cluster = old_dict[key]
                if abs(new_cluster.get('volume_usdt', 0) - old_cluster.get('volume_usdt', 0)) > self.volume_threshold:
                    changes.append(
                        f"📊 Изменение объема кластера {new_cluster['side']}:\n"
                        f"Цена: {new_cluster.get('price', new_cluster.get('price_range', '0'))}\n"
                        f"Старый объем: {old_cluster.get('volume_usdt', 0):.2f} USDT\n"
                        f"Новый объем: {new_cluster.get('volume_usdt', 0):.2f} USDT\n"
                        f"Изменение: {((new_cluster.get('volume_usdt', 0) - old_cluster.get('volume_usdt', 0)) / old_cluster.get('volume_usdt', 1) * 100):.2f}%"
                    )
        
        # Проверяем удаленные кластеры
        for key, old_cluster in old_dict.items():
            if key not in new_dict:
                changes.append(
                    f"❌ Удален кластер {old_cluster['side']}:\n"
                    f"Цена: {old_cluster.get('price', old_cluster.get('price_range', '0'))}\n"
                    f"Объем: {old_cluster.get('volume_usdt', 0):.2f} USDT\n"
                    f"Ордера: {old_cluster.get('orders', 0)}"
                )
        
        return changes

    def _calculate_average_volume(self, orderbook_data):
        """Рассчитывает средний объем в стакане"""
        try:
            total_volume = 0
            total_orders = 0
            
            # Считаем объемы по бидам
            for price, quantity in orderbook_data.get('bids', []):
                total_volume += float(price) * float(quantity)
                total_orders += 1
                
            # Считаем объемы по аскам
            for price, quantity in orderbook_data.get('asks', []):
                total_volume += float(price) * float(quantity)
                total_orders += 1
                
            if total_orders == 0:
                return 0
                
            return total_volume / total_orders
            
        except Exception as e:
            self.logger.error(f"Ошибка при расчете среднего объема: {str(e)}")
            return 0

    def _monitor_performance(self, start_time: float, operation: str):
        """Мониторинг производительности операций"""
        execution_time = time.time() - start_time
        if execution_time > self.performance_threshold:
            logger.warning(f"Медленная операция {operation}: {execution_time:.2f} сек")
            asyncio.create_task(self.telegram_notifier.notify(
                f"⚠️ Медленная операция {operation}: {execution_time:.2f} сек"
            ))

    def _cleanup_old_notifications(self):
        """Очистка старых уведомлений"""
        current_time = time.time()
        # Ограничиваем размер словаря
        if len(self.sent_notifications) > self.max_notifications:
            self.sent_notifications = {
                k: v for k, v in self.sent_notifications.items()
                if current_time - v['timestamp'] < 3600
            }

    def _calculate_volume(self, price: str, quantity: str) -> Decimal:
        """Безопасный расчет объема с использованием Decimal"""
        try:
            return Decimal(str(price)) * Decimal(str(quantity))
        except (ValueError, TypeError, decimal.InvalidOperation) as e:
            logger.error(f"Ошибка расчета объема: {str(e)}")
            return Decimal('0')

    def _analyze_clusters(self, symbol: str) -> List[dict]:
        """Анализирует кластеры ликвидности и отправляет уведомления в Telegram."""
        start_time = time.time()
        try:
            if symbol not in self.orderbooks:
                logger.error(f"Отсутствует стакан для {symbol}")
                return []

            orderbook = self.orderbooks[symbol]
            
            # Получаем текущую цену
            try:
                # Получаем текущую цену через REST API
                ticker = self.binance_client.get_symbol_ticker(symbol=symbol)
                current_price = Decimal(str(ticker['price']))
            except Exception as e:
                logger.error(f"Ошибка при получении текущей цены для {symbol}: {str(e)}")
                return []

            if not current_price:
                logger.error(f"Отсутствует текущая цена для {symbol}")
                return []

            # Рассчитываем диапазон цен (±10%)
            price_range = Decimal('0.10')  # 10%
            min_price = current_price * (Decimal('1') - price_range)
            max_price = current_price * (Decimal('1') + price_range)

            # Фильтруем ордера в заданном диапазоне
            filtered_orderbook = orderbook[
                (orderbook['price'] >= float(min_price)) & 
                (orderbook['price'] <= float(max_price))
            ]

            if filtered_orderbook.empty:
                logger.warning(f"Нет ордеров в диапазоне ±10% для {symbol}")
                return []

            # Рассчитываем средний объем в стакане
            total_volume = Decimal('0')
            total_orders = 0
            bid_volume = Decimal('0')
            ask_volume = Decimal('0')
            
            # Считаем объемы по бидам
            for _, row in filtered_orderbook[filtered_orderbook['side'] == 'bid'].iterrows():
                try:
                    volume_usdt = self._calculate_volume(str(row['price']), str(row['quantity']))
                    # Проверяем минимальный объем ордера
                    if volume_usdt >= Decimal(str(self.min_order_volume)):
                        bid_volume += volume_usdt
                        total_volume += volume_usdt
                        total_orders += 1
                except Exception as e:
                    logger.error(f"Ошибка при обработке бида: {str(e)}")
                    continue
            
            # Считаем объемы по аскам
            for _, row in filtered_orderbook[filtered_orderbook['side'] == 'ask'].iterrows():
                try:
                    volume_usdt = self._calculate_volume(str(row['price']), str(row['quantity']))
                    # Проверяем минимальный объем ордера
                    if volume_usdt >= Decimal(str(self.min_order_volume)):
                        ask_volume += volume_usdt
                        total_volume += volume_usdt
                        total_orders += 1
                except Exception as e:
                    logger.error(f"Ошибка при обработке аска: {str(e)}")
                    continue

            if total_orders == 0:
                logger.warning(f"Нет ордеров с достаточным объемом для {symbol}")
                return []

            # Рассчитываем средний объем
            avg_volume = total_volume / Decimal(str(total_orders))

            # Группируем ордера по ценам
            clusters = []
            
            # Обрабатываем биды
            bid_clusters = self._group_orders(filtered_orderbook[filtered_orderbook['side'] == 'bid'], avg_volume, 'bid')
            clusters.extend(bid_clusters)
            
            # Обрабатываем аски
            ask_clusters = self._group_orders(filtered_orderbook[filtered_orderbook['side'] == 'ask'], avg_volume, 'ask')
            clusters.extend(ask_clusters)

            # Логируем информацию о найденных кластерах
            if clusters:
                logger.info(f"\n{'='*50}")
                logger.info(f"Найдены кластеры для {symbol}")
                logger.info(f"Текущая цена: {current_price:.8f}")
                logger.info(f"Диапазон анализа: [{min_price:.8f}, {max_price:.8f}]")
                logger.info(f"{'='*50}")
                
                # Группируем кластеры по стороне
                bid_clusters = [c for c in clusters if c['side'] == 'bid']
                ask_clusters = [c for c in clusters if c['side'] == 'ask']
                
                if bid_clusters:
                    logger.info("Кластеры на покупку:")
                    for cluster in bid_clusters:
                        logger.info(f"Цена: {cluster['price_range'][0]:.8f} - {cluster['price_range'][1]:.8f}")
                        logger.info(f"Объем: {cluster['volume']:.2f} USDT")
                        logger.info(f"Ордера: {cluster['orders']}")
                        logger.info("-" * 30)
                
                if ask_clusters:
                    logger.info("Кластеры на продажу:")
                    for cluster in ask_clusters:
                        logger.info(f"Цена: {cluster['price_range'][0]:.8f} - {cluster['price_range'][1]:.8f}")
                        logger.info(f"Объем: {cluster['volume']:.2f} USDT")
                        logger.info(f"Ордера: {cluster['orders']}")
                        logger.info("-" * 30)
                
                logger.info(f"{'='*50}\n")

            # Мониторим производительность
            self._monitor_performance(start_time, f"Анализ кластеров {symbol}")

            return clusters

        except Exception as e:
            logger.error(f"Ошибка при анализе кластеров: {str(e)}")
            logger.error(traceback.format_exc())
            return []

    async def _send_cluster_notification(self, symbol: str, cluster: dict) -> None:
        """Отправка уведомления о кластере"""
        try:
            if not self.telegram_notifier:
                logger.warning("Telegram notifier не инициализирован")
                return

            # Проверяем, не отправляли ли мы уже уведомление для этого кластера
            cluster_id = f"{symbol}_{cluster['price_range'][0]}_{cluster['volume']}"
            
            if cluster_id in self.sent_notifications:
                return
            
            # Форматируем объем
            volume_str = self._format_volume(float(cluster['volume']))

            # Определяем тип кластера
            cluster_type = "Покупка" if cluster['side'] == 'bid' else "Продажа"
            
            message = (
                f"🔍 Новый кластер на {symbol}:\n"
                f"Тип: {cluster_type}\n"
                f"Цена: {float(cluster['price_range'][0]):.8f} - {float(cluster['price_range'][1]):.8f}\n"
                f"Объем: {volume_str} USDT\n"
                f"Количество ордеров: {cluster['orders']}"
            )
            
            # Отправляем уведомление
            success = await self.telegram_notifier.notify(message)
            
            if success:
                logger.info(f"Уведомление о кластере успешно отправлено для {symbol}")
                # Сохраняем информацию об отправленном уведомлении
                self.sent_notifications[cluster_id] = datetime.now()
                # Очищаем старые уведомления
                self._cleanup_old_notifications()
            else:
                logger.error(f"Не удалось отправить уведомление о кластере для {symbol}")
            
        except Exception as e:
            logger.error(f"Ошибка при отправке уведомления о кластере: {str(e)}")
            logger.error(traceback.format_exc())

    def _format_volume(self, volume: float) -> str:
        """Форматирование объема с использованием K/M/B"""
        if volume >= 1_000_000_000:
            return f"{volume/1_000_000_000:.2f}B"
        elif volume >= 1_000_000:
            return f"{volume/1_000_000:.2f}M"
        elif volume >= 1_000:
            return f"{volume/1_000:.2f}K"
        else:
            return f"{volume:.2f}"

    def _cleanup_old_notifications(self) -> None:
        """Очистка старых уведомлений"""
        current_time = datetime.now()
        # Удаляем уведомления старше 1 часа
        self.sent_notifications = {
            k: v for k, v in self.sent_notifications.items()
            if (current_time - v).total_seconds() < 3600
        }

    async def analyze_levels(self, symbol: str, orderbook_data: pd.DataFrame) -> Dict[str, List[float]]:
        """
        Анализ уровней поддержки и сопротивления
        
        Args:
            symbol: Торговая пара
            orderbook_data: Данные стакана
            
        Returns:
            Dict[str, List[float]]: Уровни поддержки и сопротивления
        """
        try:
            if orderbook_data is None or orderbook_data.empty:
                return {'support': [], 'resistance': []}
                
            # Получаем биды и аски
            bids = orderbook_data[orderbook_data['side'] == 'bid']
            asks = orderbook_data[orderbook_data['side'] == 'ask']
            
            # Находим уровни поддержки
            support_levels = []
            for price in bids['price'].unique():
                volume = bids[bids['price'] == price]['quantity'].sum()
                if volume > self.volume_threshold:
                    support_levels.append(price)
                    
            # Находим уровни сопротивления
            resistance_levels = []
            for price in asks['price'].unique():
                volume = asks[asks['price'] == price]['quantity'].sum()
                if volume > self.volume_threshold:
                    resistance_levels.append(price)
                    
            return {
                'support': sorted(support_levels),
                'resistance': sorted(resistance_levels)
            }
            
        except Exception as e:
            logger.error(f"Ошибка при анализе уровней для {symbol}: {e}")
            return {'support': [], 'resistance': []}
            
    async def analyze_volume_profile(self, symbol: str, klines: pd.DataFrame) -> List[Dict]:
        """
        Анализ профиля объема
        
        Args:
            symbol: Торговая пара
            klines: Данные свечей
            
        Returns:
            List[Dict]: Профиль объема
        """
        try:
            if klines is None or klines.empty:
                return []
                
            # Рассчитываем VWAP
            klines['vwap'] = (klines['high'] + klines['low'] + klines['close']) / 3
            klines['volume'] = klines['volume'].astype(float)
            
            # Находим уровни с высоким объемом
            volume_levels = []
            for price in klines['vwap'].unique():
                volume = klines[klines['vwap'] == price]['volume'].sum()
                if volume > self.volume_threshold:
                    volume_levels.append({
                        'price': price,
                        'volume': volume,
                        'timestamp': datetime.now()
                    })
                    
            return sorted(volume_levels, key=lambda x: x['volume'], reverse=True)
            
        except Exception as e:
            logger.error(f"Ошибка при анализе профиля объема для {symbol}: {e}")
            return []
            
    def get_orderbook_imbalance(self, symbol: str) -> float:
        """
        Расчет дисбаланса стакана
        
        Args:
            symbol: Торговая пара
            
        Returns:
            float: Значение дисбаланса от -1 до 1
        """
        try:
            if symbol not in self.orderbooks:
                return 0.0
            
            orderbook = self.orderbooks[symbol]
            bids = orderbook[orderbook['side'] == 'bid']
            asks = orderbook[orderbook['side'] == 'ask']
            
            bid_volume = bids['quantity'].sum()
            ask_volume = asks['quantity'].sum()
            
            # Рассчитываем дисбаланс от -1 до 1
            imbalance = (bid_volume - ask_volume) / (bid_volume + ask_volume)
            return float(imbalance)
            
        except Exception as e:
            logger.error(f"Ошибка при расчете дисбаланса стакана для {symbol}: {e}")
            return 0.0
            
    def get_metrics(self, symbol: str) -> Dict:
        """
        Получение метрик стакана
        
        Args:
            symbol: Торговая пара
            
        Returns:
            Dict: Метрики стакана
        """
        try:
            if symbol not in self.orderbooks:
                return {}
                
            orderbook = self.orderbooks[symbol]
            
            # Разделяем на биды и аски
            bids = orderbook[orderbook['side'] == 'bid']
            asks = orderbook[orderbook['side'] == 'ask']
            
            if bids.empty or asks.empty:
                return {}
            
            # Рассчитываем метрики
            best_bid = float(bids['price'].max())
            best_ask = float(asks['price'].min())
            spread = (best_ask - best_bid) / best_bid
            
            bid_volume = float(bids['quantity'].sum())
            ask_volume = float(asks['quantity'].sum())
            total_volume = bid_volume + ask_volume
            
            imbalance = (bid_volume - ask_volume) / total_volume if total_volume > 0 else 0.0
            
            return {
                'best_bid': best_bid,
                'best_ask': best_ask,
                'spread': spread,
                'bid_volume': bid_volume,
                'ask_volume': ask_volume,
                'total_volume': total_volume,
                'imbalance': imbalance,
                'timestamp': datetime.now()
            }
            
        except Exception as e:
            logger.error(f"Ошибка при получении метрик стакана: {e}")
            return {}
            
    async def get_nearest_levels(self, symbol: str, current_price: float) -> Dict[str, float]:
        """
        Получение ближайших уровней поддержки и сопротивления
        
        Args:
            symbol: Торговая пара
            current_price: Текущая цена
            
        Returns:
            Dict[str, float]: Словарь с уровнями поддержки и сопротивления
        """
        try:
            if symbol not in self.orderbooks:
                logger.warning(f"Нет данных стакана для {symbol}")
                return {'support': None, 'resistance': None}
                
            orderbook = self.orderbooks[symbol]
            
            if len(orderbook) == 0:
                logger.warning(f"Пустой стакан для {symbol}")
                return {'support': None, 'resistance': None}
            
            # Получаем биды и аски
            bids = orderbook[orderbook['side'] == 'bid'].sort_values('price', ascending=False)
            asks = orderbook[orderbook['side'] == 'ask'].sort_values('price', ascending=True)
            
            if bids.empty or asks.empty:
                logger.warning(f"Нет бидов или асков для {symbol}")
                return {'support': None, 'resistance': None}
            
            # Находим ближайшие уровни
            support = None
            resistance = None
            
            # Ищем поддержку (ниже текущей цены)
            bids_below = bids[bids['price'] < current_price]
            if not bids_below.empty:
                support = float(bids_below.iloc[0]['price'])
            
            # Ищем сопротивление (выше текущей цены)
            asks_above = asks[asks['price'] > current_price]
            if not asks_above.empty:
                resistance = float(asks_above.iloc[0]['price'])
            
            return {
                'support': support,
                'resistance': resistance
            }
            
        except Exception as e:
            logger.error(f"Ошибка при получении уровней для {symbol}: {e}")
            return {'support': None, 'resistance': None} 

    def _update_clusters(self, symbol: str) -> None:
        """
        Обновление кластеров ликвидности
        
        Args:
            symbol: Торговая пара
        """
        try:
            if symbol not in self.orderbooks:
                return
                
            orderbook = self.orderbooks[symbol]
            
            # Получаем текущую цену
            try:
                # Получаем текущую цену через REST API
                ticker = self.binance_client.get_symbol_ticker(symbol=symbol)
                current_price = Decimal(str(ticker['price']))
            except Exception as e:
                logger.error(f"Ошибка при получении текущей цены для {symbol}: {str(e)}")
                return []

            # Подготавливаем данные для анализа
            orderbook_data = {
                'current_price': current_price,
                'bids': orderbook[orderbook['side'] == 'bid'][['price', 'quantity']].values.tolist(),
                'asks': orderbook[orderbook['side'] == 'ask'][['price', 'quantity']].values.tolist()
            }

            # Анализируем кластеры
            clusters = self._analyze_clusters(symbol)
            
            # Обновляем кластеры
            self.liquidity_clusters[symbol] = clusters
            
            # Проверяем изменения в кластерах
            if symbol in self.previous_clusters:
                for cluster in clusters:
                    # Проверяем, является ли кластер новым или значительно изменившимся
                    is_new = True
                    for prev_cluster in self.previous_clusters[symbol]:
                        if (abs(cluster['price_range'][0] - prev_cluster['price_range'][0]) < self.price_threshold and
                            abs(cluster['price_range'][1] - prev_cluster['price_range'][1]) < self.price_threshold):
                            is_new = False
                            # Проверяем значительное изменение объема
                            if abs(cluster['volume'] - prev_cluster['volume']) > self.volume_threshold:
                                self._send_cluster_notification(symbol, cluster, 'изменился')
                            break
                    
                    if is_new:
                        self._send_cluster_notification(symbol, cluster, 'появился')
            
            # Сохраняем текущие кластеры для следующего сравнения
            self.previous_clusters[symbol] = clusters
            
            # Сохраняем кластеры в файл
            if datetime.now() - self.last_cluster_save > self.cluster_save_interval:
                self._save_clusters()
                self.last_cluster_save = datetime.now()
                
        except Exception as e:
            logger.error(f"Ошибка при обновлении кластеров: {str(e)}")
            logger.error(traceback.format_exc())

    async def _process_orderbook_update(self, symbol: str, orderbook_data: Dict) -> None:
        """Обработка обновления стакана"""
        try:
            if not orderbook_data or 'bids' not in orderbook_data or 'asks' not in orderbook_data:
                logger.warning(f"Получены некорректные данные стакана для {symbol}")
                return

            # Получаем текущую цену
            bids = orderbook_data['bids']
            asks = orderbook_data['asks']
            
            if not bids or not asks:
                logger.warning(f"Пустой стакан для {symbol}")
                return
                
            best_bid = float(bids[0][0])
            best_ask = float(asks[0][0])
            current_price = (best_bid + best_ask) / 2
            
            # Рассчитываем диапазон цен (±10%)
            price_range = 0.10  # 10%
            min_price = current_price * (1 - price_range)
            max_price = current_price * (1 + price_range)
            
            # Фильтруем ордера в заданном диапазоне
            filtered_bids = [[price, qty] for price, qty in bids if min_price <= float(price) <= max_price]
            filtered_asks = [[price, qty] for price, qty in asks if min_price <= float(price) <= max_price]
            
            # Создаем DataFrame с отфильтрованными ордерами
            bids_data = [[float(price), float(qty), 'bid'] for price, qty in filtered_bids]
            asks_data = [[float(price), float(qty), 'ask'] for price, qty in filtered_asks]
            filtered_orderbook = pd.DataFrame(bids_data + asks_data, columns=['price', 'quantity', 'side'])
            
            # Обновляем данные стакана
            self.orderbooks[symbol] = filtered_orderbook
            
            # Анализируем кластеры только для отфильтрованных ордеров
            clusters = self._analyze_clusters(symbol)
            
            # Отправляем уведомления о новых кластерах
            for cluster in clusters:
                if (cluster['volume_usdt'] >= self.min_cluster_volume and 
                    min_price <= float(cluster['start_price']) <= max_price):
                    await self._send_cluster_notification(symbol, cluster)
                    
        except Exception as e:
            logger.error(f"Ошибка при обработке обновления стакана для {symbol}: {str(e)}")
            logger.error(traceback.format_exc())

    def _calculate_average_volume(self, symbol: str) -> float:
        """Расчет среднего объема для символа"""
        try:
            if symbol not in self.liquidity_clusters:
                return 0.0
                
            volumes = [cluster['volume_usdt'] for cluster in self.liquidity_clusters[symbol]]
            if not volumes:
                return 0.0
                
            return sum(volumes) / len(volumes)
            
        except Exception as e:
            logger.error(f"Ошибка при расчете среднего объема для {symbol}: {str(e)}")
            return 0.0

    def _group_orders(self, orders: pd.DataFrame, avg_volume: Decimal, side: str) -> List[Dict]:
        """
        Группировка ордеров в кластеры
        
        Args:
            orders: DataFrame с ордерами
            avg_volume: Средний объем ордера
            side: Сторона (bid/ask)
            
        Returns:
            List[Dict]: Список кластеров
        """
        try:
            clusters = []
            current_cluster = None
            
            # Сортируем ордера по цене
            if side == 'bid':
                orders = orders.sort_values('price', ascending=False)
            else:
                orders = orders.sort_values('price', ascending=True)
            
            for _, row in orders.iterrows():
                try:
                    price = Decimal(str(row['price']))
                    quantity = Decimal(str(row['quantity']))
                    volume_usdt = self._calculate_volume(price, quantity)
                    
                    # Проверяем минимальный объем ордера
                    if volume_usdt < Decimal(str(self.min_order_volume)):
                        continue
                    
                    if not current_cluster:
                        current_cluster = {
                            'price_range': [price, price],
                            'volume': volume_usdt,
                            'orders': 1,
                            'side': side
                        }
                    elif abs(price - current_cluster['price_range'][1]) <= self.price_step:
                        current_cluster['price_range'][1] = price
                        current_cluster['volume'] += volume_usdt
                        current_cluster['orders'] += 1
                    else:
                        if (current_cluster['volume'] >= Decimal(str(self.min_cluster_volume)) and 
                            current_cluster['orders'] >= self.min_cluster_size):
                            clusters.append(current_cluster)
                        current_cluster = {
                            'price_range': [price, price],
                            'volume': volume_usdt,
                            'orders': 1,
                            'side': side
                        }
                except Exception as e:
                    logger.error(f"Ошибка при обработке ордера: {str(e)}")
                    continue
            
            # Добавляем последний кластер
            if current_cluster and (current_cluster['volume'] >= Decimal(str(self.min_cluster_volume)) and 
                                  current_cluster['orders'] >= self.min_cluster_size):
                clusters.append(current_cluster)
            
            return clusters
            
        except Exception as e:
            logger.error(f"Ошибка при группировке ордеров: {str(e)}")
            logger.error(traceback.format_exc())
            return []

class RateLimiter:
    """Класс для ограничения частоты запросов"""
    def __init__(self, max_requests: int, period: int):
        self.max_requests = max_requests
        self.period = period
        self.requests = []
        self.lock = asyncio.Lock()

    def __call__(self, func):
        async def wrapper(*args, **kwargs):
            async with self.lock:
                current_time = time.time()
                
                # Удаляем старые запросы
                self.requests = [req_time for req_time in self.requests 
                               if current_time - req_time < self.period]
                
                # Если достигнут лимит, ждем
                if len(self.requests) >= self.max_requests:
                    sleep_time = self.requests[0] + self.period - current_time
                    if sleep_time > 0:
                        await asyncio.sleep(sleep_time)
                
                # Добавляем новый запрос
                self.requests.append(time.time())
                
                # Выполняем функцию
                if asyncio.iscoroutinefunction(func):
                    result = await func(*args, **kwargs)
                else:
                    result = func(*args, **kwargs)
                
                return result
        return wrapper 