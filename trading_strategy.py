import logging
from typing import Dict, List, Optional, Tuple
import pandas as pd
import numpy as np
from binance.enums import *
import time
from datetime import datetime, timedelta
from market_analyzer import MarketAnalyzer
import asyncio
from ml_model import PricePredictionModel
import traceback
from decimal import Decimal

logger = logging.getLogger(__name__)

class TradingStrategy:
    def __init__(self, market_analyzer: MarketAnalyzer, risk_manager: 'RiskManager', 
                 order_executor: 'OrderExecutor', config: Dict):
        """
        Инициализация торговой стратегии
        
        Args:
            market_analyzer: Анализатор рынка
            risk_manager: Менеджер рисков
            order_executor: Исполнитель ордеров
            config: Конфигурация
        """
        self.market_analyzer = market_analyzer
        self.risk_manager = risk_manager
        self.order_executor = order_executor
        self.config = config
        
        # Инициализация торговых пар
        self.symbols = config.get('trading_pairs', ['BTCUSDT'])
        self.symbol = self.symbols[0]  # Основная торговая пара
        
        # Параметры стратегии
        self.max_position_size = float(config.get('max_position_size', 0.05))  # 5% от депозита
        
        # Пороги для ML модели
        self.entry_probability_threshold = float(config.get('entry_probability_threshold', 0.7))
        self.exit_probability_threshold = float(config.get('exit_probability_threshold', 0.8))
        
        # Пороги для кластеров ликвидности
        self.min_cluster_volume = float(config.get('min_cluster_volume', 150000))  # Минимальный объем для кластера в USDT
        self.min_orders_in_cluster = int(config.get('min_orders_in_cluster', 3))  # Минимальное количество ордеров в кластере
        
        # Текущие предсказания модели
        self.current_probability = 0.0
        self.current_position_size = 0.0
        
        # Инициализация данных
        self.orderbook_data = None
        self.klines_data = None
        self.active_positions = {}
        self.channels = {}
        
        # Инициализация ML модели
        self.ml_model = PricePredictionModel()
        
        # Пытаемся загрузить сохраненную модель
        model_path = config.get('ml_model_path', 'models/price_prediction_model')
        try:
            self.ml_model.load_model(model_path)
            logger.info(f"Загружена ML модель из {model_path}")
        except Exception as e:
            logger.warning(f"Не удалось загрузить ML модель из {model_path}: {e}")
            logger.warning("Будет использована новая модель")
        
        # Инициализация Telegram
        self.telegram_bot = None
        self.telegram_chat_id = None
        
        logger.info("Инициализирована торговая стратегия")
        
    async def process_orderbook(self, data: Dict) -> None:
        """
        Обработка данных стакана
        
        Args:
            data: Данные стакана в формате Binance WebSocket, тестовых данных или DataFrame
        """
        try:
            # Определяем формат входных данных
            if isinstance(data, pd.DataFrame):
                orderbook_data = data
                symbol = self.symbol
            else:
                # Получаем символ из данных (поддерживаем оба формата)
                symbol = data.get('s', data.get('symbol', self.symbol))
                
                # Преобразуем данные в DataFrame
                if 'data' in data:  # WebSocket формат
                    ws_data = data['data']
                    logger.debug(f"Получены WebSocket данные: {ws_data}")
                    
                    # Проверяем наличие необходимых полей
                    if 'b' not in ws_data or 'a' not in ws_data:
                        logger.error(f"Отсутствуют необходимые поля в данных стакана: {ws_data}")
                        return
                    
                    # Преобразуем биды и аски
                    bids_data = [[float(price), float(qty), 'bid'] for price, qty in ws_data['b']]
                    asks_data = [[float(price), float(qty), 'ask'] for price, qty in ws_data['a']]
                    
                    orderbook_data = pd.DataFrame(bids_data + asks_data, columns=['price', 'quantity', 'side'])
                elif 'bids' in data and 'asks' in data:  # REST API формат
                    bids_data = [[float(price), float(qty), 'bid'] for price, qty in data['bids']]
                    asks_data = [[float(price), float(qty), 'ask'] for price, qty in data['asks']]
                    orderbook_data = pd.DataFrame(bids_data + asks_data, columns=['price', 'quantity', 'side'])
                else:
                    logger.error(f"Неверный формат данных стакана для {symbol}")
                    logger.error(f"Полученные данные: {data}")
                    return
            
            logger.info(f"\nОбработка стакана для {symbol}")
            logger.info(f"Размер стакана: {len(orderbook_data)} ордеров")
            logger.info(f"Биды: {len(orderbook_data[orderbook_data['side'] == 'bid'])} ордеров")
            logger.info(f"Аски: {len(orderbook_data[orderbook_data['side'] == 'ask'])} ордеров")
            
            # Получаем текущую цену
            if not orderbook_data.empty:
                bids = orderbook_data[orderbook_data['side'] == 'bid'].sort_values('price', ascending=False)
                asks = orderbook_data[orderbook_data['side'] == 'ask'].sort_values('price', ascending=True)
                
                if not bids.empty and not asks.empty:
                    current_price = (float(bids.iloc[0]['price']) + float(asks.iloc[0]['price'])) / 2
                    logger.info(f"Текущая цена: {current_price}")
                    
                    # Обновляем данные стакана в MarketAnalyzer
                    self.market_analyzer.orderbooks[symbol] = orderbook_data
                    self.market_analyzer._update_metrics(symbol)
                    
                    # Получаем кластеры ликвидности
                    liquidity_clusters = await self.market_analyzer._update_liquidity_clusters(symbol)
                    self.market_analyzer.liquidity_clusters[symbol] = liquidity_clusters
                    
                    if liquidity_clusters:
                        logger.info(f"Найдены кластеры ликвидности: {len(liquidity_clusters)}")
                        for cluster in liquidity_clusters:
                            logger.info(f"Кластер: сторона={cluster['side']}, "
                                      f"цена={cluster['price_range'][0]:.8f} - {cluster['price_range'][1]:.8f}, "
                                      f"объем={cluster['volume']:.2f} USDT, "
                                      f"ордера={cluster['orders']}")
                    
                        # Анализируем сигналы
                        signal = await self.analyze_signals(orderbook_data)
                        
                        if signal:
                            logger.info(f"Получен сигнал: {signal}")
                            await self._execute_signal(signal)
                        else:
                            logger.info("Нет сигналов для входа/выхода")
                    else:
                        logger.warning("Не найдены кластеры ликвидности")
                else:
                    logger.warning("Пустой стакан (нет бидов или асков)")
            else:
                logger.warning("Получен пустой стакан")
                
        except Exception as e:
            logger.error(f"Ошибка при обработке стакана: {e}")
            logger.error(traceback.format_exc())
            
    async def analyze_signals(self, orderbook_data: pd.DataFrame) -> Dict:
        """Анализ сигналов на основе данных стакана"""
        try:
            # Получаем текущую цену
            bids = orderbook_data[orderbook_data['side'] == 'bid'].sort_values('price', ascending=False)
            asks = orderbook_data[orderbook_data['side'] == 'ask'].sort_values('price', ascending=True)
            
            if not bids.empty and not asks.empty:
                current_price = (float(bids.iloc[0]['price']) + float(asks.iloc[0]['price'])) / 2
            else:
                logger.warning("Не удалось получить текущую цену: пустой стакан")
                return None
            
            # Получаем кластеры ликвидности
            liquidity_clusters = await self.market_analyzer._update_liquidity_clusters(self.symbol)
            
            # Подготавливаем данные для ML модели
            ml_data = {
                'orderbook': orderbook_data,
                'liquidity_clusters': liquidity_clusters,
                'historical': []  # Пока не используем исторические данные
            }
            
            # Получаем предсказание от ML-модели
            prediction = await self.ml_model.predict(ml_data)
            self.current_probability = prediction['probability']
            self.current_position_size = prediction['position_size']
            
            # Логируем предсказание
            logger.info(f"\n{'='*50}")
            logger.info(f"Предсказание ML-модели для {self.symbol}:")
            logger.info(f"Вероятность движения: {self.current_probability:.2%}")
            logger.info(f"Рекомендуемый размер позиции: {self.current_position_size:.2%}")
            logger.info(f"{'='*50}\n")
            
            # Отправляем уведомление в Telegram
            if self.telegram_bot and self.telegram_chat_id:
                message = (
                    f"🤖 Предсказание ML-модели для {self.symbol}:\n\n"
                    f"Вероятность движения: {self.current_probability:.2%}\n"
                    f"Рекомендуемый размер позиции: {self.current_position_size:.2%}\n"
                    f"Текущая цена: {current_price:.8f}"
                )
                await self.telegram_bot.send_message(
                    chat_id=self.telegram_chat_id,
                    text=message,
                    parse_mode='HTML'
                )
            
            # Проверяем условия для входа в позицию
            if self.current_probability > self.entry_probability_threshold:
                # Проверяем количество открытых позиций
                active_positions = await self.order_executor.get_active_positions()
                if len(active_positions) >= 5:
                    logger.warning(f"Достигнут лимит открытых позиций (5)")
                    return None
                
                # Определяем сторону позиции на основе кластеров
                side = self._determine_position_side(liquidity_clusters)
                
                # Рассчитываем размер позиции с учетом плеча
                account_balance = await self.order_executor.get_account_balance()
                position_size = self.current_position_size * 3  # Умножаем на плечо
                
                # Ограничиваем размер позиции
                max_position_size = account_balance * 0.2  # Максимум 20% от баланса
                position_size = min(position_size, max_position_size)
                
                return {
                    'action': 'open',
                    'side': side,
                    'price': current_price,
                    'size': position_size,
                    'probability': self.current_probability
                }
            
            # Проверяем условия для выхода из позиции
            elif self.current_probability > self.exit_probability_threshold:
                # Получаем активные позиции
                active_positions = await self.order_executor.get_active_positions()
                if active_positions:
                    return {
                        'action': 'close',
                        'positions': active_positions,
                        'probability': self.current_probability
                    }
            
            return None
            
        except Exception as e:
            logger.error(f"Ошибка при анализе сигналов: {e}")
            logger.error(traceback.format_exc())
            return None

    def _calculate_sl_tp_from_clusters(self, current_price: float, side: str,
                                     liquidity_clusters: List[Dict]) -> Tuple[float, float]:
        """
        Расчет уровней стоп-лосс и тейк-профит на основе кластеров ликвидности
        
        Args:
            current_price: Текущая цена
            side: Сторона позиции (buy/sell)
            liquidity_clusters: Кластеры ликвидности
            
        Returns:
            Tuple[float, float]: (стоп-лосс, тейк-профит)
        """
        try:
            # Фильтруем значимые кластеры
            significant_clusters = [c for c in liquidity_clusters 
                                  if c['volume'] >= self.min_cluster_volume and
                                  c['orders'] >= self.min_orders_in_cluster]
            
            if side == 'buy':
                # Для длинной позиции
                stop_clusters = [c for c in significant_clusters 
                               if c['price'] < current_price and c['side'] == 'ask']  # Стоп-лосс ниже цены
                profit_clusters = [c for c in significant_clusters 
                                 if c['price'] > current_price and c['side'] == 'bid']  # Тейк-профит выше цены
                
                stop_loss = max([c['price'] for c in stop_clusters]) if stop_clusters else current_price * 0.99  # Максимальная цена из стоп-кластеров
                take_profit = min([c['price'] for c in profit_clusters]) if profit_clusters else current_price * 1.02  # Минимальная цена из профит-кластеров
            else:
                # Для короткой позиции
                stop_clusters = [c for c in significant_clusters 
                               if c['price'] > current_price and c['side'] == 'bid']  # Стоп-лосс выше цены
                profit_clusters = [c for c in significant_clusters 
                                 if c['price'] < current_price and c['side'] == 'ask']  # Тейк-профит ниже цены
                
                stop_loss = min([c['price'] for c in stop_clusters]) if stop_clusters else current_price * 1.01  # Минимальная цена из стоп-кластеров
                take_profit = max([c['price'] for c in profit_clusters]) if profit_clusters else current_price * 0.98  # Максимальная цена из профит-кластеров
            
            return stop_loss, take_profit
            
        except Exception as e:
            logger.error(f"Ошибка при расчете уровней SL/TP: {e}")
            # Возвращаем дефолтные значения
            if side == 'buy':
                return current_price * 0.99, current_price * 1.02
            else:
                return current_price * 1.01, current_price * 0.98
            
    def _determine_position_side(self, liquidity_clusters: List[Dict]) -> str:
        """
        Определение стороны позиции на основе кластеров ликвидности
        
        Args:
            liquidity_clusters: Кластеры ликвидности
            
        Returns:
            str: Сторона позиции (buy/sell)
        """
        try:
            # Анализируем кластеры
            bid_clusters = [c for c in liquidity_clusters if c['side'] == 'bid']
            ask_clusters = [c for c in liquidity_clusters if c['side'] == 'ask']
            
            # Считаем общий объем по каждой стороне
            bid_volume = sum(c['volume'] for c in bid_clusters)
            ask_volume = sum(c['volume'] for c in ask_clusters)
            
            # Определяем сторону на основе объема
            if bid_volume > ask_volume:
                return 'buy'
            else:
                return 'sell'
                
        except Exception as e:
            logger.error(f"Ошибка при определении стороны позиции: {e}")
            return 'buy'  # По умолчанию покупаем
            
    async def _execute_signal(self, signal: Dict) -> None:
        """
        Исполнение торгового сигнала
        
        Args:
            signal: Сигнал для торговли
        """
        try:
            if signal['action'] == 'open':
                # Проверяем возможность открытия позиции
                account_balance = await self.order_executor.get_account_balance()
                if self.risk_manager.can_open_position(
                    signal['symbol'],
                    signal['price'],
                    account_balance
                ):
                    # Открываем позицию
                    order = await self.order_executor.open_position(
                        symbol=signal['symbol'],
                        side=signal['side'],
                        quantity=signal['size'],
                        price=signal['price'],
                        stop_loss=signal['stop_loss'],
                        take_profit=signal['take_profit']
                    )
                    
                    if order:
                        # Добавляем позицию в риск-менеджер
                        self.risk_manager.add_position(
                            symbol=signal['symbol'],
                            side=signal['side'],
                            entry_price=signal['price'],
                            quantity=signal['size'],
                            stop_loss=signal['stop_loss'],
                            take_profit=signal['take_profit']
                        )
                        
                        logger.info(f"Открыта позиция по {signal['symbol']}")
                        logger.info(f"Сторона: {signal['side']}")
                        logger.info(f"Цена: {signal['price']:.8f}")
                        logger.info(f"Количество: {signal['size']:.8f}")
                        logger.info(f"Стоп-лосс: {signal['stop_loss']:.8f}")
                        logger.info(f"Тейк-профит: {signal['take_profit']:.8f}")
                        
            elif signal['action'] == 'close':
                # Закрываем позицию
                position = self.risk_manager.get_position(signal['symbol'])
                if position:
                    order = await self.order_executor.close_position(
                        symbol=signal['symbol'],
                        quantity=position['quantity']
                    )
                    
                    if order:
                        # Удаляем позицию из риск-менеджера
                        self.risk_manager.remove_position(
                            signal['symbol'],
                            pnl=(signal['price'] - position['entry_price']) * position['quantity']
                        )
                        
                        logger.info(f"Закрыта позиция по {signal['symbol']}")
                        logger.info(f"Цена закрытия: {signal['price']:.8f}")
                        
        except Exception as e:
            logger.error(f"Ошибка при исполнении сигнала: {e}")
            
    async def close_positions(self) -> None:
        """Закрытие всех позиций"""
        try:
            position = self.risk_manager.get_position(self.symbol)
            if position:
                await self.order_executor.close_position(
                    symbol=self.symbol,
                    quantity=position['quantity']
                )
                self.risk_manager.remove_position(self.symbol)
                logger.info(f"Закрыты все позиции по {self.symbol}")
                
        except Exception as e:
            logger.error(f"Ошибка при закрытии позиций: {e}")
            
    async def update_channels(self, symbol: str, orderbook_data: pd.DataFrame) -> None:
        """
        Обновление каналов на основе данных стакана
        
        Args:
            symbol: Торговая пара
            orderbook_data: DataFrame с данными стакана
        """
        try:
            if orderbook_data.empty:
                return
                
            # Получаем биды и аски
            bids = orderbook_data[orderbook_data['side'] == 'bid'].sort_values('price', ascending=False)
            asks = orderbook_data[orderbook_data['side'] == 'ask'].sort_values('price', ascending=True)
            
            if bids.empty or asks.empty:
                return
                
            # Рассчитываем текущую цену как среднее между лучшим бидом и аском
            current_price = (float(bids.iloc[0]['price']) + float(asks.iloc[0]['price'])) / 2
            
            # Получаем текущие уровни
            levels = await self.market_analyzer.get_nearest_levels(symbol, current_price)
            
            if levels and levels['support'] is not None and levels['resistance'] is not None:
                # Обновляем канал
                self.channels[symbol] = {
                    'support': levels['support'],
                    'resistance': levels['resistance'],
                    'width': levels['resistance'] - levels['support'],
                    'timestamp': time.time()
                }
                
                logger.info(f"Обновлен канал для {symbol}: support={levels['support']}, resistance={levels['resistance']}")
            
        except Exception as e:
            logger.error(f"Ошибка при обновлении каналов: {e}")
            return None
    
    def update_position(self, symbol: str, position_data: Dict):
        """
        Обновление информации о позиции
        
        Args:
            symbol: Торговая пара
            position_data: Данные о позиции
        """
        try:
            self.active_positions[symbol] = position_data
            
        except Exception as e:
            logger.error(f"Ошибка при обновлении позиции: {e}")
    
    def remove_position(self, symbol: str):
        """
        Удаление позиции
        
        Args:
            symbol: Торговая пара
        """
        try:
            if symbol in self.active_positions:
                del self.active_positions[symbol]
            
        except Exception as e:
            logger.error(f"Ошибка при удалении позиции: {e}")
    
    def get_active_positions(self) -> Dict[str, Dict]:
        """
        Получение активных позиций
        
        Returns:
            Dict[str, Dict]: Активные позиции
        """
        return self.active_positions.copy()

    async def process_trade(self, data: Dict) -> None:
        """
        Обработка сделки
        
        Args:
            data: Данные сделки в формате Binance WebSocket
        """
        try:
            symbol = data['s']  # Symbol
            price = float(data['p'])  # Price
            quantity = float(data['q'])  # Quantity
            side = "SELL" if data['m'] else "BUY"  # Is the buyer the market maker?
            
            # Получаем данные стакана
            orderbook = await self.market_analyzer.get_orderbook(symbol)
            if not orderbook:
                logger.warning(f"Не удалось получить данные стакана для {symbol}")
                return
                
            # Анализируем сигналы
            liquidity_clusters = await self.market_analyzer._update_liquidity_clusters(symbol)
            levels = await self.market_analyzer.analyze_levels(symbol, pd.DataFrame(orderbook))
            
            signals = {
                'liquidity_clusters': liquidity_clusters,
                'levels': levels
            }
            
            logger.info(f"Сгенерированы сигналы для {symbol}: {signals}")
            
        except Exception as e:
            logger.error(f"Ошибка при обработке сделки: {e}")
            logger.error("Traceback:", exc_info=True)

    async def update_klines(self, symbol: str, klines: pd.DataFrame) -> None:
        """
        Обновление данных свечей

        Args:
            symbol: Торговая пара
            klines: DataFrame с данными свечей
        """
        try:
            if klines.empty:
                logger.warning(f"Получены пустые данные свечей для {symbol}")
                return
                
            self.klines_data = klines
            logger.info(f"Обновлены данные свечей для {symbol}")
            
        except Exception as e:
            logger.error(f"Ошибка при обновлении данных свечей для {symbol}: {e}") 

    async def update_model_prediction(self, probability: float, position_size: float) -> None:
        """
        Обновление предсказаний модели
        
        Args:
            probability: Вероятность движения цены
            position_size: Рекомендуемый размер позиции
        """
        self.current_probability = probability
        self.current_position_size = position_size
        logger.info(f"Обновлены предсказания модели: вероятность={probability:.2%}, размер позиции={position_size:.2%}")

    async def run(self):
        """Основной цикл стратегии"""
        try:
            while True:
                try:
                    for symbol in self.symbols:
                        try:
                            # Получаем данные стакана
                            orderbook = await self.market_analyzer.get_orderbook(symbol)
                            if orderbook:
                                # Обрабатываем данные
                                await self.process_orderbook(orderbook)
                            else:
                                logger.warning(f"Не удалось получить данные стакана для {symbol}")
                        except Exception as e:
                            logger.error(f"Ошибка при обработке {symbol}: {str(e)}")
                            logger.error(traceback.format_exc())
                            continue
                    
                    # Ждем перед следующей итерацией
                    await asyncio.sleep(1)
                    
                except Exception as e:
                    logger.error(f"Ошибка в основном цикле стратегии: {str(e)}")
                    logger.error(traceback.format_exc())
                    await asyncio.sleep(5)  # Ждем дольше при ошибке
                    
        except asyncio.CancelledError:
            logger.info("Стратегия остановлена")
        except Exception as e:
            logger.error(f"Критическая ошибка в стратегии: {str(e)}")
            logger.error(traceback.format_exc())
            raise 

    async def analyze_orderbook(self, symbol: str, orderbook: pd.DataFrame):
        """Анализ стакана и принятие торговых решений"""
        try:
            logger.info(f"\nОбработка стакана для {symbol}")
            logger.info(f"Размер стакана: {len(orderbook)} ордеров")
            logger.info(f"Биды: {len(orderbook[orderbook['side'] == 'bid'])} ордеров")
            logger.info(f"Аски: {len(orderbook[orderbook['side'] == 'ask'])} ордеров")

            # Получаем текущую цену
            try:
                ticker = self.binance_client.get_symbol_ticker(symbol=symbol)
                current_price = Decimal(str(ticker['price']))
                logger.info(f"Текущая цена: {current_price}")
            except Exception as e:
                logger.error(f"Ошибка при получении текущей цены: {str(e)}")
                return

            # Получаем кластеры ликвидности
            clusters = self.market_analyzer.liquidity_clusters.get(symbol, [])
            if not clusters:
                logger.warning("Не найдены кластеры ликвидности")
                return

            # Анализируем кластеры
            for cluster in clusters:
                if cluster['side'] == 'bid':
                    # Анализ кластера на покупку
                    if self._is_good_buy_cluster(cluster, current_price):
                        await self._execute_buy_order(symbol, cluster)
                else:
                    # Анализ кластера на продажу
                    if self._is_good_sell_cluster(cluster, current_price):
                        await self._execute_sell_order(symbol, cluster)

        except Exception as e:
            logger.error(f"Ошибка при анализе стакана: {str(e)}") 