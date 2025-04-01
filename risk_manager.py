"""
Модуль управления рисками для торгового бота
"""
import logging
import os
from typing import Dict, Optional
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from binance.client import Client
from binance.enums import *
import asyncio
import requests
import json

logger = logging.getLogger(__name__)

class RiskManager:
    def __init__(self, config: Dict):
        """
        Инициализация менеджера рисков
        
        Args:
            config: Конфигурация
        """
        self.config = config
        self.max_positions = 2  # Максимум 2 позиции одновременно
        self.position_size = float(config.get('position_size', 0.05))  # 5% от депозита
        self.leverage = 3  # Плечо 3x
        self.stop_loss_percent = float(config.get('stop_loss_percent', 2.0))
        self.take_profit_percent = float(config.get('take_profit_percent', 3.0))
        self.max_daily_loss = float(config.get('max_daily_loss', 5.0))
        self.max_drawdown = float(config.get('max_drawdown', 10.0))
        
        # Хранилище для отслеживания позиций
        self.positions = {}
        self.daily_pnl = 0.0
        self.peak_balance = 0.0
        self.current_balance = 0.0
        
        logger.info(f"Инициализирован RiskManager с плечом {self.leverage}x")
        
    def can_open_position(self, symbol: str, price: float, account_balance: float) -> bool:
        """
        Проверка возможности открытия позиции
        
        Args:
            symbol: Торговая пара
            price: Текущая цена
            account_balance: Баланс аккаунта
            
        Returns:
            bool: Можно ли открыть позицию
        """
        try:
            # Проверяем количество открытых позиций
            if len(self.positions) >= self.max_positions:
                logger.warning(f"Достигнут лимит открытых позиций ({self.max_positions})")
                return False
            
            # Проверяем, нет ли уже позиции по этой паре
            if symbol in self.positions:
                logger.warning(f"Уже есть открытая позиция по {symbol}")
                return False
            
            # Рассчитываем размер позиции с учетом плеча
            position_value = account_balance * self.position_size * self.leverage
            
            # Проверяем дневной убыток
            if self.daily_pnl <= -self.max_daily_loss:
                logger.warning(f"Достигнут лимит дневного убытка ({self.max_daily_loss}%)")
                return False
            
            # Проверяем просадку
            if self.current_balance < self.peak_balance * (1 - self.max_drawdown/100):
                logger.warning(f"Достигнут лимит просадки ({self.max_drawdown}%)")
                return False
            
            logger.info(f"Можно открыть позицию по {symbol}")
            logger.info(f"Размер позиции: {position_value:.2f} USDT (с плечом {self.leverage}x)")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка при проверке возможности открытия позиции: {e}")
            return False
            
    def calculate_position_size(self, account_balance: float) -> float:
        """
        Расчет размера позиции с учетом плеча
        
        Args:
            account_balance: Баланс аккаунта
            
        Returns:
            float: Размер позиции в USDT
        """
        try:
            # Рассчитываем базовый размер позиции (5% от депозита)
            base_size = account_balance * self.position_size
            
            # Применяем плечо
            leveraged_size = base_size * self.leverage
            
            logger.info(f"Рассчитан размер позиции: {leveraged_size:.2f} USDT (с плечом {self.leverage}x)")
            return leveraged_size
            
        except Exception as e:
            logger.error(f"Ошибка при расчете размера позиции: {e}")
            return 0.0
            
    def calculate_stop_loss(self, entry_price: float, side: str) -> float:
        """
        Расчет стоп-лосса с учетом плеча
        
        Args:
            entry_price: Цена входа
            side: Сторона позиции (buy/sell)
            
        Returns:
            float: Цена стоп-лосса
        """
        try:
            # Рассчитываем процент стоп-лосса с учетом плеча
            # При плече 3x, стоп-лосс 2% превращается в 0.67%
            stop_loss_percent = self.stop_loss_percent / self.leverage
            
            if side == 'buy':
                stop_loss = entry_price * (1 - stop_loss_percent/100)
            else:
                stop_loss = entry_price * (1 + stop_loss_percent/100)
                
            logger.info(f"Рассчитан стоп-лосс: {stop_loss:.8f} ({stop_loss_percent:.2f}%)")
            return stop_loss
            
        except Exception as e:
            logger.error(f"Ошибка при расчете стоп-лосса: {e}")
            return 0.0
            
    def calculate_take_profit(self, entry_price: float, side: str) -> float:
        """
        Расчет тейк-профита с учетом плеча
        
        Args:
            entry_price: Цена входа
            side: Сторона позиции (buy/sell)
            
        Returns:
            float: Цена тейк-профита
        """
        try:
            # Рассчитываем процент тейк-профита с учетом плеча
            # При плече 3x, тейк-профит 3% превращается в 1%
            take_profit_percent = self.take_profit_percent / self.leverage
            
            if side == 'buy':
                take_profit = entry_price * (1 + take_profit_percent/100)
            else:
                take_profit = entry_price * (1 - take_profit_percent/100)
                
            logger.info(f"Рассчитан тейк-профит: {take_profit:.8f} ({take_profit_percent:.2f}%)")
            return take_profit
            
        except Exception as e:
            logger.error(f"Ошибка при расчете тейк-профита: {e}")
            return 0.0
            
    def add_position(self, symbol: str, side: str, entry_price: float, 
                    quantity: float, stop_loss: float, take_profit: float) -> None:
        """
        Добавление позиции
        
        Args:
            symbol: Торговая пара
            side: Сторона позиции (buy/sell)
            entry_price: Цена входа
            quantity: Количество
            stop_loss: Цена стоп-лосса
            take_profit: Цена тейк-профита
        """
        try:
            self.positions[symbol] = {
                'side': side,
                'entry_price': entry_price,
                'quantity': quantity,
                'stop_loss': stop_loss,
                'take_profit': take_profit,
                'entry_time': datetime.now(),
                'pnl': 0.0
            }
            
            logger.info(f"Добавлена позиция по {symbol}")
            logger.info(f"Сторона: {side}")
            logger.info(f"Цена входа: {entry_price:.8f}")
            logger.info(f"Количество: {quantity:.8f}")
            logger.info(f"Стоп-лосс: {stop_loss:.8f}")
            logger.info(f"Тейк-профит: {take_profit:.8f}")
            
        except Exception as e:
            logger.error(f"Ошибка при добавлении позиции: {e}")
            
    def remove_position(self, symbol: str, pnl: float = 0.0) -> None:
        """
        Удаление позиции
        
        Args:
            symbol: Торговая пара
            pnl: Прибыль/убыток по позиции
        """
        try:
            if symbol in self.positions:
                self.daily_pnl += pnl
                self.current_balance += pnl
                self.peak_balance = max(self.peak_balance, self.current_balance)
                
                del self.positions[symbol]
                logger.info(f"Удалена позиция по {symbol}")
                logger.info(f"P&L: {pnl:.2f} USDT")
                logger.info(f"Дневной P&L: {self.daily_pnl:.2f} USDT")
                
        except Exception as e:
            logger.error(f"Ошибка при удалении позиции: {e}")
            
    def update_position_pnl(self, symbol: str, current_price: float) -> None:
        """
        Обновление P&L позиции
        
        Args:
            symbol: Торговая пара
            current_price: Текущая цена
        """
        try:
            if symbol in self.positions:
                position = self.positions[symbol]
                if position['side'] == 'buy':
                    pnl = (current_price - position['entry_price']) * position['quantity']
                else:
                    pnl = (position['entry_price'] - current_price) * position['quantity']
                    
                position['pnl'] = pnl
                logger.info(f"Обновлен P&L позиции {symbol}: {pnl:.2f} USDT")
                
        except Exception as e:
            logger.error(f"Ошибка при обновлении P&L позиции: {e}")
            
    def should_close_position(self, symbol: str, current_price: float) -> bool:
        """
        Проверка необходимости закрытия позиции
        
        Args:
            symbol: Торговая пара
            current_price: Текущая цена
            
        Returns:
            bool: Нужно ли закрыть позицию
        """
        try:
            if symbol in self.positions:
                position = self.positions[symbol]
                
                # Проверяем стоп-лосс
                if position['side'] == 'buy' and current_price <= position['stop_loss']:
                    logger.info(f"Достигнут стоп-лосс для {symbol}")
                    return True
                elif position['side'] == 'sell' and current_price >= position['stop_loss']:
                    logger.info(f"Достигнут стоп-лосс для {symbol}")
                    return True
                    
                # Проверяем тейк-профит
                if position['side'] == 'buy' and current_price >= position['take_profit']:
                    logger.info(f"Достигнут тейк-профит для {symbol}")
                    return True
                elif position['side'] == 'sell' and current_price <= position['take_profit']:
                    logger.info(f"Достигнут тейк-профит для {symbol}")
                    return True
                    
            return False
            
        except Exception as e:
            logger.error(f"Ошибка при проверке необходимости закрытия позиции: {e}")
            return False

    def update_capital(self, pnl: float) -> None:
        """
        Обновление текущего капитала
        
        Args:
            pnl: Прибыль/убыток
        """
        self.current_balance += pnl
        self.daily_pnl += pnl
        logger.info(f"Капитал обновлен: {self.current_balance:.2f} (P&L: {pnl:.2f})")
        
    def reset_daily_stats(self) -> None:
        """Сброс дневной статистики"""
        self.daily_pnl = 0.0
        logger.info("Дневная статистика сброшена")
        
    def get_position(self, symbol: str) -> Optional[Dict]:
        """
        Получение данных позиции
        
        Args:
            symbol: Торговая пара
            
        Returns:
            Optional[Dict]: Данные позиции
        """
        return self.positions.get(symbol)
        
    def update_stop_loss(self, symbol: str, new_stop_loss: float) -> None:
        """
        Обновление стоп-лосса
        
        Args:
            symbol: Торговая пара
            new_stop_loss: Новый уровень стоп-лосса
        """
        if symbol in self.positions:
            self.positions[symbol]['stop_loss'] = new_stop_loss
            logger.info(f"Обновлен стоп-лосс для {symbol}: {new_stop_loss:.8f}")
            
    def get_risk_metrics(self) -> Dict:
        """
        Получение метрик риска
        
        Returns:
            Dict: Метрики риска
        """
        return {
            'current_balance': self.current_balance,
            'daily_pnl': self.daily_pnl,
            'positions': len(self.positions),
            'daily_drawdown': (self.daily_pnl / self.current_balance) if self.current_balance > 0 else 0
        }

    async def check_risk_limits(self, symbol: str, position_size: float,
                              entry_price: float) -> bool:
        """
        Проверка лимитов риска
        
        Args:
            symbol: Торговая пара
            position_size: Размер позиции
            entry_price: Цена входа
            
        Returns:
            bool: Разрешение на сделку
        """
        try:
            # Проверяем ликвидность
            if not await self._check_liquidity(symbol):
                logger.warning(f"Недостаточная ликвидность для {symbol}")
                return False
            
            # Проверяем новости
            if not await self._check_high_impact_news(symbol):
                logger.warning(f"Обнаружены важные новости для {symbol}")
                return False
            
            # Проверяем волатильность
            if not await self._check_volatility(symbol):
                logger.warning(f"Высокая волатильность для {symbol}")
                return False
            
            # Проверяем размер позиции
            if position_size > self.position_size:
                max_position = self.position_size * self.current_balance
                if position_size > max_position:
                    logger.warning(f"Превышен лимит размера позиции для {symbol}: {position_size:.2f} > {max_position:.2f}")
                    return False
            
            # Проверяем общую экспозицию
            total_exposure = sum(self.positions.values())
            max_exposure = self.max_positions * self.current_balance
            if total_exposure + position_size > max_exposure:
                logger.warning(f"Превышен лимит общей экспозиции: {total_exposure + position_size:.2f} > {max_exposure:.2f}")
                return False
            
            logger.info(f"Все проверки рисков пройдены для {symbol}")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка при проверке лимитов риска: {e}")
            return False
    
    async def update_position_exposure(self, symbol: str, position_size: float):
        """
        Обновление экспозиции по позиции
        
        Args:
            symbol: Торговая пара
            position_size: Размер позиции
        """
        try:
            self.positions[symbol] = position_size
            
        except Exception as e:
            logger.error(f"Ошибка при обновлении экспозиции: {e}")
    
    async def check_daily_drawdown(self) -> bool:
        """
        Проверка дневной просадки
        
        Returns:
            bool: Нужно ли активировать circuit breaker
        """
        try:
            current_time = datetime.now()
            
            # Сбрасываем счетчик в начале нового дня
            if current_time.date() > self.daily_start_time.date():
                self.daily_pnl = 0.0
                self.daily_start_time = current_time
            
            # Получаем текущий P&L
            account = self.client.futures_account()
            total_pnl = float(account['totalUnrealizedProfit'])
            
            # Рассчитываем дневную просадку
            daily_drawdown = (self.current_balance - (self.current_balance + total_pnl)) / self.current_balance
            
            # Проверяем лимит просадки
            if daily_drawdown < -self.max_drawdown:
                logger.warning(f"Превышен лимит дневной просадки: {daily_drawdown:.2%}")
                await self._trigger_circuit_breaker()
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"Ошибка при проверке дневной просадки: {e}")
            return False
    
    async def check_price_gaps(self, symbol: str, current_price: float) -> bool:
        """
        Проверка гэпов цены
        
        Args:
            symbol: Торговая пара
            current_price: Текущая цена
            
        Returns:
            bool: Нужно ли закрывать позиции
        """
        try:
            if symbol not in self.positions:
                return True
            
            # Получаем последнюю цену
            last_price = await self._get_last_price(symbol)
            if last_price is None:
                return True
            
            # Рассчитываем гэп
            price_gap = abs(current_price - last_price) / last_price
            
            if price_gap > self.max_spread:
                logger.warning(f"Обнаружен большой гэп цены для {symbol}: {price_gap:.2%}")
                await self._trigger_circuit_breaker()
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"Ошибка при проверке гэпов цены: {e}")
            return True
    
    async def _check_volatility(self, symbol: str) -> bool:
        """
        Проверка волатильности
        
        Args:
            symbol: Торговая пара
            
        Returns:
            bool: True если волатильность в норме
        """
        try:
            # Добавляем задержку между запросами
            await asyncio.sleep(1)
            
            # Получаем исторические данные за 7 дней
            try:
                klines = self.client.get_historical_klines(
                    symbol,
                    Client.KLINE_INTERVAL_1HOUR,
                    "7 days ago UTC"
                )
            except Exception as e:
                logger.error(f"Ошибка при получении исторических данных для {symbol}: {e}")
                return False
            
            if not klines:
                logger.warning(f"Не удалось получить данные для {symbol}")
                return False
            
            # Рассчитываем волатильность
            closes = [float(k[4]) for k in klines]  # Цены закрытия
            returns = np.diff(closes) / closes[:-1]
            volatility = np.std(returns) * np.sqrt(24 * 7)  # Годовая волатильность
            
            # Проверяем порог
            if volatility > self.volatility_threshold:
                logger.warning(f"Высокая волатильность для {symbol}: {volatility:.2%}")
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"Ошибка при проверке волатильности: {e}")
            return True  # Разрешаем торговлю в случае ошибки
    
    async def _check_high_impact_news(self, symbol: str) -> bool:
        """
        Проверка важных новостей
        
        Args:
            symbol: Торговая пара
            
        Returns:
            bool: Нет важных новостей
        """
        try:
            current_time = datetime.now()
            
            # Проверяем необходимость обновления
            if current_time - self.last_news_check > self.news_check_interval:
                # Получаем новости через API (пример с CoinGecko)
                response = requests.get(
                    f"https://api.coingecko.com/api/v3/events"
                )
                
                if response.status_code == 200:
                    events = response.json()
                    for event in events:
                        if event['type'] == 'news':
                            # Проверяем влияние на конкретную торговую пару
                            if symbol in event['currencies']:
                                impact = float(event.get('impact', 0))
                                importance = float(event.get('importance', 0))
                                
                                if impact > self.news_impact_threshold and importance > self.news_importance_threshold:
                                    logger.warning(f"Обнаружены важные новости для {symbol}: {event['title']}")
                                    return False
                    
                    self.last_news_check = current_time
            
            return True
            
        except Exception as e:
            logger.error(f"Ошибка при проверке новостей: {e}")
            return True  # Разрешаем торговлю в случае ошибки
    
    async def _check_liquidity(self, symbol: str) -> bool:
        """
        Проверка ликвидности
        
        Args:
            symbol: Торговая пара
            
        Returns:
            bool: True если ликвидность достаточна
        """
        try:
            # Получаем данные стакана
            depth = self.client.get_order_book(symbol=symbol)
            
            if not depth or 'bids' not in depth or 'asks' not in depth:
                return False
            
            # Рассчитываем объемы на покупку и продажу
            bid_volume = sum(float(bid[1]) for bid in depth['bids'][:10])
            ask_volume = sum(float(ask[1]) for ask in depth['asks'][:10])
            
            # Проверяем соотношение объемов
            volume_ratio = min(bid_volume, ask_volume) / max(bid_volume, ask_volume)
            
            if volume_ratio < 0.5:  # Минимальное соотношение объемов
                logger.warning(f"Недостаточная ликвидность для {symbol}: ratio={volume_ratio:.2f}")
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"Ошибка при проверке ликвидности: {e}")
            return False
    
    async def _trigger_circuit_breaker(self):
        """Активация circuit breaker"""
        try:
            self.circuit_breaker_triggered = True
            logger.warning("Активация circuit breaker")
            
            # Закрываем все позиции
            for symbol in self.positions:
                await self._close_position(symbol)
            
            # Сбрасываем экспозицию
            self.positions.clear()
            
            # Сбрасываем P&L
            self.daily_pnl = 0.0
            
        except Exception as e:
            logger.error(f"Ошибка при активации circuit breaker: {e}")
    
    async def _get_last_price(self, symbol: str) -> Optional[float]:
        """
        Получение последней цены
        
        Args:
            symbol: Торговая пара
            
        Returns:
            Optional[float]: Последняя цена
        """
        try:
            ticker = self.client.futures_symbol_ticker(symbol=symbol)
            return float(ticker['price'])
            
        except Exception as e:
            logger.error(f"Ошибка при получении последней цены: {e}")
            return None
    
    async def _close_position(self, symbol: str):
        """
        Закрытие позиции
        
        Args:
            symbol: Торговая пара
        """
        try:
            # Получаем текущую позицию
            position = self.client.futures_position_information(symbol=symbol)
            
            if float(position['positionAmt']) != 0:
                # Создаем ордер на закрытие
                self.client.futures_create_order(
                    symbol=symbol,
                    side=SIDE_SELL if float(position['positionAmt']) > 0 else SIDE_BUY,
                    type=ORDER_TYPE_MARKET,
                    quantity=abs(float(position['positionAmt']))
                )
                
                logger.info(f"Позиция закрыта для {symbol}")
                
        except Exception as e:
            logger.error(f"Ошибка при закрытии позиции: {e}")
    
    def get_risk_metrics(self) -> Dict:
        """
        Получение метрик риска
        
        Returns:
            Dict: Метрики риска
        """
        try:
            return {
                'current_balance': self.current_balance,
                'daily_pnl': self.daily_pnl,
                'positions': len(self.positions),
                'daily_drawdown': self.daily_pnl / self.current_balance,
                'circuit_breaker': self.circuit_breaker_triggered,
                'volatility': {
                    symbol: values[-1] if values else 0.0
                    for symbol, values in self.volatility_history.items()
                }
            }
            
        except Exception as e:
            logger.error(f"Ошибка при получении метрик риска: {e}")
            return {} 