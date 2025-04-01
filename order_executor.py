"""
Модуль для выполнения торговых ордеров
"""
import logging
from typing import Dict, List, Optional, Tuple
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from binance.client import Client
from binance.enums import *
import asyncio
from binance.exceptions import BinanceAPIException
import traceback

logger = logging.getLogger(__name__)

class OrderExecutor:
    def __init__(self, client: Client, max_position_size: float = 1.0):
        """
        Инициализация исполнителя ордеров
        
        Args:
            client: Клиент Binance
            max_position_size: Максимальный размер позиции в BTC
        """
        self.client = client
        self.max_position_size = max_position_size
        self.active_orders: Dict[str, List[Dict]] = {}
        self.iceberg_orders: Dict[str, Dict] = {}
        self.last_price_update: Dict[str, datetime] = {}
        self.price_correction_interval = timedelta(seconds=5)
        
    async def execute_order(self, symbol: str, direction: str, 
                          quantity: float, price: float,
                          order_type: str = ORDER_TYPE_LIMIT) -> bool:
        """
        Исполнение ордера с учетом рыночных условий
        
        Args:
            symbol: Торговая пара
            direction: Направление (BUY/SELL)
            quantity: Количество
            price: Цена
            order_type: Тип ордера
            
        Returns:
            bool: Успешность исполнения
        """
        try:
            # Проверяем спред спот/фьючерс
            if not await self._check_spot_futures_spread(symbol):
                logger.warning(f"Спред спот/фьючерс слишком большой для {symbol}")
                return False
            
            # Разбиваем ордер на части для Iceberg
            if quantity > self.max_position_size:
                return await self._execute_iceberg_order(
                    symbol, direction, quantity, price
                )
            
            # Исполняем обычный ордер
            return await self._execute_single_order(
                symbol, direction, quantity, price, order_type
            )
            
        except Exception as e:
            logger.error(f"Ошибка при исполнении ордера: {e}")
            return False
    
    async def _execute_iceberg_order(self, symbol: str, direction: str,
                                   quantity: float, price: float) -> bool:
        """Исполнение Iceberg ордера"""
        try:
            # Разбиваем на части
            parts = self._split_iceberg_order(quantity)
            
            # Создаем Iceberg ордер
            iceberg_order = {
                'symbol': symbol,
                'direction': direction,
                'total_quantity': quantity,
                'remaining_quantity': quantity,
                'price': price,
                'parts': parts,
                'current_part': 0,
                'timestamp': datetime.now()
            }
            
            self.iceberg_orders[symbol] = iceberg_order
            
            # Исполняем первую часть
            return await self._execute_single_order(
                symbol, direction, parts[0], price
            )
            
        except Exception as e:
            logger.error(f"Ошибка при исполнении Iceberg ордера: {e}")
            return False
    
    async def _execute_single_order(self, symbol: str, direction: str,
                                  quantity: float, price: float,
                                  order_type: str = ORDER_TYPE_LIMIT) -> bool:
        """Исполнение одиночного ордера"""
        try:
            # Корректируем цену
            adjusted_price = await self._adjust_order_price(
                symbol, direction, price
            )
            
            # Создаем ордер
            order = self.client.futures_create_order(
                symbol=symbol,
                side=direction,
                type=order_type,
                quantity=quantity,
                price=adjusted_price
            )
            
            # Сохраняем информацию об ордере
            if symbol not in self.active_orders:
                self.active_orders[symbol] = []
            self.active_orders[symbol].append(order)
            
            return True
            
        except Exception as e:
            logger.error(f"Ошибка при исполнении одиночного ордера: {e}")
            return False
    
    async def _adjust_order_price(self, symbol: str, direction: str,
                                price: float) -> float:
        """Корректировка цены ордера"""
        try:
            current_time = datetime.now()
            
            # Проверяем необходимость обновления цены
            if (symbol not in self.last_price_update or
                current_time - self.last_price_update[symbol] > self.price_correction_interval):
                
                # Получаем текущие цены
                ticker = self.client.futures_symbol_ticker(symbol=symbol)
                current_price = float(ticker['price'])
                
                # Рассчитываем корректировку
                if direction == SIDE_BUY:
                    # Для покупки устанавливаем цену чуть ниже
                    adjustment = -0.0001  # 0.01%
                else:
                    # Для продажи устанавливаем цену чуть выше
                    adjustment = 0.0001
                
                adjusted_price = current_price * (1 + adjustment)
                self.last_price_update[symbol] = current_time
                
                return adjusted_price
                
            return price
            
        except Exception as e:
            logger.error(f"Ошибка при корректировке цены: {e}")
            return price
    
    async def _check_spot_futures_spread(self, symbol: str) -> bool:
        """Проверка спреда между спотом и фьючерсами"""
        try:
            # Получаем цены
            spot_ticker = self.client.get_symbol_ticker(symbol=symbol)
            futures_ticker = self.client.futures_symbol_ticker(symbol=symbol)
            
            spot_price = float(spot_ticker['price'])
            futures_price = float(futures_ticker['price'])
            
            # Рассчитываем спред
            spread = abs(futures_price - spot_price) / spot_price
            
            # Максимально допустимый спред 0.1%
            return spread <= 0.001
            
        except Exception as e:
            logger.error(f"Ошибка при проверке спреда: {e}")
            return False
    
    def _split_iceberg_order(self, quantity: float) -> List[float]:
        """Разбивка ордера на части для Iceberg"""
        try:
            # Разбиваем на части по 20% от максимального размера
            part_size = self.max_position_size * 0.2
            parts = []
            
            remaining = quantity
            while remaining > 0:
                current_part = min(part_size, remaining)
                parts.append(current_part)
                remaining -= current_part
            
            return parts
            
        except Exception as e:
            logger.error(f"Ошибка при разбивке Iceberg ордера: {e}")
            return [quantity]
    
    async def update_iceberg_orders(self):
        """Обновление Iceberg ордеров"""
        try:
            for symbol, order in self.iceberg_orders.items():
                # Проверяем статус текущей части
                current_orders = self.active_orders.get(symbol, [])
                filled_quantity = sum(
                    float(o['executedQty']) for o in current_orders
                )
                
                # Если текущая часть исполнена
                if filled_quantity >= order['parts'][order['current_part']]:
                    order['current_part'] += 1
                    
                    # Если все части исполнены
                    if order['current_part'] >= len(order['parts']):
                        del self.iceberg_orders[symbol]
                        continue
                    
                    # Исполняем следующую часть
                    await self._execute_single_order(
                        symbol,
                        order['direction'],
                        order['parts'][order['current_part']],
                        order['price']
                    )
                    
        except Exception as e:
            logger.error(f"Ошибка при обновлении Iceberg ордеров: {e}")
    
    async def cancel_all_orders(self, symbol: str):
        """Отмена всех активных ордеров"""
        try:
            # Отменяем обычные ордера
            if symbol in self.active_orders:
                for order in self.active_orders[symbol]:
                    self.client.futures_cancel_order(
                        symbol=symbol,
                        orderId=order['orderId']
                    )
                del self.active_orders[symbol]
            
            # Отменяем Iceberg ордера
            if symbol in self.iceberg_orders:
                del self.iceberg_orders[symbol]
                
        except Exception as e:
            logger.error(f"Ошибка при отмене ордеров: {e}")
    
    async def get_order_status(self, symbol: str, order_id: str) -> Dict:
        """Получение статуса ордера"""
        try:
            return self.client.futures_get_order(
                symbol=symbol,
                orderId=order_id
            )
        except Exception as e:
            logger.error(f"Ошибка при получении статуса ордера: {e}")
            return {}

    async def get_account_balance(self) -> float:
        """
        Получение баланса аккаунта
        
        Returns:
            float: Баланс в USDT
        """
        try:
            account = self.client.get_account()
            for balance in account['balances']:
                if balance['asset'] == 'USDT':
                    return float(balance['free'])
            return 0.0
            
        except Exception as e:
            logger.error(f"Ошибка при получении баланса: {e}")
            return 0.0
            
    async def open_position(self, symbol: str, side: str, quantity: float,
                          price: float, stop_loss: float, take_profit: float,
                          leverage: int = 3) -> Dict:
        """Открытие позиции"""
        try:
            # Устанавливаем плечо
            await self.client.futures_change_leverage(
                symbol=symbol,
                leverage=leverage
            )
            
            # Открываем позицию
            order = await self.client.futures_create_order(
                symbol=symbol,
                side=side,
                type='MARKET',
                quantity=quantity,
                reduce_only=False
            )
            
            # Устанавливаем стоп-лосс и тейк-профит
            if stop_loss and take_profit:
                await self._set_stop_loss_take_profit(
                    symbol=symbol,
                    side=side,
                    stop_loss=stop_loss,
                    take_profit=take_profit
                )
            
            logger.info(f"Открыта позиция: {symbol} {side} {quantity} @ {price}")
            return order
            
        except Exception as e:
            logger.error(f"Ошибка при открытии позиции: {e}")
            logger.error(traceback.format_exc())
            return None
            
    async def close_position(self, symbol: str, quantity: float) -> Optional[Dict]:
        """
        Закрытие позиции
        
        Args:
            symbol: Торговая пара
            quantity: Количество
            
        Returns:
            Optional[Dict]: Информация об ордере
        """
        try:
            # Отменяем все открытые ордера
            self.client.futures_cancel_all_open_orders(symbol=symbol)
            
            # Закрываем позицию
            order = self.client.futures_create_order(
                symbol=symbol,
                side='SELL',  # Закрываем в любом случае через SELL
                type=ORDER_TYPE_MARKET,
                quantity=quantity
            )
            
            if order:
                logger.info(f"Закрыта позиция по {symbol}")
                logger.info(f"Количество: {quantity:.8f}")
                
            return order
            
        except Exception as e:
            logger.error(f"Ошибка при закрытии позиции: {e}")
            return None
            
    async def update_stop_loss(self, symbol: str, stop_loss: float) -> bool:
        """
        Обновление стоп-лосса
        
        Args:
            symbol: Торговая пара
            stop_loss: Новая цена стоп-лосса
            
        Returns:
            bool: Успешно ли обновлен стоп-лосс
        """
        try:
            # Отменяем старый стоп-лосс
            self.client.futures_cancel_all_open_orders(symbol=symbol)
            
            # Получаем текущую позицию
            position = self.client.futures_position_information(symbol=symbol)[0]
            side = 'SELL' if float(position['positionAmt']) > 0 else 'BUY'
            
            # Устанавливаем новый стоп-лосс
            self.client.futures_create_order(
                symbol=symbol,
                side=side,
                type=ORDER_TYPE_STOP_MARKET,
                stopPrice=stop_loss,
                closePosition=True
            )
            
            logger.info(f"Обновлен стоп-лосс для {symbol}: {stop_loss:.8f}")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка при обновлении стоп-лосса: {e}")
            return False
            
    async def update_take_profit(self, symbol: str, take_profit: float) -> bool:
        """
        Обновление тейк-профита
        
        Args:
            symbol: Торговая пара
            take_profit: Новая цена тейк-профита
            
        Returns:
            bool: Успешно ли обновлен тейк-профит
        """
        try:
            # Отменяем старый тейк-профит
            self.client.futures_cancel_all_open_orders(symbol=symbol)
            
            # Получаем текущую позицию
            position = self.client.futures_position_information(symbol=symbol)[0]
            side = 'SELL' if float(position['positionAmt']) > 0 else 'BUY'
            
            # Устанавливаем новый тейк-профит
            self.client.futures_create_order(
                symbol=symbol,
                side=side,
                type=ORDER_TYPE_TAKE_PROFIT_MARKET,
                stopPrice=take_profit,
                closePosition=True
            )
            
            logger.info(f"Обновлен тейк-профит для {symbol}: {take_profit:.8f}")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка при обновлении тейк-профита: {e}")
            return False 