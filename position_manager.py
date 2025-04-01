"""
Менеджер позиций
"""
import logging
from typing import Dict, List, Optional
from datetime import datetime
from binance.client import Client
from binance.enums import *

logger = logging.getLogger(__name__)

class PositionManager:
    def __init__(self):
        """Инициализация менеджера позиций"""
        self.positions: Dict[str, Dict] = {}
        self.orders: Dict[str, List[Dict]] = {}
        self.trade_history: List[Dict] = []
        
    def open_position(self, signal: Dict) -> Optional[Dict]:
        """
        Открытие позиции
        
        Args:
            signal: Торговый сигнал
            
        Returns:
            Optional[Dict]: Данные открытой позиции
        """
        try:
            symbol = signal['symbol']
            
            # Проверяем, нет ли уже открытой позиции
            if symbol in self.positions:
                logger.warning(f"Уже есть открытая позиция для {symbol}")
                return None
            
            # Создаем новую позицию
            position = {
                'symbol': symbol,
                'direction': signal['direction'],
                'entry_price': signal['entry_price'],
                'position_size': signal['position_size'],
                'stop_loss': signal['stop_loss'],
                'take_profit': signal['take_profit'],
                'entry_time': datetime.now(),
                'status': 'OPEN'
            }
            
            # Сохраняем позицию
            self.positions[symbol] = position
            logger.info(f"Открыта позиция для {symbol}: {position}")
            
            return position
            
        except Exception as e:
            logger.error(f"Ошибка при открытии позиции: {e}")
            return None
    
    def close_position(self, symbol: str, exit_price: float,
                      reason: str = 'manual') -> Optional[Dict]:
        """
        Закрытие позиции
        
        Args:
            symbol: Торговая пара
            exit_price: Цена выхода
            reason: Причина закрытия
            
        Returns:
            Optional[Dict]: Результат сделки
        """
        try:
            if symbol not in self.positions:
                logger.warning(f"Нет открытой позиции для {symbol}")
                return None
            
            position = self.positions[symbol]
            
            # Рассчитываем P&L
            entry_price = position['entry_price']
            position_size = position['position_size']
            
            if position['direction'] == SIDE_BUY:
                pnl = (exit_price - entry_price) / entry_price * position_size
            else:
                pnl = (entry_price - exit_price) / entry_price * position_size
            
            # Создаем запись о сделке
            trade = {
                'symbol': symbol,
                'direction': position['direction'],
                'entry_price': entry_price,
                'exit_price': exit_price,
                'position_size': position_size,
                'pnl': pnl,
                'entry_time': position['entry_time'],
                'exit_time': datetime.now(),
                'duration': (datetime.now() - position['entry_time']).total_seconds(),
                'reason': reason
            }
            
            # Сохраняем сделку в историю
            self.trade_history.append(trade)
            
            # Удаляем позицию
            del self.positions[symbol]
            
            logger.info(f"Закрыта позиция для {symbol}: {trade}")
            return trade
            
        except Exception as e:
            logger.error(f"Ошибка при закрытии позиции: {e}")
            return None
    
    def update_position(self, symbol: str, current_price: float) -> None:
        """
        Обновление позиции
        
        Args:
            symbol: Торговая пара
            current_price: Текущая цена
        """
        try:
            if symbol not in self.positions:
                return
            
            position = self.positions[symbol]
            
            # Обновляем нереализованный P&L
            entry_price = position['entry_price']
            position_size = position['position_size']
            
            if position['direction'] == SIDE_BUY:
                unrealized_pnl = (current_price - entry_price) / entry_price * position_size
            else:
                unrealized_pnl = (entry_price - current_price) / entry_price * position_size
            
            position['unrealized_pnl'] = unrealized_pnl
            position['current_price'] = current_price
            position['last_update'] = datetime.now()
            
        except Exception as e:
            logger.error(f"Ошибка при обновлении позиции: {e}")
    
    def check_exit_conditions(self, symbol: str, current_price: float) -> Optional[Dict]:
        """
        Проверка условий выхода
        
        Args:
            symbol: Торговая пара
            current_price: Текущая цена
            
        Returns:
            Optional[Dict]: Сигнал на выход
        """
        try:
            if symbol not in self.positions:
                return None
            
            position = self.positions[symbol]
            
            # Проверяем стоп-лосс
            if position['direction'] == SIDE_BUY:
                if current_price <= position['stop_loss']:
                    return {
                        'type': 'stop_loss',
                        'price': current_price
                    }
            else:
                if current_price >= position['stop_loss']:
                    return {
                        'type': 'stop_loss',
                        'price': current_price
                    }
            
            # Проверяем тейк-профит
            if position['direction'] == SIDE_BUY:
                if current_price >= position['take_profit']:
                    return {
                        'type': 'take_profit',
                        'price': current_price
                    }
            else:
                if current_price <= position['take_profit']:
                    return {
                        'type': 'take_profit',
                        'price': current_price
                    }
            
            return None
            
        except Exception as e:
            logger.error(f"Ошибка при проверке условий выхода: {e}")
            return None
    
    def get_position(self, symbol: str) -> Optional[Dict]:
        """
        Получение данных позиции
        
        Args:
            symbol: Торговая пара
            
        Returns:
            Optional[Dict]: Данные позиции
        """
        return self.positions.get(symbol)
    
    def get_open_positions(self) -> List[Dict]:
        """
        Получение всех открытых позиций
        
        Returns:
            List[Dict]: Список открытых позиций
        """
        return list(self.positions.values())
    
    def get_trade_history(self, symbol: Optional[str] = None) -> List[Dict]:
        """
        Получение истории сделок
        
        Args:
            symbol: Торговая пара (опционально)
            
        Returns:
            List[Dict]: История сделок
        """
        if symbol:
            return [trade for trade in self.trade_history if trade['symbol'] == symbol]
        return self.trade_history
    
    def get_position_count(self) -> int:
        """
        Получение количества открытых позиций
        
        Returns:
            int: Количество позиций
        """
        return len(self.positions)
    
    def get_total_exposure(self) -> float:
        """
        Получение общего риска
        
        Returns:
            float: Общий риск
        """
        return sum(
            pos['position_size'] * pos['entry_price']
            for pos in self.positions.values()
        ) 