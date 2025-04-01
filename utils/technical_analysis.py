"""
Утилиты для технического анализа
"""
import numpy as np
from typing import List, Dict, Optional
import pandas as pd
from datetime import datetime, timedelta

def calculate_volatility(prices: List[float], window: int = 20) -> float:
    """
    Расчет волатильности
    
    Args:
        prices: Список цен
        window: Размер окна для расчета
        
    Returns:
        float: Значение волатильности
    """
    if len(prices) < window:
        return 0
        
    returns = np.diff(np.log(prices[-window:]))
    return np.std(returns) * np.sqrt(252)  # Годовая волатильность
    
def calculate_trend(prices: List[float], window: int = 20) -> str:
    """
    Определение тренда
    
    Args:
        prices: Список цен
        window: Размер окна для расчета
        
    Returns:
        str: Направление тренда ('UP', 'DOWN' или 'SIDEWAYS')
    """
    if len(prices) < window:
        return 'SIDEWAYS'
        
    # Рассчитываем SMA
    sma = np.mean(prices[-window:])
    current_price = prices[-1]
    
    # Рассчитываем наклон
    x = np.arange(window)
    y = prices[-window:]
    slope = np.polyfit(x, y, 1)[0]
    
    # Определяем тренд
    if current_price > sma and slope > 0:
        return 'UP'
    elif current_price < sma and slope < 0:
        return 'DOWN'
    else:
        return 'SIDEWAYS'
        
def calculate_rsi(prices: List[float], period: int = 14) -> float:
    """
    Расчет RSI
    
    Args:
        prices: Список цен
        period: Период для расчета
        
    Returns:
        float: Значение RSI
    """
    if len(prices) < period + 1:
        return 50
        
    # Рассчитываем изменения цен
    deltas = np.diff(prices)
    
    # Разделяем положительные и отрицательные изменения
    gains = np.where(deltas > 0, deltas, 0)
    losses = np.where(deltas < 0, -deltas, 0)
    
    # Рассчитываем средние значения
    avg_gain = np.mean(gains[-period:])
    avg_loss = np.mean(losses[-period:])
    
    if avg_loss == 0:
        return 100
        
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    
    return rsi
    
def calculate_support_resistance(prices: List[float], volumes: List[float], window: int = 20) -> Dict[str, List[float]]:
    """
    Поиск уровней поддержки и сопротивления
    
    Args:
        prices: Список цен
        volumes: Список объемов
        window: Размер окна для расчета
        
    Returns:
        Dict[str, List[float]]: Уровни поддержки и сопротивления
    """
    if len(prices) < window or len(volumes) < window:
        return {'support': [], 'resistance': []}
        
    # Создаем DataFrame
    df = pd.DataFrame({
        'price': prices[-window:],
        'volume': volumes[-window:]
    })
    
    # Находим локальные максимумы и минимумы
    price_series = pd.Series(df['price'])
    volume_series = pd.Series(df['volume'])
    
    # Находим точки разворота
    peaks = []
    troughs = []
    
    for i in range(1, len(price_series) - 1):
        if price_series[i] > price_series[i-1] and price_series[i] > price_series[i+1]:
            if volume_series[i] > volume_series.mean():
                peaks.append(price_series[i])
        elif price_series[i] < price_series[i-1] and price_series[i] < price_series[i+1]:
            if volume_series[i] > volume_series.mean():
                troughs.append(price_series[i])
                
    return {
        'support': sorted(list(set(troughs))),
        'resistance': sorted(list(set(peaks)))
    }
    
def is_breakout(price: float, volume: float, level: float, avg_volume: float, direction: str = 'up') -> bool:
    """
    Проверка пробоя уровня
    
    Args:
        price: Текущая цена
        volume: Текущий объем
        level: Уровень для проверки
        avg_volume: Средний объем
        direction: Направление пробоя ('up' или 'down')
        
    Returns:
        bool: True если произошел пробой
    """
    # Проверяем объем
    volume_breakout = volume > avg_volume * 1.5
    
    # Проверяем цену
    if direction == 'up':
        price_breakout = price > level * 1.002  # 0.2% выше уровня
    else:
        price_breakout = price < level * 0.998  # 0.2% ниже уровня
        
    return volume_breakout and price_breakout 