"""
Утилиты для работы со стаканом заявок
"""
import numpy as np
from typing import List, Dict, Tuple

def calculate_orderbook_imbalance(bids: List[List[float]], asks: List[List[float]]) -> float:
    """
    Расчет дисбаланса стакана
    
    Args:
        bids: Список ордеров на покупку [[цена, объем], ...]
        asks: Список ордеров на продажу [[цена, объем], ...]
        
    Returns:
        float: Значение дисбаланса от -1 до 1
    """
    bid_volume = sum(order[1] for order in bids)
    ask_volume = sum(order[1] for order in asks)
    
    total_volume = bid_volume + ask_volume
    if total_volume == 0:
        return 0
        
    return (bid_volume - ask_volume) / total_volume
    
def calculate_vwap(orders: List[List[float]]) -> float:
    """
    Расчет средневзвешенной цены
    
    Args:
        orders: Список ордеров [[цена, объем], ...]
        
    Returns:
        float: Средневзвешенная цена
    """
    if not orders:
        return 0
        
    volume_sum = sum(order[1] for order in orders)
    if volume_sum == 0:
        return 0
        
    weighted_sum = sum(order[0] * order[1] for order in orders)
    return weighted_sum / volume_sum
    
def calculate_spread(best_bid: float, best_ask: float) -> float:
    """
    Расчет спреда
    
    Args:
        best_bid: Лучшая цена покупки
        best_ask: Лучшая цена продажи
        
    Returns:
        float: Спред в процентах
    """
    if best_bid <= 0 or best_ask <= 0:
        return 0
        
    return (best_ask - best_bid) / best_bid * 100
    
def find_liquidity_clusters(orders: List[List[float]], price_threshold: float = 0.001, volume_threshold: float = 0.1) -> List[Dict]:
    """
    Поиск кластеров ликвидности
    
    Args:
        orders: Список ордеров [[цена, объем], ...]
        price_threshold: Порог для объединения цен
        volume_threshold: Минимальный объем для кластера
        
    Returns:
        List[Dict]: Список кластеров
    """
    if not orders:
        return []
        
    clusters = []
    current_cluster = {
        'price': orders[0][0],
        'volume': orders[0][1],
        'orders': 1
    }
    
    for i in range(1, len(orders)):
        price_diff = abs(orders[i][0] - current_cluster['price']) / current_cluster['price']
        
        if price_diff <= price_threshold:
            # Добавляем к текущему кластеру
            current_cluster['volume'] += orders[i][1]
            current_cluster['price'] = (current_cluster['price'] * current_cluster['orders'] + orders[i][0]) / (current_cluster['orders'] + 1)
            current_cluster['orders'] += 1
        else:
            # Сохраняем текущий кластер если он достаточно большой
            if current_cluster['volume'] >= volume_threshold:
                clusters.append(current_cluster)
                
            # Начинаем новый кластер
            current_cluster = {
                'price': orders[i][0],
                'volume': orders[i][1],
                'orders': 1
            }
            
    # Добавляем последний кластер
    if current_cluster['volume'] >= volume_threshold:
        clusters.append(current_cluster)
        
    return clusters
    
def calculate_order_flow(trades: List[Dict]) -> Tuple[float, float]:
    """
    Расчет потока ордеров
    
    Args:
        trades: Список сделок [{'price': float, 'volume': float, 'side': str}, ...]
        
    Returns:
        Tuple[float, float]: (Дисбаланс потока, Агрессивность)
    """
    if not trades:
        return 0.0, 0.0
        
    buy_volume = sum(trade['volume'] for trade in trades if trade['side'] == 'buy')
    sell_volume = sum(trade['volume'] for trade in trades if trade['side'] == 'sell')
    
    total_volume = buy_volume + sell_volume
    if total_volume == 0:
        return 0.0, 0.0
        
    # Дисбаланс потока
    flow_imbalance = (buy_volume - sell_volume) / total_volume
    
    # Агрессивность (отношение рыночных ордеров к общему объему)
    market_volume = sum(trade['volume'] for trade in trades if trade.get('is_market', False))
    aggressiveness = market_volume / total_volume if total_volume > 0 else 0
    
    return flow_imbalance, aggressiveness 