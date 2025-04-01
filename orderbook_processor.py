"""
Модуль для обработки данных стакана
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Optional
import logging

logger = logging.getLogger(__name__)

class OrderBookProcessor:
    def __init__(self):
        """Инициализация процессора стакана"""
        # Параметры для анализа стакана
        self.min_order_volume = 50  # Минимальный объем ордера в USDT
        self.min_cluster_volume = 200  # Минимальный объем кластера в USDT
        self.max_cluster_volume = 1000000  # Максимальный объем кластера в USDT
        self.price_std_threshold = 0.0005  # Порог стандартного отклонения цены
        self.spread_threshold = 0.001  # Порог спреда
        self.min_cluster_size = 2  # Минимальное количество ордеров в кластере
        self.max_cluster_size = 50  # Максимальное количество ордеров в кластере
        
        # Хранилище данных
        self.orderbooks: Dict[str, pd.DataFrame] = {}
        self.liquidity_clusters: Dict[str, List[Dict]] = {}
        
    def process_depth_update(self, symbol: str, orderbook_data: pd.DataFrame) -> None:
        """
        Обработка обновления стакана
        
        Args:
            symbol: Торговая пара
            orderbook_data: Данные стакана
        """
        try:
            # Сохраняем данные стакана
            self.orderbooks[symbol] = orderbook_data
            
            # Обновляем кластеры
            self._update_liquidity_clusters(symbol)
            
        except Exception as e:
            logger.error(f"Ошибка при обработке стакана для {symbol}: {e}")
            
    def get_liquidity_clusters(self, symbol: str) -> List[Dict]:
        """
        Получение кластеров ликвидности
        
        Args:
            symbol: Торговая пара
            
        Returns:
            List[Dict]: Список кластеров
        """
        return self.liquidity_clusters.get(symbol, [])
        
    def _update_liquidity_clusters(self, symbol: str) -> None:
        """
        Обновление кластеров ликвидности
        
        Args:
            symbol: Торговая пара
        """
        try:
            if symbol not in self.orderbooks:
                logger.warning(f"Нет данных стакана для {symbol}")
                return
                
            orderbook = self.orderbooks[symbol]
            if orderbook.empty:
                logger.warning(f"Пустой стакан для {symbol}")
                return
                
            logger.info(f"\n{'='*50}")
            logger.info(f"Анализ стакана для {symbol}")
            logger.info(f"{'='*50}")
            logger.info(f"Всего ордеров: {len(orderbook)}")
            logger.info(f"Параметры поиска кластеров:")
            logger.info(f"min_order_volume: {self.min_order_volume} USDT")
            logger.info(f"min_cluster_volume: {self.min_cluster_volume} USDT")
            logger.info(f"min_cluster_size: {self.min_cluster_size} ордеров")
            logger.info(f"price_std_threshold: {self.price_std_threshold}")
            
            # Разделяем на биды и аски
            bids = orderbook[orderbook['side'] == 'bid'].sort_values('price', ascending=False)
            asks = orderbook[orderbook['side'] == 'ask'].sort_values('price', ascending=True)
            
            logger.info(f"\nСтатистика стакана:")
            logger.info(f"Биды: {len(bids)} ордеров")
            logger.info(f"Аски: {len(asks)} ордеров")
            logger.info(f"Лучший бид: {bids['price'].max():.8f}")
            logger.info(f"Лучший аск: {asks['price'].min():.8f}")
            
            clusters = []
            
            # Анализ бидов
            logger.info(f"\n{'='*50}")
            logger.info(f"Анализ кластеров бидов")
            logger.info(f"{'='*50}")
            current_cluster = None
            for _, row in bids.iterrows():
                price = float(row['price'])
                quantity = float(row['quantity'])
                volume_usdt = price * quantity
                
                if volume_usdt < self.min_order_volume:
                    continue
                    
                if current_cluster is None:
                    current_cluster = {
                        'side': 'bid',
                        'price': price,
                        'quantity': quantity,
                        'orders': 1,
                        'min_price': price,
                        'max_price': price,
                        'volume_usdt': volume_usdt
                    }
                elif abs(price - current_cluster['price']) / current_cluster['price'] <= self.price_std_threshold:
                    current_cluster['quantity'] += quantity
                    current_cluster['orders'] += 1
                    current_cluster['min_price'] = min(current_cluster['min_price'], price)
                    current_cluster['max_price'] = max(current_cluster['max_price'], price)
                    current_cluster['volume_usdt'] += volume_usdt
                else:
                    if (current_cluster['volume_usdt'] >= self.min_cluster_volume and 
                        current_cluster['orders'] >= self.min_cluster_size):
                        clusters.append(current_cluster)
                        logger.info(f"\nНайден кластер бидов:")
                        logger.info(f"Цена: {current_cluster['price']:.8f}")
                        logger.info(f"Объем: {current_cluster['volume_usdt']:.2f} USDT")
                        logger.info(f"Количество ордеров: {current_cluster['orders']}")
                        logger.info(f"Диапазон цен: [{current_cluster['min_price']:.8f}, {current_cluster['max_price']:.8f}]")
                    current_cluster = {
                        'side': 'bid',
                        'price': price,
                        'quantity': quantity,
                        'orders': 1,
                        'min_price': price,
                        'max_price': price,
                        'volume_usdt': volume_usdt
                    }
            
            # Добавляем последний кластер бидов
            if current_cluster and (current_cluster['volume_usdt'] >= self.min_cluster_volume and 
                                  current_cluster['orders'] >= self.min_cluster_size):
                clusters.append(current_cluster)
                logger.info(f"\nНайден последний кластер бидов:")
                logger.info(f"Цена: {current_cluster['price']:.8f}")
                logger.info(f"Объем: {current_cluster['volume_usdt']:.2f} USDT")
                logger.info(f"Количество ордеров: {current_cluster['orders']}")
                logger.info(f"Диапазон цен: [{current_cluster['min_price']:.8f}, {current_cluster['max_price']:.8f}]")
            
            # Анализ асков
            logger.info(f"\n{'='*50}")
            logger.info(f"Анализ кластеров асков")
            logger.info(f"{'='*50}")
            current_cluster = None
            for _, row in asks.iterrows():
                price = float(row['price'])
                quantity = float(row['quantity'])
                volume_usdt = price * quantity
                
                if volume_usdt < self.min_order_volume:
                    continue
                    
                if current_cluster is None:
                    current_cluster = {
                        'side': 'ask',
                        'price': price,
                        'quantity': quantity,
                        'orders': 1,
                        'min_price': price,
                        'max_price': price,
                        'volume_usdt': volume_usdt
                    }
                elif abs(price - current_cluster['price']) / current_cluster['price'] <= self.price_std_threshold:
                    current_cluster['quantity'] += quantity
                    current_cluster['orders'] += 1
                    current_cluster['min_price'] = min(current_cluster['min_price'], price)
                    current_cluster['max_price'] = max(current_cluster['max_price'], price)
                    current_cluster['volume_usdt'] += volume_usdt
                else:
                    if (current_cluster['volume_usdt'] >= self.min_cluster_volume and 
                        current_cluster['orders'] >= self.min_cluster_size):
                        clusters.append(current_cluster)
                        logger.info(f"\nНайден кластер асков:")
                        logger.info(f"Цена: {current_cluster['price']:.8f}")
                        logger.info(f"Объем: {current_cluster['volume_usdt']:.2f} USDT")
                        logger.info(f"Количество ордеров: {current_cluster['orders']}")
                        logger.info(f"Диапазон цен: [{current_cluster['min_price']:.8f}, {current_cluster['max_price']:.8f}]")
                    current_cluster = {
                        'side': 'ask',
                        'price': price,
                        'quantity': quantity,
                        'orders': 1,
                        'min_price': price,
                        'max_price': price,
                        'volume_usdt': volume_usdt
                    }
            
            # Добавляем последний кластер асков
            if current_cluster and (current_cluster['volume_usdt'] >= self.min_cluster_volume and 
                                  current_cluster['orders'] >= self.min_cluster_size):
                clusters.append(current_cluster)
                logger.info(f"\nНайден последний кластер асков:")
                logger.info(f"Цена: {current_cluster['price']:.8f}")
                logger.info(f"Объем: {current_cluster['volume_usdt']:.2f} USDT")
                logger.info(f"Количество ордеров: {current_cluster['orders']}")
                logger.info(f"Диапазон цен: [{current_cluster['min_price']:.8f}, {current_cluster['max_price']:.8f}]")
            
            # Сохраняем кластеры
            self.liquidity_clusters[symbol] = clusters
            
            # Выводим итоговую статистику
            logger.info(f"\n{'='*50}")
            logger.info(f"Итоги анализа для {symbol}")
            logger.info(f"{'='*50}")
            logger.info(f"Всего найдено кластеров: {len(clusters)}")
            if clusters:
                # Сортируем кластеры по объему
                sorted_clusters = sorted(clusters, key=lambda x: x['volume_usdt'], reverse=True)
                logger.info("\nТоп-5 кластеров по объему:")
                for i, cluster in enumerate(sorted_clusters[:5], 1):
                    logger.info(f"\n{i}. Кластер {cluster['side']}:")
                    logger.info(f"Цена: {cluster['price']:.8f}")
                    logger.info(f"Объем: {cluster['volume_usdt']:.2f} USDT")
                    logger.info(f"Ордера: {cluster['orders']}")
                    logger.info(f"Диапазон: [{cluster['min_price']:.8f}, {cluster['max_price']:.8f}]")
            
        except Exception as e:
            logger.error(f"Ошибка при обновлении кластеров для {symbol}: {e}") 