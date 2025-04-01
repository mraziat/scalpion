from orderbook_processor import OrderBookProcessor
import pandas as pd
import logging
from logging_config import orderbook_logger

# Импортируем настройки логирования
from logging_config import orderbook_logger

# Создаем тестовые данные стакана с реальными значениями
test_data = {
    'bids': [
        # Кластер 1 (45000.00 - 45000.50)
        [45000.50, 1.2345],
        [45000.45, 2.3456],
        [45000.40, 3.4567],
        [45000.35, 2.5678],
        [45000.30, 1.6789],
        [45000.25, 2.7890],
        [45000.20, 3.8901],
        [45000.15, 2.9012],
        [45000.10, 1.0123],
        [45000.05, 2.1234],
        [45000.00, 3.2345],
        
        # Кластер 2 (44999.00 - 44999.50)
        [44999.50, 1.3456],
        [44999.45, 2.4567],
        [44999.40, 3.5678],
        [44999.35, 2.6789],
        [44999.30, 1.7890],
        [44999.25, 2.8901],
        [44999.20, 3.9012],
        [44999.15, 2.0123],
        [44999.10, 1.1234],
        [44999.05, 2.2345],
        [44999.00, 3.3456],
        
        # Разрозненные ордера
        [44998.50, 0.4567],
        [44998.00, 0.5678],
        [44997.50, 0.6789],
        [44997.00, 0.7890],
        [44996.50, 0.8901]
    ],
    'asks': [
        # Кластер 1 (45001.00 - 45001.50)
        [45001.00, 1.4567],
        [45001.05, 2.5678],
        [45001.10, 3.6789],
        [45001.15, 2.7890],
        [45001.20, 1.8901],
        [45001.25, 2.9012],
        [45001.30, 3.0123],
        [45001.35, 2.1234],
        [45001.40, 1.2345],
        [45001.45, 2.3456],
        [45001.50, 3.4567],
        
        # Кластер 2 (45002.00 - 45002.50)
        [45002.00, 1.5678],
        [45002.05, 2.6789],
        [45002.10, 3.7890],
        [45002.15, 2.8901],
        [45002.20, 1.9012],
        [45002.25, 2.0123],
        [45002.30, 3.1234],
        [45002.35, 2.2345],
        [45002.40, 1.3456],
        [45002.45, 2.4567],
        [45002.50, 3.5678],
        
        # Разрозненные ордера
        [45003.00, 0.5678],
        [45003.50, 0.6789],
        [45004.00, 0.7890],
        [45004.50, 0.8901],
        [45005.00, 0.9012]
    ]
}

def test_orderbook_processor():
    """Тестирование процессора стакана"""
    try:
        # Создаем DataFrame из тестовых данных
        bids_df = pd.DataFrame(test_data['bids'], columns=['price', 'quantity'])
        bids_df['side'] = 'bid'
        asks_df = pd.DataFrame(test_data['asks'], columns=['price', 'quantity'])
        asks_df['side'] = 'ask'
        orderbook_data = pd.concat([bids_df, asks_df], ignore_index=True)
        
        # Создаем процессор
        processor = OrderBookProcessor()
        
        # Обрабатываем данные
        orderbook_logger.info("Начало обработки данных стакана")
        processor.process_depth_update('BTCUSDT', orderbook_data)
        
        # Получаем результаты
        clusters = processor.get_liquidity_clusters('BTCUSDT')
        metrics = processor.get_orderbook_metrics('BTCUSDT')
        
        # Выводим результаты
        orderbook_logger.info("\nМетрики стакана:")
        orderbook_logger.info(f"Дисбаланс: {metrics.get('imbalance', 0):.4f}")
        orderbook_logger.info(f"Спред: {metrics.get('spread', 0):.4f}")
        orderbook_logger.info(f"Объем бидов: {metrics.get('bid_volume', 0):.4f}")
        orderbook_logger.info(f"Объем асков: {metrics.get('ask_volume', 0):.4f}")
        
        orderbook_logger.info("\nСформированные кластеры:")
        for cluster in clusters:
            orderbook_logger.info(f"\nКластер:")
            orderbook_logger.info(f"Сторона: {cluster['side']}")
            orderbook_logger.info(f"Цена: {cluster['price']:.2f}")
            orderbook_logger.info(f"Объем: {cluster['quantity']:.2f}")
            orderbook_logger.info(f"Количество ордеров: {cluster['orders']}")
            orderbook_logger.info(f"Диапазон цен: [{cluster['price_range'][0]:.2f}, {cluster['price_range'][1]:.2f}]")
            orderbook_logger.info(f"Отношение объема: {cluster['volume_ratio']:.2f}")
            orderbook_logger.info(f"Стандартное отклонение цен: {cluster['price_std']:.6f}")
        
        return True
        
    except Exception as e:
        orderbook_logger.error(f"Ошибка при тестировании: {e}")
        return False

if __name__ == "__main__":
    test_orderbook_processor() 