from binance.client import Client
from orderbook_processor import OrderBookProcessor
import pandas as pd
import logging
from dotenv import load_dotenv
import os
import time

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_real_orderbook():
    """Тестирование процессора стакана на реальных данных"""
    try:
        # Загружаем переменные окружения
        load_dotenv()
        
        # Инициализируем клиент Binance
        client = Client(
            os.getenv('BINANCE_API_KEY'),
            os.getenv('BINANCE_API_SECRET')
        )
        
        # Создаем процессор
        processor = OrderBookProcessor()
        
        # Получаем реальные данные стакана
        symbol = 'BTCUSDT'
        depth = client.get_order_book(symbol=symbol, limit=1000)
        
        # Преобразуем данные в DataFrame и конвертируем строки в числа
        bids_df = pd.DataFrame(depth['bids'], columns=['price', 'quantity'])
        bids_df['price'] = bids_df['price'].astype(float)
        bids_df['quantity'] = bids_df['quantity'].astype(float)
        bids_df['side'] = 'bid'
        
        asks_df = pd.DataFrame(depth['asks'], columns=['price', 'quantity'])
        asks_df['price'] = asks_df['price'].astype(float)
        asks_df['quantity'] = asks_df['quantity'].astype(float)
        asks_df['side'] = 'ask'
        
        orderbook_data = pd.concat([bids_df, asks_df], ignore_index=True)
        
        # Обрабатываем данные
        logger.info("Начало обработки данных стакана")
        processor.process_depth_update(symbol, orderbook_data)
        
        # Получаем результаты
        clusters = processor.get_liquidity_clusters(symbol)
        metrics = processor.get_orderbook_metrics(symbol)
        
        # Выводим результаты
        logger.info("\nМетрики стакана:")
        logger.info(f"Дисбаланс: {metrics.get('imbalance', 0):.4f}")
        logger.info(f"Спред: {metrics.get('spread', 0):.4f}")
        logger.info(f"Объем бидов: {metrics.get('bid_volume', 0):.4f}")
        logger.info(f"Объем асков: {metrics.get('ask_volume', 0):.4f}")
        
        logger.info("\nСформированные кластеры:")
        for cluster in clusters:
            logger.info(f"\nКластер:")
            logger.info(f"Сторона: {cluster['side']}")
            logger.info(f"Цена: {cluster['price']:.2f}")
            logger.info(f"Объем: {cluster['quantity']:.2f}")
            logger.info(f"Количество ордеров: {cluster['orders']}")
            logger.info(f"Диапазон цен: [{cluster['price_range'][0]:.2f}, {cluster['price_range'][1]:.2f}]")
            logger.info(f"Отношение объема: {cluster['volume_ratio']:.2f}")
            logger.info(f"Стандартное отклонение цен: {cluster['price_std']:.6f}")
        
        return True
        
    except Exception as e:
        logger.error(f"Ошибка при тестировании: {e}")
        logger.error("Traceback:", exc_info=True)
        return False

if __name__ == "__main__":
    test_real_orderbook() 