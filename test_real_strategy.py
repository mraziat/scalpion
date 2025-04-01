from binance.client import Client
from orderbook_processor import OrderBookProcessor
from trading_strategy import TradingStrategy
import pandas as pd
import logging
from dotenv import load_dotenv
import os
import time
from datetime import datetime, timedelta
import numpy as np
from risk_manager import RiskManager
from order_executor import OrderExecutor
import random
import asyncio
import traceback
from ml_model import PricePredictionModel

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/test_strategy.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Загружаем переменные окружения
load_dotenv()

# Заглушка для ML-модели
class MockMLModel:
    def prepare_features(self, data):
        return np.random.rand(1, 100, 10)  # Возвращаем случайные признаки
        
    def predict(self, features):
        return 0.7, 0.1  # Возвращаем фиксированные значения для тестирования

async def test_strategy(symbol: str, timeframe: str = '1m', duration: int = 30) -> None:
    """
    Тестирование торговой стратегии на реальных данных
    
    Args:
        symbol: Торговая пара
        timeframe: Таймфрейм
        duration: Длительность тестирования в минутах
    """
    try:
        # Инициализируем клиент Binance
        client = Client()
        
        # Инициализируем компоненты стратегии
        config = {
            'min_volume_ratio': 0.5,
            'min_confidence': 0.7,
            'position_size': 0.1,
            'max_positions': 3,
            'stop_loss_atr_multiplier': 1.5,
            'take_profit_atr_multiplier': 2.0,
            'risk_per_trade': 0.02
        }
        
        risk_manager = RiskManager(config)
        order_executor = OrderExecutor(client, config)
        strategy = TradingStrategy(symbol, risk_manager, order_executor, config)
        orderbook_processor = OrderBookProcessor()
        ml_model = PricePredictionModel()
        
        # Получаем исторические данные
        end_time = int(time.time() * 1000)
        start_time = end_time - (duration * 60 * 1000)
        
        klines = client.get_klines(
            symbol=symbol,
            interval=timeframe,
            startTime=start_time,
            endTime=end_time
        )
        
        # Симулируем торговлю
        total_pnl = 0
        wins = 0
        losses = 0
        
        for i in range(len(klines) - 1):
            # Получаем стакан
            depth = client.get_order_book(symbol=symbol, limit=1000)
            
            # Преобразуем в DataFrame
            bids = pd.DataFrame(depth['bids'], columns=['price', 'quantity'], dtype=float)
            asks = pd.DataFrame(depth['asks'], columns=['price', 'quantity'], dtype=float)
            bids['side'] = 'bid'
            asks['side'] = 'ask'
            orderbook = pd.concat([bids, asks])
            
            # Получаем текущую цену как среднее между лучшим бидом и аском
            current_price = (orderbook[orderbook['side'] == 'bid']['price'].max() + 
                            orderbook[orderbook['side'] == 'ask']['price'].min()) / 2
            
            # Анализируем стакан
            orderbook_processor.process_depth_update(symbol, orderbook)
            liquidity_clusters = orderbook_processor.get_liquidity_clusters(symbol)
            
            # Создаем словарь с данными для ML модели
            ml_data = {
                'orderbook': orderbook,
                'liquidity_clusters': liquidity_clusters,
                'historical': [{'price': current_price, 'quantity': 1.0}] * 100  # Добавляем исторические данные
            }

            # Получаем предсказания от ML модели
            features = ml_model.prepare_features(ml_data)
            
            # Генерируем сигнал
            signal = await strategy.analyze_signals(
                symbol,
                float(orderbook.iloc[0]['price']),
                orderbook,
                liquidity_clusters
            )
            
            if signal:
                logger.info(f"Сгенерирован сигнал: {signal}")
                
                # Симулируем результат
                if random.random() > 0.5:  # 50% шанс на успех
                    pnl = signal['position_size'] * 0.01  # 1% прибыли
                    wins += 1
                else:
                    pnl = -signal['position_size'] * 0.005  # 0.5% убытка
                    losses += 1
                    
                total_pnl += pnl
                logger.info(f"P&L: {pnl:.2f} USDT")
            
            time.sleep(1)  # Пауза между итерациями
            
        # Выводим статистику
        logger.info(f"\nРезультаты тестирования:")
        logger.info(f"Всего сделок: {wins + losses}")
        logger.info(f"Выигрышных: {wins}")
        logger.info(f"Проигрышных: {losses}")
        logger.info(f"Винрейт: {wins/(wins + losses)*100:.1f}%")
        logger.info(f"Общий P&L: {total_pnl:.2f} USDT")
        
    except Exception as e:
        logger.error(f"Ошибка при тестировании: {e}")
        logger.error(traceback.format_exc())

# Запускаем тест
if __name__ == "__main__":
    asyncio.run(test_strategy('BTCUSDT')) 