from binance.client import Client
from market_analyzer import MarketAnalyzer
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
import asyncio

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

async def test_strategy():
    """Тестирование стратегии на реальных данных"""
    try:
        # Загружаем переменные окружения
        load_dotenv()
        
        # Создаем директорию для логов
        os.makedirs('logs', exist_ok=True)
        
        # Инициализируем клиент Binance
        client = Client(
            os.getenv('BINANCE_API_KEY'),
            os.getenv('BINANCE_API_SECRET')
        )
        
        # Создаем анализатор рынка и стратегию
        market_analyzer_config = {
            'min_orders_in_cluster': 5,
            'max_price_difference': 0.001,
            'std_deviation_threshold': 2.0,
            'min_order_volume': 1000,
            'min_cluster_volume': 5000,
            'max_cluster_volume': 1000000,
            'price_std_threshold': 0.0002,
            'spread_threshold': 0.0005,
            'min_cluster_size': 1,
            'max_cluster_size': 50,
            'api_key': os.getenv('BINANCE_API_KEY'),
            'api_secret': os.getenv('BINANCE_API_SECRET')
        }
        market_analyzer = MarketAnalyzer(market_analyzer_config)
        
        # Создаем компоненты для торговой стратегии
        risk_manager = RiskManager({
            'max_positions': int(os.getenv('MAX_POSITIONS', 3)),
            'position_size': float(os.getenv('POSITION_SIZE', 0.1)),
            'stop_loss_percent': float(os.getenv('STOP_LOSS_PERCENT', 2.0)),
            'take_profit_percent': float(os.getenv('TAKE_PROFIT_PERCENT', 3.0)),
            'max_daily_loss': float(os.getenv('MAX_DAILY_LOSS', 5.0)),
            'max_drawdown': float(os.getenv('MAX_DRAWDOWN', 10.0))
        })
        
        order_executor = OrderExecutor(client)
        
        # Базовая конфигурация стратегии
        strategy_config = {
            'symbol': 'BTCUSDT',
            'min_volume_ratio': 3.0,
            'min_confidence': 0.75,
            'position_size': 0.02,
            'max_positions': 3,
            'stop_loss_atr_multiplier': 2.0,
            'take_profit_atr_multiplier': 3.0,
            'risk_per_trade': 0.01,
            'min_cluster_volume': 5000,
            'min_orders_in_cluster': 5,
            'entry_probability_threshold': 0.7,
            'exit_probability_threshold': 0.8
        }
        
        strategy = TradingStrategy(
            symbol='BTCUSDT',
            risk_manager=risk_manager,
            order_executor=order_executor,
            config=strategy_config
        )
        
        # Параметры тестирования
        symbol = 'BTCUSDT'
        test_duration = timedelta(minutes=30)  # Тестируем 30 минут
        update_interval = 5  # Обновляем данные каждые 5 секунд
        
        # Статистика тестирования
        signals_generated = 0
        trades_executed = 0
        winning_trades = 0
        losing_trades = 0
        total_pnl = 0.0
        
        logger.info(f"Начало тестирования стратегии для {symbol}")
        start_time = datetime.now()
        end_time = start_time + test_duration
        
        while datetime.now() < end_time:
            try:
                # Получаем актуальные данные стакана
                depth = client.get_order_book(symbol=symbol, limit=1000)
                
                # Получаем текущую цену
                ticker = client.get_symbol_ticker(symbol=symbol)
                current_price = float(ticker['price'])
                
                # Преобразуем данные в DataFrame
                bids_df = pd.DataFrame(depth['bids'], columns=['price', 'quantity'])
                bids_df['price'] = bids_df['price'].astype(float)
                bids_df['quantity'] = bids_df['quantity'].astype(float)
                bids_df['side'] = 'bid'
                
                asks_df = pd.DataFrame(depth['asks'], columns=['price', 'quantity'])
                asks_df['price'] = asks_df['price'].astype(float)
                asks_df['quantity'] = asks_df['quantity'].astype(float)
                asks_df['side'] = 'ask'
                
                orderbook_data = pd.concat([bids_df, asks_df], ignore_index=True)
                
                # Обрабатываем данные стакана
                market_analyzer.orderbooks[symbol] = orderbook_data
                market_analyzer._update_metrics(symbol)
                clusters = await market_analyzer._update_liquidity_clusters(symbol)
                market_analyzer.liquidity_clusters[symbol] = clusters
                
                # Получаем кластеры ликвидности
                liquidity_clusters = market_analyzer.liquidity_clusters.get(symbol, [])
                
                # Получаем предсказание от ML-модели
                ml_probability = 0.7  # Заглушка, в реальности здесь будет вызов модели
                ml_position_size = strategy_config['position_size']  # Заглушка
                
                # Анализируем сигналы
                signal = await strategy.analyze_signals(
                    symbol,
                    current_price,
                    orderbook_data,
                    liquidity_clusters
                )
                
                if signal:
                    signals_generated += 1
                    logger.info(f"Сгенерирован сигнал: {signal}")
                    
                    # В реальной торговле здесь будет исполнение ордера
                    trades_executed += 1
                    
                    # Симулируем результат (в реальности будет реальный P&L)
                    if np.random.random() > 0.5:  # Симуляция выигрышной сделки
                        winning_trades += 1
                        pnl = signal['position_size'] * 0.01  # 1% прибыли
                    else:
                        losing_trades += 1
                        pnl = -signal['position_size'] * 0.005  # 0.5% убытка
                    
                    total_pnl += pnl
                
                # Ждем следующего обновления
                time.sleep(update_interval)
                
            except Exception as e:
                logger.error(f"Ошибка при обработке итерации: {e}")
                logger.error("Traceback:", exc_info=True)
                continue
        
        # Выводим итоговую статистику
        logger.info("\nРезультаты тестирования:")
        logger.info(f"Всего сигналов: {signals_generated}")
        logger.info(f"Всего сделок: {trades_executed}")
        logger.info(f"Выигрышных сделок: {winning_trades}")
        logger.info(f"Проигрышных сделок: {losing_trades}")
        logger.info(f"Винрейт: {winning_trades / trades_executed * 100:.2f}% (если были сделки)")
        logger.info(f"Общий P&L: {total_pnl:.4f}")
        
        return True
        
    except Exception as e:
        logger.error(f"Ошибка при тестировании: {e}")
        logger.error("Traceback:", exc_info=True)
        return False

if __name__ == "__main__":
    asyncio.run(test_strategy()) 