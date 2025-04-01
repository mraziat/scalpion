import logging
from datetime import datetime, timedelta
from binance.client import Client
from backtest import Backtest
from dotenv import load_dotenv
import os

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_backtest():
    try:
        # Загружаем переменные окружения
        load_dotenv()
        
        # Инициализируем клиент Binance
        client = Client(
            os.getenv('BINANCE_API_KEY'),
            os.getenv('BINANCE_API_SECRET')
        )
        
        # Базовая конфигурация
        base_config = {
            'min_volume_ratio': 3.0,
            'min_confidence': 0.75,
            'position_size': 0.02,
            'max_positions': 3,
            'stop_loss': 0.02,
            'take_profit': 0.04
        }
        
        # Инициализируем бэктестер
        backtest = Backtest(client, float(os.getenv('INITIAL_CAPITAL', 10000)), base_config)
        
        # Проверяем импорты
        print("\nПроверка импортов в Backtest:")
        print("✓ LevelAnalyzer:", hasattr(backtest, 'level_analyzer'))
        print("✓ OrderBookProcessor:", hasattr(backtest, 'orderbook_processor'))
        print("✓ PricePredictionModel:", hasattr(backtest, 'ml_model'))
        print("✓ TradingStrategy:", hasattr(backtest, 'trading_strategy'))
        print("✓ RiskManager:", hasattr(backtest, 'risk_manager'))
        
        # Проверяем методы
        print("\nПроверка методов в Backtest:")
        print("✓ run_backtest:", hasattr(backtest, 'run_backtest'))
        print("✓ run_forward_test:", hasattr(backtest, 'run_forward_test'))
        print("✓ run_ab_test:", hasattr(backtest, 'run_ab_test'))
        
        print("\nВсе проверки пройдены успешно!")
        
    except Exception as e:
        logger.error(f"Ошибка при тестировании: {e}")
        raise

if __name__ == "__main__":
    test_backtest() 