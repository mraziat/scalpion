import asyncio
import logging
from datetime import datetime, timedelta
from binance.client import Client
from backtest import Backtest
from dotenv import load_dotenv
import os

# Создаем директорию для логов если её нет
os.makedirs('logs', exist_ok=True)

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'logs/testing_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

async def main():
    try:
        logger.info("Загрузка переменных окружения...")
        load_dotenv()
        
        logger.info("Инициализация клиента Binance...")
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
            'take_profit': 0.04,
            'model_save_path': 'models'  # Путь для сохранения моделей
        }
        
        logger.info("Инициализация бэктестера...")
        backtest = Backtest(client, float(os.getenv('INITIAL_CAPITAL', 10000)), base_config)
        
        # Параметры тестирования
        symbol = 'BTCUSDT'
        end_date = datetime.now()
        start_date = end_date - timedelta(days=180)  # 6 месяцев исторических данных
        
        logger.info(f"Параметры тестирования:")
        logger.info(f"Символ: {symbol}")
        logger.info(f"Начальная дата: {start_date}")
        logger.info(f"Конечная дата: {end_date}")
        
        # Определяем версии стратегий для A/B тестирования
        strategy_versions = {
            'base': base_config,
            'aggressive': {
                **base_config,
                'min_volume_ratio': 2.0,
                'min_confidence': 0.6,
                'position_size': 0.03,
                'max_positions': 5,
                'stop_loss': 0.015,
                'take_profit': 0.03
            },
            'conservative': {
                **base_config,
                'min_volume_ratio': 4.0,
                'min_confidence': 0.85,
                'position_size': 0.01,
                'max_positions': 2,
                'stop_loss': 0.025,
                'take_profit': 0.05
            }
        }
        
        # Создаем директорию для результатов если её нет
        os.makedirs('results', exist_ok=True)
        
        logger.info("Запуск бэктеста...")
        try:
            backtest_results = backtest.run_backtest(
                symbol, start_date, end_date, "backtest",
                save_path='results/backtest_plots.png'
            )
        except Exception as e:
            logger.error(f"Ошибка при выполнении бэктеста: {e}")
            logger.error("Traceback:", exc_info=True)
            return
        
        logger.info("Результаты бэктеста:")
        logger.info(f"Общая доходность: {backtest_results.get('total_return', 0):.2%}")
        logger.info(f"Максимальная просадка: {backtest_results.get('max_drawdown', 0):.2%}")
        logger.info(f"Profit Factor: {backtest_results.get('profit_factor', 0):.2f}")
        logger.info(f"Sharpe Ratio: {backtest_results.get('sharpe_ratio', 0):.2f}")
        logger.info(f"Win Rate: {backtest_results.get('win_rate', 0):.2%}")
        
        logger.info("\nЗапуск форвард-теста...")
        forward_results = backtest.run_backtest(
            symbol,
            end_date - timedelta(days=14),
            end_date,
            "forward_test"
        )
        
        logger.info("Результаты форвард-теста:")
        logger.info(f"Общая доходность: {forward_results.get('total_return', 0):.2%}")
        logger.info(f"Максимальная просадка: {forward_results.get('max_drawdown', 0):.2%}")
        logger.info(f"Profit Factor: {forward_results.get('profit_factor', 0):.2f}")
        logger.info(f"Sharpe Ratio: {forward_results.get('sharpe_ratio', 0):.2f}")
        logger.info(f"Win Rate: {forward_results.get('win_rate', 0):.2%}")
        
        logger.info("\nЗапуск A/B тестирования...")
        ab_results = {}
        for version_name, version_config in strategy_versions.items():
            logger.info(f"Тестирование версии {version_name}...")
            version_results = backtest.run_backtest(
                symbol,
                start_date,
                end_date,
                version_name
            )
            ab_results[version_name] = version_results
        
        logger.info("Результаты A/B тестирования:")
        best_version = None
        best_profit_factor = 0
        
        for version_name, result in ab_results.items():
            logger.info(f"\nВерсия {version_name}:")
            logger.info(f"Общая доходность: {result.get('total_return', 0):.2%}")
            logger.info(f"Максимальная просадка: {result.get('max_drawdown', 0):.2%}")
            logger.info(f"Profit Factor: {result.get('profit_factor', 0):.2f}")
            logger.info(f"Sharpe Ratio: {result.get('sharpe_ratio', 0):.2f}")
            logger.info(f"Win Rate: {result.get('win_rate', 0):.2%}")
            
            # Определяем лучшую версию по profit factor
            if result.get('profit_factor', 0) > best_profit_factor:
                best_profit_factor = result.get('profit_factor', 0)
                best_version = version_name
        
        logger.info(f"\nЛучшая версия: {best_version}")
        
        # Сохраняем результаты
        backtest.save_results('results/backtest_results.json')
        
        # Строим графики
        backtest.plot_results('results/backtest_plots.png')
        
        logger.info("\nТестирование завершено. Результаты сохранены в директории results/")
        
    except Exception as e:
        logger.error(f"Ошибка при запуске тестирования: {e}")
        logger.error("Traceback:", exc_info=True)

if __name__ == "__main__":
    asyncio.run(main()) 