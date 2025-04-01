"""
Основной модуль торгового бота
"""
import os
import logging
import asyncio
from datetime import datetime
from typing import Dict, List, Optional, Any
from dotenv import load_dotenv
import pandas as pd
import numpy as np
import time
import traceback
from binance.client import Client
from binance.streams import BinanceSocketManager
from binance.enums import *
import glob
import signal
import sys
from logging.handlers import RotatingFileHandler

from utils.telegram_notifier import TelegramNotifier
from order_executor import OrderExecutor
from risk_manager import RiskManager
from trading_strategy import TradingStrategy
from utils.log_manager import setup_logging
from ml_model import PricePredictionModel
from market_analyzer import MarketAnalyzer
from config import load_config
from websocket_manager import WebSocketManager

def cleanup_old_logs(log_dir: str = 'logs', max_files: int = 5) -> None:
    """
    Очистка старых лог-файлов, оставляя только последние max_files файлов
    
    Args:
        log_dir: Директория с логами
        max_files: Максимальное количество файлов для хранения
    """
    try:
        # Получаем список всех лог-файлов
        log_files = glob.glob(os.path.join(log_dir, 'trading_bot_*.log'))
        
        # Сортируем файлы по времени создания (новые первыми)
        log_files.sort(key=os.path.getctime, reverse=True)
        
        # Удаляем старые файлы
        for old_file in log_files[max_files:]:
            try:
                os.remove(old_file)
                logger.info(f"Удален старый лог-файл: {old_file}")
            except Exception as e:
                logger.error(f"Ошибка при удалении файла {old_file}: {e}")
                
    except Exception as e:
        logger.error(f"Ошибка при очистке лог-файлов: {e}")

# Создаем директорию для логов, если она не существует
if not os.path.exists('logs'):
    os.makedirs('logs')

# Очищаем старые лог-файлы
cleanup_old_logs()

# Создаем уникальное имя файла логов с временной меткой
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
log_file = f'logs/trading_bot_{timestamp}.log'

# Настраиваем логирование с ротацией файлов
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        RotatingFileHandler(
            log_file,
            maxBytes=10*1024*1024,  # 10MB
            backupCount=5,
            encoding='utf-8'
        ),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

async def graceful_shutdown(signal: signal.Signals, loop: asyncio.AbstractEventLoop, telegram_notifier: TelegramNotifier) -> None:
    """Корректное завершение работы"""
    logger.info(f"Получен сигнал {signal.name}")
    
    # Отправляем уведомление об остановке
    await telegram_notifier.notify_stop()
    
    # Отменяем все задачи
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    for task in tasks:
        task.cancel()
    
    # Ждем завершения всех задач
    await asyncio.gather(*tasks, return_exceptions=True)
    
    # Останавливаем event loop
    loop.stop()

async def check_balance(client: Any, config: Dict[str, Any], telegram_notifier: TelegramNotifier) -> None:
    """Проверка баланса аккаунта"""
    try:
        account = await client.get_account()
        balance = float(account['totalAsset'])
        if balance < config['min_balance']:
            logger.warning(f"Низкий баланс: {balance} USDT")
            await telegram_notifier.notify(f"⚠️ Низкий баланс: {balance} USDT")
    except Exception as e:
        logger.error(f"Ошибка проверки баланса: {str(e)}")

async def main():
    try:
        # Загрузка конфигурации
        load_dotenv()
        
        # Формируем конфигурацию
        config = {
            'api_key': os.getenv('BINANCE_API_KEY'),
            'api_secret': os.getenv('BINANCE_API_SECRET'),
            'telegram_token': os.getenv('TELEGRAM_BOT_TOKEN'),
            'telegram_chat_id': os.getenv('TELEGRAM_CHAT_ID'),
            'trading_pairs': os.getenv('TRADING_PAIRS', 'BTCUSDT,ETHUSDT').split(','),
            'max_positions': int(os.getenv('MAX_POSITIONS', '3')),
            'position_size': float(os.getenv('POSITION_SIZE', '0.1')),
            'leverage': int(os.getenv('LEVERAGE', '1')),
            'initial_capital': float(os.getenv('INITIAL_CAPITAL', '1000.0')),
            'max_risk_per_trade': float(os.getenv('MAX_RISK_PER_TRADE', '0.02')),
            'max_daily_drawdown': float(os.getenv('MAX_DAILY_DRAWDOWN', '0.05'))
        }
        
        # Инициализация клиента Binance
        binance_client = Client(
            api_key=config['api_key'],
            api_secret=config['api_secret']
        )
        
        # Инициализация WebSocket менеджера
        ws_manager = WebSocketManager(client=binance_client, cache_timeout=1.0)
        
        # Добавляем торговые пары
        for symbol in config['trading_pairs']:
            ws_manager.trading_pairs.append(symbol)
        
        # Инициализация компонентов
        telegram_notifier = TelegramNotifier(config)
        await telegram_notifier.start()
        
        market_analyzer = MarketAnalyzer(
            config=config,
            binance_client=binance_client,
            websocket_manager=ws_manager
        )
        
        risk_manager = RiskManager(config)
        order_executor = OrderExecutor(config)
        trading_strategy = TradingStrategy(
            market_analyzer=market_analyzer,
            risk_manager=risk_manager,
            order_executor=order_executor,
            config=config
        )
        
        # Подписываемся на обновления стакана для всех пар
        for symbol in config['trading_pairs']:
            await ws_manager.subscribe_orderbook(symbol)
        
        # Запускаем WebSocket менеджер
        ws_task = asyncio.create_task(ws_manager.start())
        
        # Запускаем торговую стратегию
        strategy_task = asyncio.create_task(trading_strategy.run())
        
        # Ожидаем завершения задач
        await asyncio.gather(ws_task, strategy_task)
        
    except Exception as e:
        logger.error(f"Ошибка в main: {str(e)}")
        raise
    finally:
        # Останавливаем WebSocket менеджер
        if 'ws_manager' in locals():
            await ws_manager.stop()
        # Останавливаем Telegram уведомления
        if 'telegram_notifier' in locals():
            await telegram_notifier.stop()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Бот остановлен пользователем")
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
        raise 