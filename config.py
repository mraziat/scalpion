import os
from typing import Dict, Any
import logging
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

def validate_config(config: Dict[str, Any]) -> None:
    """
    Валидация конфигурации
    
    Args:
        config: Словарь с конфигурацией
        
    Raises:
        ValueError: Если конфигурация некорректна
    """
    required_fields = [
        'api_key',
        'api_secret',
        'telegram_token',
        'telegram_chat_id',
        'trading_pairs'
    ]
    
    for field in required_fields:
        if not config.get(field):
            raise ValueError(f"Отсутствует обязательный параметр: {field}")
    
    if not isinstance(config['trading_pairs'], list):
        raise ValueError("trading_pairs должен быть списком")
    
    if not config['trading_pairs']:
        raise ValueError("Список trading_pairs пуст")
    
    if not isinstance(config.get('max_positions', 0), int) or config['max_positions'] <= 0:
        raise ValueError("max_positions должен быть положительным целым числом")
    
    if not isinstance(config.get('position_size', 0), (int, float)) or config['position_size'] <= 0:
        raise ValueError("position_size должен быть положительным числом")
    
    if not isinstance(config.get('leverage', 0), int) or config['leverage'] <= 0:
        raise ValueError("leverage должен быть положительным целым числом")

def load_config() -> Dict[str, Any]:
    """
    Загрузка конфигурации из переменных окружения
    
    Returns:
        Dict[str, Any]: Конфигурация
        
    Raises:
        ValueError: Если конфигурация некорректна
    """
    load_dotenv()
    
    config = {
        'api_key': os.getenv('BINANCE_API_KEY'),
        'api_secret': os.getenv('BINANCE_API_SECRET'),
        'telegram_token': os.getenv('TELEGRAM_BOT_TOKEN'),
        'telegram_chat_id': os.getenv('TELEGRAM_CHAT_ID'),
        'trading_pairs': ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'SOLUSDT', 'XRPUSDT'],
        'max_positions': int(os.getenv('MAX_POSITIONS', '3')),
        'position_size': float(os.getenv('POSITION_SIZE', '0.1')),
        'leverage': int(os.getenv('LEVERAGE', '1')),
        'min_order_volume': float(os.getenv('MIN_ORDER_VOLUME', '10')),
        'min_cluster_volume': 1000.0,  # Минимальный объем для значимого кластера
        'max_cluster_volume': float(os.getenv('MAX_CLUSTER_VOLUME', '10000')),
        'price_std_threshold': float(os.getenv('PRICE_STD_THRESHOLD', '0.02')),
        'spread_threshold': float(os.getenv('SPREAD_THRESHOLD', '0.001')),
        'min_cluster_size': int(os.getenv('MIN_CLUSTER_SIZE', '5')),
        'max_cluster_size': int(os.getenv('MAX_CLUSTER_SIZE', '50')),
        'min_balance': float(os.getenv('MIN_BALANCE', '100')),
        'log_level': os.getenv('LOG_LEVEL', 'INFO'),
        'log_file': os.getenv('LOG_FILE', 'logs/trading_bot.log')
    }
    
    try:
        validate_config(config)
        return config
    except ValueError as e:
        logger.error(f"Ошибка валидации конфигурации: {str(e)}")
        raise 