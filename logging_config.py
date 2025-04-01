import logging
import sys

# Настраиваем логирование
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

# Создаем логгеры для разных компонентов
orderbook_logger = logging.getLogger('orderbook')
trading_logger = logging.getLogger('trading')
level_logger = logging.getLogger('level') 