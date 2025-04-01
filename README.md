# Scalpion - Анализатор ликвидности криптовалютных бирж

Бот для анализа ликвидности на криптовалютных биржах, который отслеживает кластеры ликвидности и отправляет уведомления в Telegram.

## Основные возможности

- Анализ стакана ордеров в реальном времени
- Определение кластеров ликвидности
- Отслеживание изменений в кластерах
- Уведомления в Telegram
- Анализ дисбаланса стакана
- Определение уровней поддержки и сопротивления

## Установка

1. Клонируйте репозиторий:
```bash
git clone https://github.com/yourusername/scalpion.git
cd scalpion
```

2. Создайте виртуальное окружение и активируйте его:
```bash
python -m venv venv
source venv/bin/activate  # для Linux/Mac
venv\Scripts\activate  # для Windows
```

3. Установите зависимости:
```bash
pip install -r requirements.txt
```

4. Создайте файл конфигурации `config.json`:
```json
{
    "binance_api_key": "your_api_key",
    "binance_api_secret": "your_api_secret",
    "telegram_token": "your_telegram_bot_token",
    "telegram_chat_id": "your_chat_id",
    "symbols": ["BTCUSDT", "ETHUSDT"],
    "min_order_volume": 1000,
    "min_cluster_volume": 150000,
    "max_cluster_volume": 1000000,
    "price_std_threshold": 0.0005,
    "spread_threshold": 0.001,
    "min_cluster_size": 3,
    "max_cluster_size": 50
}
```

## Использование

1. Запустите бота:
```bash
python main.py
```

2. Бот начнет анализировать стаканы указанных торговых пар и отправлять уведомления в Telegram при обнаружении значимых кластеров ликвидности.

## Структура проекта

```
scalpion/
├── main.py                 # Основной файл запуска
├── market_analyzer.py      # Анализатор рынка
├── trading_strategy.py     # Торговая стратегия
├── utils/
│   ├── __init__.py
│   ├── telegram_notifier.py  # Уведомления в Telegram
│   └── websocket_manager.py  # Менеджер WebSocket соединений
├── config.json            # Конфигурация
└── requirements.txt       # Зависимости
```

## Требования

- Python 3.8+
- Binance API ключи
- Telegram Bot Token
- Установленные зависимости из requirements.txt

## Лицензия

MIT License 