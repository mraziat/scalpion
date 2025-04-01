"""
Тестовый скрипт для проверки анализа стакана
"""
import asyncio
import logging
from binance.client import Client
from market_analyzer import MarketAnalyzer
import pandas as pd
import numpy as np

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def test_orderbook_analysis():
    # Инициализация клиента Binance без API ключей
    client = Client()
    
    # Инициализация анализатора с пустой конфигурацией
    analyzer = MarketAnalyzer({})
    
    # Список пар для тестирования
    symbols = ['BTCUSDT', 'ETHUSDT', 'XRPUSDT']
    
    for symbol in symbols:
        try:
            logger.info(f"\nАнализ стакана для {symbol}")
            
            # Получаем данные стакана
            depth = await analyzer.get_orderbook(symbol)
            if not depth:
                logger.error(f"Не удалось получить данные стакана для {symbol}")
                continue
            
            # Получаем метрики стакана
            metrics = analyzer.get_metrics(symbol)
            logger.info(f"\nМетрики стакана для {symbol}:")
            logger.info(f"Лучший бид: {metrics.get('best_bid', 'N/A')}")
            logger.info(f"Лучший аск: {metrics.get('best_ask', 'N/A')}")
            logger.info(f"Спред: {metrics.get('spread', 'N/A')}")
            logger.info(f"Объем бидов: {metrics.get('bid_volume', 'N/A')}")
            logger.info(f"Объем асков: {metrics.get('ask_volume', 'N/A')}")
            logger.info(f"Дисбаланс: {metrics.get('imbalance', 'N/A')}")
            
            # Получаем кластеры ликвидности
            clusters = await analyzer._update_liquidity_clusters(symbol)
            
            if clusters:
                logger.info(f"\nНайдены кластеры для {symbol}:")
                for cluster in clusters:
                    logger.info(f"Цена: {cluster['price']}")
                    logger.info(f"Объем: {cluster['volume_usdt']} USDT")
                    logger.info(f"Сторона: {cluster['side']}")
                    logger.info(f"Количество ордеров: {cluster['orders']}")
                    logger.info("---")
            else:
                logger.info(f"\nКластеры для {symbol} не найдены")
            
            # Получаем ближайшие уровни
            current_price = float(client.get_symbol_ticker(symbol=symbol)['price'])
            levels = await analyzer.get_nearest_levels(symbol, current_price)
            logger.info(f"\nБлижайшие уровни для {symbol}:")
            logger.info(f"Поддержка: {levels.get('support', 'N/A')}")
            logger.info(f"Сопротивление: {levels.get('resistance', 'N/A')}")
            
        except Exception as e:
            logger.error(f"Ошибка при анализе {symbol}: {e}")
        
        # Небольшая задержка между парами
        await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.run(test_orderbook_analysis()) 