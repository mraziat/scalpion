"""
Модуль для выбора торговых пар
"""
import logging
from typing import List, Dict
from binance.client import Client
import pandas as pd

logger = logging.getLogger(__name__)

class SymbolSelector:
    def __init__(self, client: Client):
        """
        Инициализация селектора торговых пар
        
        Args:
            client: Клиент Binance API
        """
        self.client = client
        self.stablecoins = ['USDT', 'BUSD', 'USDC', 'DAI', 'TUSD', 'FDUSD']
        self.min_volume = float(1000000)  # Минимальный объем за 24 часа
        self.min_price = float(1.0)  # Минимальная цена
        
    async def get_top_pairs(self, limit: int = 20) -> List[str]:
        """
        Получение топ N торговых пар к USDT по объему, исключая стейблкоины
        
        Args:
            limit: Количество пар
            
        Returns:
            List[str]: Список торговых пар
        """
        try:
            # Получаем информацию о всех парах
            tickers = self.client.get_ticker()
            
            # Фильтруем только USDT пары, исключаем стейблкоины и проверяем минимальный объем
            filtered_tickers = [
                t for t in tickers
                if (t['symbol'].endswith('USDT') and 
                    float(t['quoteVolume']) >= self.min_volume and
                    not any(t['symbol'].startswith(stable) for stable in self.stablecoins))
            ]
            
            # Сортируем по объему в USDT
            sorted_tickers = sorted(
                filtered_tickers,
                key=lambda x: float(x['quoteVolume']),
                reverse=True
            )
            
            # Берем топ N пар
            top_pairs = [t['symbol'] for t in sorted_tickers[:limit]]
            logger.info(f"Получены топ {limit} торговых пар по объему (без стейблкоинов): {top_pairs}")
            
            return top_pairs
            
        except Exception as e:
            logger.error(f"Ошибка при получении торговых пар: {e}")
            return []
            
    async def get_symbol_info(self, symbol: str) -> dict:
        """
        Получение информации о торговой паре
        
        Args:
            symbol: Торговая пара
            
        Returns:
            dict: Информация о паре
        """
        try:
            info = self.client.get_symbol_info(symbol)
            return info
            
        except Exception as e:
            logger.error(f"Ошибка при получении информации о паре {symbol}: {e}")
            return {}
    
    def filter_symbols(self, symbols: List[str], min_volume: float = None,
                      min_price: float = None) -> List[str]:
        """
        Фильтрация торговых пар по критериям
        
        Args:
            symbols: Список пар для фильтрации
            min_volume: Минимальный объем
            min_price: Минимальная цена
            
        Returns:
            List[str]: Отфильтрованный список пар
        """
        try:
            if not symbols:
                return []
                
            # Получаем информацию о парах
            tickers = self.client.get_ticker()
            df = pd.DataFrame(tickers)
            
            # Фильтруем только нужные пары
            df = df[df['symbol'].isin(symbols)]
            
            # Применяем фильтры
            if min_volume:
                df = df[df['quoteVolume'].astype(float) >= min_volume]
            if min_price:
                df = df[df['lastPrice'].astype(float) >= min_price]
            
            return df['symbol'].tolist()
            
        except Exception as e:
            logger.error(f"Ошибка при фильтрации торговых пар: {e}")
            return []
    
    async def get_trading_pairs(self) -> List[str]:
        """
        Получение списка торговых пар для анализа
        
        Returns:
            List[str]: Список торговых пар
        """
        try:
            # Получаем все спотовые пары
            spot_symbols = self.client.get_exchange_info()['symbols']
            spot_pairs = [s['symbol'] for s in spot_symbols if s['status'] == 'TRADING']
            
            # Получаем все фьючерсные пары
            futures_symbols = self.client.futures_exchange_info()['symbols']
            futures_pairs = [s['symbol'] for s in futures_symbols if s['status'] == 'TRADING']
            
            # Получаем объемы за 24 часа для всех пар
            volumes = {}
            for symbol in spot_pairs:
                try:
                    # Получаем статистику за 24 часа
                    ticker = self.client.get_ticker(symbol=symbol)
                    volume = float(ticker['quoteVolume'])  # Объем в USDT
                    
                    # Проверяем, что это не стейблкоин
                    if not any(symbol.startswith(stable) for stable in self.stablecoins):
                        volumes[symbol] = volume
                except Exception as e:
                    logger.error(f"Ошибка при получении объема для {symbol}: {e}")
                    continue
            
            # Сортируем пары по объему и берем топ-30
            sorted_pairs = sorted(volumes.items(), key=lambda x: x[1], reverse=True)
            top_pairs = [pair[0] for pair in sorted_pairs[:30]]
            
            # Фильтруем только те пары, у которых есть фьючерсы
            trading_pairs = [pair for pair in top_pairs if pair in futures_pairs]
            
            logger.info(f"Получены топ-30 пар по объему с фьючерсами: {trading_pairs}")
            return trading_pairs
            
        except Exception as e:
            logger.error(f"Ошибка при получении торговых пар: {e}")
            return [] 