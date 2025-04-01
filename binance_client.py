from binance.client import Client
from binance.websockets import BinanceSocketManager
from binance.enums import *
import pandas as pd
import os
from dotenv import load_dotenv
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BinanceDataClient:
    def __init__(self):
        load_dotenv()
        self.api_key = os.getenv('BINANCE_API_KEY')
        self.api_secret = os.getenv('BINANCE_API_SECRET')
        self.client = Client(self.api_key, self.api_secret)
        self.bm = BinanceSocketManager(self.client)
        
    def get_klines(self, symbol, interval, limit=100):
        """Получение исторических свечей"""
        try:
            klines = self.client.get_klines(
                symbol=symbol,
                interval=interval,
                limit=limit
            )
            
            df = pd.DataFrame(klines, columns=[
                'timestamp', 'open', 'high', 'low', 'close', 'volume',
                'close_time', 'quote_volume', 'trades', 'taker_buy_base',
                'taker_buy_quote', 'ignore'
            ])
            
            # Конвертируем типы данных
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            for col in ['open', 'high', 'low', 'close', 'volume']:
                df[col] = df[col].astype(float)
                
            return df
            
        except Exception as e:
            logger.error(f"Ошибка при получении свечей для {symbol}: {e}")
            return None

    def start_depth_socket(self, symbol, callback):
        """Запуск WebSocket для получения данных стакана"""
        try:
            conn_key = self.bm.start_depth_socket(
                symbol.lower(),
                callback,
                depth=BinanceSocketManager.WEBSOCKET_DEPTH_1000
            )
            self.bm.start()
            return conn_key
        except Exception as e:
            logger.error(f"Ошибка при запуске WebSocket для {symbol}: {e}")
            return None

    def stop_socket(self, conn_key):
        """Остановка WebSocket соединения"""
        try:
            self.bm.stop_socket(conn_key)
        except Exception as e:
            logger.error(f"Ошибка при остановке WebSocket: {e}")

    def close(self):
        """Закрытие всех соединений"""
        try:
            self.bm.close()
        except Exception as e:
            logger.error(f"Ошибка при закрытии соединений: {e}") 