import logging
from typing import Dict, List, Optional, Tuple
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from binance.client import Client
from binance.enums import *
import asyncio
import json
import os
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor
import matplotlib.pyplot as plt
import seaborn as sns

from orderbook_processor import OrderBookProcessor
from level_analyzer import LevelAnalyzer
from price_prediction import PricePredictionModel
from trading_strategy import TradingStrategy
from risk_manager import RiskManager
from utils import calculate_volatility, calculate_slippage, calculate_trend

logger = logging.getLogger(__name__)

@dataclass
class TradeResult:
    """Результат сделки"""
    symbol: str
    entry_time: datetime
    exit_time: datetime
    direction: str
    entry_price: float
    exit_price: float
    position_size: float
    pnl: float
    market_context: Dict
    strategy_version: str

class Backtest:
    def __init__(self, client: Client, initial_capital: float = 10000, config: Dict = None):
        """
        Инициализация бэктестера
        
        Args:
            client: Клиент Binance
            initial_capital: Начальный капитал
            config: Конфигурация для компонентов
        """
        self.client = client
        self.initial_capital = initial_capital
        self.current_capital = initial_capital
        self.trades: List[TradeResult] = []
        self.strategy_versions: Dict[str, Dict] = {}
        self.results: Dict[str, Dict] = {}
        self.config = config or {}
        
        # Инициализируем компоненты
        self.orderbook_processor = OrderBookProcessor()
        self.level_analyzer = LevelAnalyzer(self.config)
        self.ml_model = PricePredictionModel()
        self.trading_strategy = TradingStrategy(self.config)
        self.risk_manager = RiskManager(self.client, self.initial_capital)
        
        # Метрики для оценки
        self.min_profit_factor = 1.5
        self.max_drawdown = 0.10
        self.min_sharpe_ratio = 1.2
        
    def run_backtest(
        self,
        symbol: str,
        start_date: datetime,
        end_date: datetime,
        test_type: str = "backtest",
        save_path: Optional[str] = None
    ) -> Dict:
        """
        Запуск бэктеста
        
        Args:
            symbol: Торговая пара
            start_date: Начальная дата
            end_date: Конечная дата
            test_type: Тип тестирования
            save_path: Путь для сохранения графиков
            
        Returns:
            Dict: Результаты тестирования
        """
        try:
            logger.info(f"Начало {test_type} для {symbol}")
            logger.info(f"Период: {start_date} - {end_date}")
            
            # Получаем исторические данные
            historical_data = self._get_historical_data(symbol, start_date, end_date)
            if historical_data.empty:
                logger.error("Не удалось получить исторические данные")
                return {}
            
            logger.info(f"Получено {len(historical_data)} свечей")
            
            # Инициализируем метрики
            self.initial_capital = self.config.get('initial_capital', 10000)
            self.current_capital = self.initial_capital
            self.positions = []
            self.trades = []
            self.equity_curve = pd.DataFrame(columns=['timestamp', 'equity'])
            
            # Проходим по историческим данным
            for index, candle in historical_data.iterrows():
                try:
                    # Симулируем стакан
                    orderbook_data = self._simulate_orderbook(candle)
                    if orderbook_data.empty:
                        logger.warning(f"Пустой стакан для свечи {index}")
                        continue
                    
                    # Анализируем сигналы
                    signals = self._analyze_signals(
                        symbol,
                        candle,
                        orderbook_data,
                        self.orderbook_processor,
                        self.level_analyzer,
                        self.ml_model
                    )
                    
                    if signals is not None:
                        trade_result = self._simulate_trade(
                            symbol,
                            signals,
                            candle,
                            historical_data.iloc[index + 1],
                            test_type
                        )
                        
                        if trade_result:
                            self.trades.append(trade_result)
                            self.current_capital += trade_result.pnl
                    
                    # Обновляем кривую капитала
                    self.equity_curve = pd.concat([
                        self.equity_curve,
                        pd.DataFrame({
                            'timestamp': [index],
                            'equity': [self.current_capital]
                        })
                    ])
                    
                except Exception as e:
                    logger.error(f"Ошибка при обработке свечи {index}: {e}")
                    logger.error("Traceback:", exc_info=True)
                    continue
            
            # Рассчитываем результаты
            results = self._calculate_results()
            
            # Сохраняем графики
            if save_path:
                self.plot_results(save_path)
            
            logger.info(f"Завершение {test_type}")
            return results
            
        except Exception as e:
            logger.error(f"Ошибка при выполнении {test_type}: {e}")
            logger.error("Traceback:", exc_info=True)
            return {}
    
    async def run_forward_test(self, symbol: str, duration: int = 14) -> Dict:
        """
        Запуск форвард-теста
        
        Args:
            symbol: Торговая пара
            duration: Длительность в днях
            
        Returns:
            Dict: Результаты форвард-теста
        """
        try:
            logger.info(f"Запуск форвард-теста для {symbol} на {duration} дней")
            
            # Получаем текущие данные
            end_date = datetime.now()
            start_date = end_date - timedelta(days=duration)
            
            # Запускаем тест
            results = await self.run_backtest(symbol, start_date, end_date, "forward_test")
            
            # Сравниваем с бэктестом
            if "backtest" in self.results:
                comparison = self._compare_results(
                    self.results["backtest"],
                    results
                )
                results["comparison"] = comparison
            
            return results
            
        except Exception as e:
            logger.error(f"Ошибка при запуске форвард-теста: {e}")
            return {}
    
    async def run_ab_test(self, symbol: str, start_date: datetime,
                         end_date: datetime, versions: List[Dict]) -> Dict:
        """
        Запуск A/B тестирования
        
        Args:
            symbol: Торговая пара
            start_date: Начальная дата
            end_date: Конечная дата
            versions: Список версий стратегии
            
        Returns:
            Dict: Результаты A/B тестирования
        """
        try:
            logger.info(f"Запуск A/B тестирования для {symbol}")
            
            # Сохраняем версии
            self.strategy_versions = {
                f"version_{i}": version for i, version in enumerate(versions)
            }
            
            # Запускаем тесты для каждой версии
            results = {}
            for version_name, version_config in self.strategy_versions.items():
                # Обновляем конфигурацию
                self.config.update(version_config)
                
                # Реинициализируем компоненты с новой конфигурацией
                self.level_analyzer = LevelAnalyzer(self.config)
                self.trading_strategy = TradingStrategy(self.config)
                
                # Запускаем бэктест
                version_results = await self.run_backtest(symbol, start_date, end_date, version_name)
                results[version_name] = version_results
            
            # Находим лучшую версию
            best_version = max(results.items(), key=lambda x: x[1].get('metrics', {}).get('profit_factor', 0))[0]
            
            return {
                'results': results,
                'best_version': best_version,
                'metrics': results[best_version]
            }
            
        except Exception as e:
            logger.error(f"Ошибка при анализе A/B результатов: {e}")
            return {}
    
    def _get_historical_data(self, symbol: str, start_time: datetime, end_time: datetime) -> pd.DataFrame:
        """
        Получение исторических данных
        
        Args:
            symbol: Торговая пара
            start_time: Начальная дата
            end_time: Конечная дата
            
        Returns:
            pd.DataFrame: Исторические данные
        """
        try:
            # Форматируем даты
            start_str = start_time.strftime('%Y-%m-%d %H:%M:%S')
            end_str = end_time.strftime('%Y-%m-%d %H:%M:%S')
            
            # Получаем часовые свечи
            klines = self.client.futures_historical_klines(
                symbol=symbol,
                interval=Client.KLINE_INTERVAL_1HOUR,
                start_str=start_str,
                end_str=end_str
            )
            
            # Преобразуем в DataFrame
            df = pd.DataFrame(klines, columns=[
                'timestamp', 'open', 'high', 'low', 'close', 'volume',
                'close_time', 'quote_volume', 'trades', 'taker_buy_base',
                'taker_buy_quote', 'ignore'
            ])
            
            # Конвертируем типы данных
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            for col in ['open', 'high', 'low', 'close', 'volume']:
                df[col] = df[col].astype(float)
            
            # Устанавливаем timestamp как индекс
            df.set_index('timestamp', inplace=True)
            
            # Сохраняем данные в атрибут klines
            self.klines = df
            logger.info(f"Получены исторические данные для {symbol}: {len(df)} записей")
            
            return df
            
        except Exception as e:
            logger.error(f"Ошибка при получении исторических данных: {e}")
            logger.error("Traceback:", exc_info=True)
            return pd.DataFrame()
    
    def _simulate_orderbook(self, candle: pd.Series) -> pd.DataFrame:
        """
        Симуляция стакана на основе свечи
        
        Args:
            candle: Данные свечи
            
        Returns:
            pd.DataFrame: Симулированные данные стакана
        """
        try:
            # Создаем симулированные биды и аски
            price = float(candle['close'])
            volume = float(candle['volume'])
            
            # Генерируем 10 уровней цен
            bid_prices = np.linspace(price * 0.99, price * 0.999, 10)
            ask_prices = np.linspace(price * 1.001, price * 1.01, 10)
            
            # Генерируем объемы
            volumes = np.random.exponential(volume / 10, 10)
            
            # Формируем DataFrame
            bids_df = pd.DataFrame({
                'price': bid_prices,
                'quantity': volumes,
                'side': 'bid'
            })
            
            asks_df = pd.DataFrame({
                'price': ask_prices,
                'quantity': volumes,
                'side': 'ask'
            })
            
            return pd.concat([bids_df, asks_df])
            
        except Exception as e:
            logger.error(f"Ошибка при симуляции стакана: {e}")
            return pd.DataFrame(columns=['price', 'quantity', 'side'])
            
    def _analyze_signals(
        self,
        symbol: str,
        current_candle: pd.Series,
        orderbook_data: pd.DataFrame,
        orderbook_processor: OrderBookProcessor,
        level_analyzer: LevelAnalyzer,
        price_predictor: PricePredictionModel
    ) -> Dict:
        """
        Анализ сигналов
        
        Args:
            symbol: Торговая пара
            current_candle: Текущая свеча
            orderbook_data: Данные стакана
            orderbook_processor: Процессор стакана
            level_analyzer: Анализатор уровней
            price_predictor: Модель предсказания цен
            
        Returns:
            Dict: Сигналы
        """
        try:
            # Получаем предсказание цены
            price_prediction = price_predictor.predict(current_candle)
            ml_probability, ml_position_size = price_prediction
            
            # Анализируем сигналы
            signals = self.trading_strategy.analyze_signals(
                symbol=symbol,
                current_price=float(current_candle['close']),
                orderbook_data=orderbook_data,
                liquidity_clusters=orderbook_processor.get_liquidity_clusters(symbol),
                ml_probability=ml_probability,
                ml_position_size=ml_position_size
            )
            
            return signals
            
        except Exception as e:
            logger.error(f"Ошибка при анализе сигналов: {e}")
            logger.error("Traceback:", exc_info=True)
            return None
            
    def _simulate_trade(self, symbol: str, signal: Dict,
                       entry_candle: pd.Series, exit_candle: pd.Series,
                       strategy_version: str) -> Optional[TradeResult]:
        """
        Симуляция сделки
        
        Args:
            symbol: Торговая пара
            signal: Торговый сигнал
            entry_candle: Свеча входа
            exit_candle: Свеча выхода
            strategy_version: Версия стратегии
            
        Returns:
            Optional[TradeResult]: Результат сделки
        """
        try:
            # Рассчитываем проскальзывание
            slippage = calculate_slippage(signal['entry_price'], entry_candle)
            
            # Корректируем цену входа
            entry_price = signal['entry_price'] * (1 + slippage)
            
            # Определяем цену выхода
            if signal['direction'] == SIDE_BUY:
                exit_price = min(exit_candle['high'], signal['take_profit'])
                if exit_candle['low'] <= signal['stop_loss']:
                    exit_price = signal['stop_loss']
            else:
                exit_price = max(exit_candle['low'], signal['take_profit'])
                if exit_candle['high'] >= signal['stop_loss']:
                    exit_price = signal['stop_loss']
            
            # Рассчитываем P&L
            if signal['direction'] == SIDE_BUY:
                pnl = (exit_price - entry_price) / entry_price * signal['position_size']
            else:
                pnl = (entry_price - exit_price) / entry_price * signal['position_size']
            
            return TradeResult(
                symbol=symbol,
                entry_time=entry_candle['timestamp'],
                exit_time=exit_candle['timestamp'],
                direction=signal['direction'],
                entry_price=entry_price,
                exit_price=exit_price,
                position_size=signal['position_size'],
                pnl=pnl,
                market_context=signal['market_context'],
                strategy_version=strategy_version
            )
            
        except Exception as e:
            logger.error(f"Ошибка при симуляции сделки: {e}")
            return None
            
    def _calculate_volatility(self, candle: pd.Series) -> float:
        """
        Расчет волатильности
        
        Args:
            candle: Свеча
            
        Returns:
            float: Волатильность
        """
        try:
            return (candle['high'] - candle['low']) / candle['low']
            
        except Exception as e:
            logger.error(f"Ошибка при расчете волатильности: {e}")
            return 0.0
            
    def _calculate_trend(self, candle: pd.Series) -> str:
        """
        Расчет тренда
        
        Args:
            candle: Свеча
            
        Returns:
            str: Направление тренда
        """
        try:
            if candle['close'] > candle['open']:
                return 'up'
            elif candle['close'] < candle['open']:
                return 'down'
            else:
                return 'sideways'
                
        except Exception as e:
            logger.error(f"Ошибка при расчете тренда: {e}")
            return 'sideways'
    
    def _calculate_metrics(self) -> Dict:
        """Расчет метрик бэктеста"""
        try:
            if not self.trades:
                return {
                    'total_trades': 0,
                    'winning_trades': 0,
                    'losing_trades': 0,
                    'win_rate': 0.0,
                    'profit_factor': 0.0,
                    'max_drawdown': 0.0,
                    'sharpe_ratio': 0.0,
                    'total_pnl': 0.0,
                    'average_pnl': 0.0,
                    'max_pnl': 0.0,
                    'min_pnl': 0.0
                }
            
            # Преобразуем сделки в DataFrame
            trades_df = pd.DataFrame([t.__dict__ for t in self.trades])
            
            # Рассчитываем метрики
            total_trades = len(trades_df)
            winning_trades = len(trades_df[trades_df['pnl'] > 0])
            losing_trades = len(trades_df[trades_df['pnl'] < 0])
            win_rate = winning_trades / total_trades if total_trades > 0 else 0.0
            
            # Рассчитываем P&L
            total_pnl = trades_df['pnl'].sum()
            average_pnl = trades_df['pnl'].mean()
            max_pnl = trades_df['pnl'].max()
            min_pnl = trades_df['pnl'].min()
            
            # Рассчитываем profit factor
            gross_profit = trades_df[trades_df['pnl'] > 0]['pnl'].sum()
            gross_loss = abs(trades_df[trades_df['pnl'] < 0]['pnl'].sum())
            profit_factor = gross_profit / gross_loss if gross_loss > 0 else float('inf')
            
            # Рассчитываем максимальную просадку
            cumulative_pnl = trades_df['pnl'].cumsum()
            rolling_max = cumulative_pnl.expanding().max()
            drawdowns = (cumulative_pnl - rolling_max) / rolling_max
            max_drawdown = abs(drawdowns.min())
            
            # Рассчитываем коэффициент Шарпа
            returns = trades_df['pnl'].pct_change().dropna()
            if len(returns) > 0:
                sharpe_ratio = np.sqrt(252) * returns.mean() / returns.std()
            else:
                sharpe_ratio = 0.0
            
            return {
                'total_trades': total_trades,
                'winning_trades': winning_trades,
                'losing_trades': losing_trades,
                'win_rate': win_rate,
                'profit_factor': profit_factor,
                'max_drawdown': max_drawdown,
                'sharpe_ratio': sharpe_ratio,
                'total_pnl': total_pnl,
                'average_pnl': average_pnl,
                'max_pnl': max_pnl,
                'min_pnl': min_pnl
            }
            
        except Exception as e:
            logger.error(f"Ошибка при расчете метрик: {e}")
            return {}
    
    def _compare_results(self, backtest_results: Dict,
                        forward_results: Dict) -> Dict:
        """Сравнение результатов бэктеста и форвард-теста"""
        try:
            comparison = {}
            
            for metric in backtest_results['metrics']:
                backtest_value = backtest_results['metrics'][metric]
                forward_value = forward_results['metrics'][metric]
                
                if isinstance(backtest_value, (int, float)):
                    comparison[metric] = {
                        'backtest': backtest_value,
                        'forward': forward_value,
                        'difference': forward_value - backtest_value,
                        'difference_percent': (forward_value - backtest_value) / backtest_value
                    }
            
            return comparison
            
        except Exception as e:
            logger.error(f"Ошибка при сравнении результатов: {e}")
            return {}
    
    def plot_results(self, save_path: str) -> None:
        """
        Построение графиков результатов бэктеста
        
        Args:
            save_path: Путь для сохранения графиков
        """
        try:
            logger.info("Начало построения графиков")
            
            # Создаем директорию для сохранения графиков
            os.makedirs(os.path.dirname(save_path), exist_ok=True)
            logger.info(f"Создана директория для сохранения графиков: {save_path}")
            
            # Получаем исторические данные
            historical_data = self.klines
            logger.info(f"Найдено {len(historical_data)} записей исторических данных")
            
            # Строим график цены и уровней
            plt.figure(figsize=(15, 8))
            plt.plot(historical_data.index, historical_data['close'], label='Цена')
            
            # Добавляем уровни поддержки и сопротивления
            if hasattr(self.level_analyzer, 'historical_levels') and self.level_analyzer.historical_levels:
                logger.info(f"Найдено {len(self.level_analyzer.historical_levels)} исторических уровней")
                for level in self.level_analyzer.historical_levels:
                    if level['type'] == 'support':
                        plt.axhline(y=level['price'], color='g', linestyle='--', alpha=0.5)
                    else:
                        plt.axhline(y=level['price'], color='r', linestyle='--', alpha=0.5)
            else:
                logger.warning("Исторические уровни не найдены")
            
            plt.title('Цена и уровни')
            plt.xlabel('Время')
            plt.ylabel('Цена')
            plt.legend()
            plt.grid(True)
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
            plt.close()
            
            # Строим график доходности
            if hasattr(self, 'equity_curve') and isinstance(self.equity_curve, pd.DataFrame):
                plt.figure(figsize=(15, 8))
                plt.plot(self.equity_curve.index, self.equity_curve['equity'], label='Доходность')
                plt.title('Кривая доходности')
                plt.xlabel('Время')
                plt.ylabel('Доходность')
                plt.legend()
                plt.grid(True)
                plt.savefig(save_path.replace('.png', '_equity.png'), dpi=300, bbox_inches='tight')
                plt.close()
            else:
                logger.warning("Данные о доходности не найдены или имеют неверный формат")
            
        except Exception as e:
            logger.error(f"Ошибка при построении графиков: {e}")
            logger.error("Traceback:", exc_info=True)
    
    def _calculate_results(self) -> Dict:
        """
        Расчет результатов бэктеста
        
        Returns:
            Dict: Результаты бэктеста
        """
        try:
            metrics = self._calculate_metrics()
            
            return {
                'total_return': (self.current_capital - self.initial_capital) / self.initial_capital * 100,
                'max_drawdown': metrics['max_drawdown'] * 100,
                'profit_factor': metrics['profit_factor'],
                'sharpe_ratio': metrics['sharpe_ratio'],
                'win_rate': metrics['win_rate'] * 100,
                'total_trades': metrics['total_trades'],
                'winning_trades': metrics['winning_trades'],
                'losing_trades': metrics['losing_trades'],
                'average_pnl': metrics['average_pnl'],
                'max_pnl': metrics['max_pnl'],
                'min_pnl': metrics['min_pnl'],
                'equity_curve': self.equity_curve
            }
            
        except Exception as e:
            logger.error(f"Ошибка при расчете результатов: {e}")
            return {}
            
    def save_results(self, save_path: str) -> None:
        """
        Сохранение результатов бэктеста
        
        Args:
            save_path: Путь для сохранения результатов
        """
        try:
            # Создаем директорию для сохранения результатов
            os.makedirs(os.path.dirname(save_path), exist_ok=True)
            
            # Форматируем сделки для сохранения
            trades_data = []
            for trade in self.trades:
                trade_dict = {
                    'symbol': trade.symbol,
                    'entry_time': trade.entry_time.strftime('%Y-%m-%d %H:%M:%S'),
                    'exit_time': trade.exit_time.strftime('%Y-%m-%d %H:%M:%S'),
                    'direction': trade.direction,
                    'entry_price': float(trade.entry_price),
                    'exit_price': float(trade.exit_price),
                    'position_size': float(trade.position_size),
                    'pnl': float(trade.pnl),
                    'market_context': trade.market_context,
                    'strategy_version': trade.strategy_version
                }
                trades_data.append(trade_dict)
            
            # Форматируем результаты для сохранения
            results_data = {}
            for version, data in self.results.items():
                results_data[version] = {
                    'metrics': {
                        k: float(v) if isinstance(v, (int, float, np.number)) else v
                        for k, v in data['metrics'].items()
                    },
                    'trades': [
                        {
                            'entry_time': t.entry_time.strftime('%Y-%m-%d %H:%M:%S'),
                            'exit_time': t.exit_time.strftime('%Y-%m-%d %H:%M:%S'),
                            'pnl': float(t.pnl),
                            'direction': t.direction,
                            'entry_price': float(t.entry_price),
                            'exit_price': float(t.exit_price),
                            'position_size': float(t.position_size)
                        }
                        for t in data['trades']
                    ]
                }
            
            # Сохраняем результаты
            with open(save_path, 'w') as f:
                json.dump({
                    'trades': trades_data,
                    'results': results_data,
                    'parameters': self.config
                }, f, indent=4)
            
            logger.info(f"Результаты сохранены в {save_path}")
            
        except Exception as e:
            logger.error(f"Ошибка при сохранении результатов: {e}")
            logger.error("Traceback:", exc_info=True)

if __name__ == "__main__":
    # Настраиваем логирование
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('logs/backtest.log'),
            logging.StreamHandler()
        ]
    )
    
    # Создаем директорию для логов, если её нет
    os.makedirs('logs', exist_ok=True)
    
    # Инициализируем клиент Binance
    client = Client()
    
    # Конфигурация стратегии
    config = {
        'min_volume_ratio': 3.0,
        'min_confidence': 0.75,
        'position_size': 0.02,
        'max_positions': 3,
        'stop_loss_atr_multiplier': 2.0,
        'take_profit_atr_multiplier': 3.0
    }
    
    # Создаем бэктестер
    backtest = Backtest(client, initial_capital=10000, config=config)
    
    # Запускаем бэктест
    symbol = 'BTCUSDT'
    start_date = datetime(2024, 1, 1)
    end_date = datetime(2024, 3, 1)
    
    # Запускаем асинхронно
    loop = asyncio.get_event_loop()
    results = loop.run_until_complete(
        backtest.run_backtest(
            symbol,
            start_date,
            end_date,
            'base_strategy',
            'backtest_results.json'
        )
    )
    
    # Выводим результаты
    print("\nРезультаты бэктеста:")
    print(f"Всего сделок: {results['metrics']['total_trades']}")
    print(f"Выигрышных сделок: {results['metrics']['winning_trades']}")
    print(f"Проигрышных сделок: {results['metrics']['losing_trades']}")
    print(f"Винрейт: {results['metrics']['win_rate']:.2%}")
    print(f"Profit Factor: {results['metrics']['profit_factor']:.2f}")
    print(f"Максимальная просадка: {results['metrics']['max_drawdown']:.2%}")
    print(f"Коэффициент Шарпа: {results['metrics']['sharpe_ratio']:.2f}")
    print(f"Общий P&L: {results['metrics']['total_pnl']:.2f}")
    print(f"Средний P&L: {results['metrics']['average_pnl']:.2f}")
    print(f"Максимальный P&L: {results['metrics']['max_pnl']:.2f}")
    print(f"Минимальный P&L: {results['metrics']['min_pnl']:.2f}")
    print(f"Конечный капитал: {results['final_capital']:.2f}") 