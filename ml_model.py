import numpy as np
import pandas as pd
import tensorflow as tf
from tensorflow.keras import layers, Model
from typing import Dict, List, Tuple, Optional
import logging
from datetime import datetime, timedelta
import joblib
import os
import json
from sklearn.preprocessing import StandardScaler
import traceback

logger = logging.getLogger(__name__)

@tf.keras.utils.register_keras_serializable()
class AttentionLayer(layers.Layer):
    def __init__(self, **kwargs):
        super(AttentionLayer, self).__init__(**kwargs)
        self.attention = layers.Dense(1)
        self.softmax = layers.Softmax(axis=1)
        
    def call(self, inputs):
        # inputs shape: (batch_size, seq_len, features)
        attention_weights = self.attention(inputs)
        attention_weights = self.softmax(attention_weights)
        context = tf.reduce_sum(attention_weights * inputs, axis=1)
        return context, attention_weights

class PricePredictionModel:
    def __init__(self, sequence_length: int = 100, feature_dim: int = 10):
        """
        Инициализация модели предсказания цены
        
        Args:
            sequence_length: Длина последовательности для LSTM
            feature_dim: Размерность входных признаков
        """
        self.sequence_length = sequence_length
        self.feature_dim = feature_dim
        self.model = self._build_model()
        self.scaler = None
        self.last_training_time = None
        self.training_interval = timedelta(hours=1)
        self.training_data = []
        self.min_training_samples = 1000
        self.max_training_samples = 10000
        
    def _build_model(self) -> Model:
        """Построение архитектуры модели"""
        # Входной слой
        inputs = layers.Input(shape=(self.sequence_length, self.feature_dim))
        
        # LSTM слои с attention
        x = layers.LSTM(128, return_sequences=True)(inputs)
        x = layers.LSTM(64, return_sequences=True)(x)
        
        # Attention слой
        attention, attention_weights = AttentionLayer()(x)
        
        # Dense слои
        x = layers.Dense(64, activation='relu')(attention)
        x = layers.Dropout(0.2)(x)
        
        # Выходные слои
        probability = layers.Dense(1, activation='sigmoid', name='probability')(x)
        position_size = layers.Dense(1, activation='sigmoid', name='position_size')(x)
        
        model = Model(inputs=inputs, outputs=[probability, position_size])
        
        model.compile(
            optimizer='adam',
            loss={
                'probability': 'binary_crossentropy',
                'position_size': tf.keras.losses.MeanSquaredError()
            },
            metrics={
                'probability': 'accuracy',
                'position_size': tf.keras.metrics.MeanAbsoluteError()
            }
        )
        
        return model
    
    def prepare_features(self, data: Dict) -> np.ndarray:
        """
        Подготовка признаков для модели
        
        Args:
            data: Словарь с данными
            
        Returns:
            np.ndarray: Признаки для модели
        """
        try:
            features = []
            
            # Признаки из стакана
            orderbook_features = self._calculate_orderbook_density(data['orderbook'])
            features.extend(orderbook_features)
            
            # Признаки из кластеров ликвидности
            cluster_features = self._calculate_cluster_features(data['liquidity_clusters'])
            features.extend(cluster_features)
            
            # Признаки из исторических данных
            if 'historical' in data:
                historical_features = self._extract_historical_patterns(data['historical'])
                features.extend(historical_features)
            
            # Нормализация признаков
            if self.scaler is None:
                self.scaler = StandardScaler()
                features = self.scaler.fit_transform(np.array(features).reshape(1, -1))
            else:
                features = self.scaler.transform(np.array(features).reshape(1, -1))
            
            # Преобразуем в трехмерный массив (batch_size, sequence_length, feature_dim)
            features = np.tile(features, (1, self.sequence_length, 1))
            
            return features
            
        except Exception as e:
            logger.error(f"Ошибка при подготовке признаков: {e}")
            logger.error("Traceback:", exc_info=True)
            return None
    
    def _calculate_orderbook_density(self, orderbook: pd.DataFrame) -> List[float]:
        """Расчет плотности стакана"""
        try:
            # Разделяем биды и аски
            bids = orderbook[orderbook['side'] == 'bid']
            asks = orderbook[orderbook['side'] == 'ask']
            
            # Рассчитываем плотность
            bid_density = len(bids) / (bids['price'].max() - bids['price'].min()) if not bids.empty else 0
            ask_density = len(asks) / (asks['price'].max() - asks['price'].min()) if not asks.empty else 0
            
            # Рассчитываем дисбаланс
            bid_volume = bids['quantity'].sum() if not bids.empty else 0
            ask_volume = asks['quantity'].sum() if not asks.empty else 0
            volume_imbalance = (bid_volume - ask_volume) / (bid_volume + ask_volume) if (bid_volume + ask_volume) > 0 else 0
            
            # Рассчитываем спред
            best_bid = bids['price'].max() if not bids.empty else 0
            best_ask = asks['price'].min() if not asks.empty else 0
            spread = (best_ask - best_bid) / best_bid if best_bid > 0 else 0
            
            # Общий объем
            total_volume = bid_volume + ask_volume
            
            return [bid_density, ask_density, volume_imbalance, spread, total_volume]
            
        except Exception as e:
            logger.error(f"Ошибка при расчете плотности стакана: {e}")
            return [0, 0, 0, 0, 0]
    
    def _calculate_cluster_features(self, clusters: List[Dict]) -> List[float]:
        """Расчет признаков из кластеров ликвидности"""
        try:
            if not clusters:
                return [0, 0, 0, 0, 0]
                
            # Разделяем кластеры по сторонам
            bid_clusters = [c for c in clusters if c['side'] == 'bid']
            ask_clusters = [c for c in clusters if c['side'] == 'ask']
            
            # Рассчитываем средний объем кластеров
            bid_volume = np.mean([c['quantity'] for c in bid_clusters]) if bid_clusters else 0
            ask_volume = np.mean([c['quantity'] for c in ask_clusters]) if ask_clusters else 0
            
            # Рассчитываем количество кластеров
            bid_count = len(bid_clusters)
            ask_count = len(ask_clusters)
            
            # Рассчитываем среднюю цену кластеров
            bid_price = np.mean([c['price'] for c in bid_clusters]) if bid_clusters else 0
            ask_price = np.mean([c['price'] for c in ask_clusters]) if ask_clusters else 0
            
            return [bid_volume, ask_volume, bid_count, ask_count, (bid_price + ask_price) / 2]
            
        except Exception as e:
            logger.error(f"Ошибка при расчете признаков кластеров: {e}")
            return [0, 0, 0, 0, 0]
    
    def _extract_historical_patterns(self, historical: List[Dict]) -> List[float]:
        """Извлечение паттернов из исторических данных"""
        try:
            if not historical:
                return [0, 0, 0]
                
            # Рассчитываем волатильность
            prices = [h['price'] for h in historical]
            volatility = np.std(prices) / np.mean(prices) if prices else 0
            
            # Рассчитываем тренд
            if len(prices) > 1:
                trend = (prices[-1] - prices[0]) / prices[0]
            else:
                trend = 0
                
            # Рассчитываем объем
            volume = np.mean([h['quantity'] for h in historical]) if historical else 0
            
            return [volatility, trend, volume]
            
        except Exception as e:
            logger.error(f"Ошибка при извлечении исторических паттернов: {e}")
            return [0, 0, 0]
    
    def train(self, X: np.ndarray, y_probability: np.ndarray, y_position: np.ndarray) -> None:
        """
        Обучение модели
        
        Args:
            X: Признаки
            y_probability: Целевая переменная для вероятности
            y_position: Целевая переменная для размера позиции
        """
        try:
            # Добавляем новые данные в обучающую выборку
            self.training_data.append((X, y_probability, y_position))
            
            # Ограничиваем размер обучающей выборки
            if len(self.training_data) > self.max_training_samples:
                self.training_data = self.training_data[-self.max_training_samples:]
            
            # Проверяем, нужно ли обучать модель
            if (len(self.training_data) >= self.min_training_samples and
                (self.last_training_time is None or
                 datetime.now() - self.last_training_time > self.training_interval)):
                
                # Подготавливаем данные для обучения
                X_train = np.array([x[0] for x in self.training_data])
                y_probability_train = np.array([x[1] for x in self.training_data])
                y_position_train = np.array([x[2] for x in self.training_data])
                
                # Обучаем модель
                self.model.fit(
                    X_train,
                    {
                        'probability': y_probability_train,
                        'position_size': y_position_train
                    },
                    epochs=10,
                    batch_size=32,
                    validation_split=0.2,
                    verbose=0
                )
                
                self.last_training_time = datetime.now()
                logger.info("Модель успешно обучена")
                
        except Exception as e:
            logger.error(f"Ошибка при обучении модели: {e}")
            logger.error("Traceback:", exc_info=True)
            
    async def predict(self, orderbook_data: pd.DataFrame) -> Dict[str, float]:
        """Получение предсказания от модели"""
        try:
            # Подготавливаем данные
            features = self.prepare_features(orderbook_data)
            
            # Получаем предсказание
            prediction = self.model.predict(features)
            
            # Извлекаем вероятность и размер позиции
            probability = float(prediction[0][0])
            position_size = float(prediction[0][1])
            
            # Логируем предсказание
            logger.info(f"\n{'='*50}")
            logger.info("Предсказание ML-модели:")
            logger.info(f"Вероятность движения: {probability:.2%}")
            logger.info(f"Рекомендуемый размер позиции: {position_size:.2%}")
            logger.info(f"{'='*50}\n")
            
            return {
                'probability': probability,
                'position_size': position_size
            }
            
        except Exception as e:
            logger.error(f"Ошибка при получении предсказания: {e}")
            logger.error(traceback.format_exc())
            return {
                'probability': 0.0,
                'position_size': 0.0
            }
    
    def save_model(self, path: str) -> None:
        """
        Сохранение модели
        
        Args:
            path: Путь для сохранения
        """
        try:
            # Сохраняем модель
            self.model.save(path)
            
            # Получаем директорию из пути к файлу
            model_dir = os.path.dirname(path)
            
            # Сохраняем скейлер
            if self.scaler is not None:
                joblib.dump(self.scaler, os.path.join(model_dir, 'scaler.pkl'))
            
            # Сохраняем метаданные
            metadata = {
                'sequence_length': self.sequence_length,
                'feature_dim': self.feature_dim,
                'last_training_time': self.last_training_time.isoformat() if self.last_training_time else None,
                'training_interval': self.training_interval.total_seconds()
            }
            
            with open(os.path.join(model_dir, 'metadata.json'), 'w') as f:
                json.dump(metadata, f)
                
            logger.info(f"Модель успешно сохранена в {path}")
            
        except Exception as e:
            logger.error(f"Ошибка при сохранении модели: {e}")
            logger.error("Traceback:", exc_info=True)
    
    def load_model(self, path: str) -> None:
        """
        Загрузка сохраненной модели
        
        Args:
            path: Путь к файлу модели
        """
        try:
            # Регистрируем пользовательский слой
            tf.keras.utils.get_custom_objects()['AttentionLayer'] = AttentionLayer
            
            # Загружаем модель
            self.model = tf.keras.models.load_model(path)
            logger.info("Модель успешно загружена")
            
        except Exception as e:
            logger.error(f"Ошибка при загрузке модели: {e}")
            logger.error("Traceback:", exc_info=True)
            raise 