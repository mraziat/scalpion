"""
Утилита для управления логами
"""
import logging
import os
from datetime import datetime
from typing import Dict, Any
import glob
from logging.handlers import RotatingFileHandler

def setup_logging(config: Dict[str, Any]) -> None:
    """
    Настройка логирования с ротацией файлов
    
    Args:
        config: Конфигурация с параметрами логирования
    """
    # Создаем директорию для логов если её нет
    log_dir = os.path.dirname(config['log_file'])
    if log_dir and not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    # Создаем имя файла с временной меткой
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    base_name = os.path.splitext(config['log_file'])[0]
    log_file = f"{base_name}_{timestamp}.log"
        
    # Настраиваем форматирование
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Настраиваем вывод в файл
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)
    
    # Настраиваем вывод в консоль
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    
    # Настраиваем корневой логгер
    root_logger = logging.getLogger()
    root_logger.setLevel(config['log_level'])
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
    
    # Очищаем старые логи
    log_files = glob.glob(os.path.join(log_dir, '*.log'))
    log_files.sort(key=os.path.getctime, reverse=True)
    
    # Оставляем только 5 последних файлов
    max_logs = 5
    for old_log in log_files[max_logs-1:]:
        try:
            os.remove(old_log)
            logging.info(f"Удален старый лог: {old_log}")
        except Exception as e:
            logging.error(f"Ошибка при удалении лога {old_log}: {e}")
    
    root_logger.info(f"Логирование настроено в файл: {log_file}") 