#!/usr/bin/env python
# coding: utf-8

import os
import logging
import datetime as DT
from os.path import join, dirname, exists
from dotenv import load_dotenv

# Загрузка переменных окружения
dotenv_path = join(dirname(__file__), '.env')
load_dotenv(dotenv_path)

def get_logger() -> logging.Logger:
    '''Инициализация логгера.'''

    _logger = logging.getLogger('repack_orem')
    _logger.setLevel(logging.INFO)

    #Если папки с логом нет, то создаем её
    if not exists('LOG'):
        os.makedirs('LOG')
    # создаем handler файла лога
    _fh = logging.FileHandler("LOG/{}_repack_orem.log".format(DT.date.today().strftime("%Y-%m-%d")))

    # задаем форматирование
    _formatter = logging.Formatter('%(asctime)s - %(levelname)s: %(message)s')
    _fh.setFormatter(_formatter)

    # добавляем handler в логгер
    _logger.addHandler(_fh)
    return _logger


def processing_buffer() -> None:
    '''Обработка папки-буфера с выгруженными из Диадока и СБИСа архивами'''
    logger.info('------------Старт обработки------------')
    for path in os.listdir(BUFFER_DIR):
        full_path = join(BUFFER_DIR, path)
        if os.path.isdir(full_path):
            logger.info(f"Обработка папки {path}")
            print(path)


# загружаем основной путь к папке с архивами
MAIN_DOC_DIR = os.environ.get("MAIN_DOC_DIR")
BUFFER_DIR = join(MAIN_DOC_DIR, 'Буфер')

logger = get_logger()
processing_buffer()