#!/usr/bin/env python
# coding: utf-8


"""Скрипт по перепаковке архивов из Диадока и СБИСа."""
import os
import logging
import datetime as DT
import tempfile
import zipfile
from shutil import copyfile
from os.path import join, dirname, exists
from dotenv import load_dotenv

# Загрузка переменных окружения
dotenv_path = join(dirname(__file__), '.env')
load_dotenv(dotenv_path)


def unpack_zip(archive_file: str) -> None:
    '''Распаковка архива.'''
    with zipfile.ZipFile(archive_file, 'r') as zip_file:
        for name in zip_file.namelist():
            unicode_name = name.encode('cp437').decode('cp866')
            # не декодированное имя - для чтения в архиве
            with zip_file.open(name) as f0:
                content = f0.read()
                fullpath = join(dirname(archive_file), unicode_name)
                if not exists(dirname(fullpath)):
                    os.makedirs(dirname(fullpath))
                with open(fullpath, 'wb') as f1:
                    f1.write(content)


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
    '''Обработка папки-буфера с выгруженными из Диадока и СБИСа архивами.'''
    logger.info('------------Старт обработки------------')
    for supplier_path in os.listdir(BUFFER_DIR): # проход по папкам поставщиков
        full_supplier_path = join(BUFFER_DIR, supplier_path)
        if os.path.isdir(full_supplier_path): # проверка, что это именно папка, а не файл

            # проход по файлам внутри папок поставщиков
            for archive_file in os.listdir(full_supplier_path):
                full_archive_file = join(full_supplier_path, archive_file)
                if os.path.isfile(full_archive_file) and \
                    archive_file.lower().endswith('.zip'): # проверка условий

                    # если в папке есть не обработанные архивы,
                    # тогда и показываем, что делаем обработку папки
                    logger.info("Обработка папки %s", supplier_path)
                    print(f"----------{supplier_path}----------")

                    logger.info("Распаковка файла %s", archive_file)
                    print(f"Распаковка файла {archive_file}")

                    # распаковка архива во временную папку
                    with tempfile.TemporaryDirectory() as tmpdirname:
                        _tmp_archive_file = join(tmpdirname, archive_file)

                        # копирование архива во временную папку
                        copyfile(full_archive_file, _tmp_archive_file)
                        unpack_zip(_tmp_archive_file) # распаковка
                        os.remove(_tmp_archive_file) # удаление архива
                        for doc_dir in os.listdir(tmpdirname): # перебор папок с документами
                            print(doc_dir)


# загружаем основной путь к папке с архивами
MAIN_DOC_DIR = os.environ.get("MAIN_DOC_DIR")
BUFFER_DIR = join(MAIN_DOC_DIR, 'Буфер')

logger = get_logger()
processing_buffer()
