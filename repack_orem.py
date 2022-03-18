#!/usr/bin/env python
# coding: utf-8


"""Скрипт по перепаковке архивов из Диадока и СБИСа."""
import os
import logging
import datetime as DT
import tempfile
import zipfile
import re
import xml.etree.ElementTree as ET
from shutil import copyfile
from os.path import join, dirname, exists
from dotenv import load_dotenv

# Загрузка переменных окружения
dotenv_path = join(dirname(__file__), ".env")
load_dotenv(dotenv_path)


def unpack_zip(archive_file: str) -> None:
    """Распаковка архива."""
    with zipfile.ZipFile(archive_file, "r") as zip_file:
        for name in zip_file.namelist():
            unicode_name = name.encode("cp437").decode("cp866")
            # недекодированное имя - для чтения в архиве
            with zip_file.open(name) as f0:
                content = f0.read()
                fullpath = join(dirname(archive_file), unicode_name)
                if not exists(dirname(fullpath)):
                    os.makedirs(dirname(fullpath))
                with open(fullpath, "wb") as f1:
                    f1.write(content)


def get_logger() -> logging.Logger:
    """Инициализация логгера."""

    _logger = logging.getLogger("repack_orem")
    _logger.setLevel(logging.INFO)

    # Если папки с логом нет, то создаем её
    if not exists("LOG"):
        os.makedirs("LOG")
    # создаем handler файла лога
    _fh = logging.FileHandler(
        "LOG/{}_repack_orem.log".format(DT.date.today().strftime("%Y-%m-%d"))
    )

    # задаем форматирование
    _formatter = logging.Formatter("%(asctime)s - %(levelname)s: %(message)s")
    _fh.setFormatter(_formatter)

    # добавляем handler в логгер
    _logger.addHandler(_fh)
    return _logger


def get_document_type(root: ET.Element) -> str:
    """Получение типа документа xml."""
    _doc_types = {"СЧФ": "СЧФ", "ДОП": "АПП", "СЧФДОП": "СЧФДОП"}
    type_tag = root.find("Документ")
    if not type_tag is None:
        _doc_type = type_tag.get("Функция").upper()
        if _doc_type in _doc_types:
            return _doc_types[_doc_type]
    return "Не разобрано"


def get_market(root: ET.Element) -> str:
    """Получение типа рынка."""
    _markets = {
        "RDN": "РДД",
        "DPMC": "ДПМ ТЭС",
        "DPMN": "ДПМ ТЭС",
        "DPMG": "ДПМ ГА",
        "DPMA": "ДПМ ГА",
        "DPMV": "ДПМ ВИЭ",
        "KOM": "КОМ",
        "DVR": "ДВР",
        "MNZ": "НЦЗ",
        "МNZ": "НЦЗ",
        "Д/УЭГ": "СДМ",
        "SDD": "ЭЭ СДД",
        "KOMMOD": "КОМмод",
    }
    market_mask = (r"([A-ZА-Я]{3,4})-", r"([A-ZА-Я]{3,6})-", r"№\s?(Д/УЭГ)/")

    _tov_tag = root.find("Документ/СвПродПер/СвПер/ОснПер")
    if not _tov_tag is None:
        _osn_num = _tov_tag.get("НомОсн").upper()
        for mask in market_mask:
            res = re.search(mask, _osn_num)
            if not res is None:
                eng_market = res[1]
                if eng_market in _markets:
                    return _markets[eng_market]
                break
    return "Не разобрано"


def process_xml(xml_file: str) -> None:
    root = ET.parse(xml_file).getroot()

    # определение типа
    doc_type = get_document_type(root)

    # определение рынка
    tov_tag = get_market(root)


def processing_buffer() -> None:
    """Обработка папки-буфера с выгруженными из Диадока и СБИСа архивами."""
    logger.info("------------Старт обработки------------")
    for supplier_path in os.listdir(BUFFER_DIR):  # проход по папкам поставщиков
        full_supplier_path = join(BUFFER_DIR, supplier_path)
        if os.path.isdir(
            full_supplier_path
        ):  # проверка, что это именно папка, а не файл

            # проход по файлам внутри папок поставщиков
            for archive_file in os.listdir(full_supplier_path):
                full_archive_file = join(full_supplier_path, archive_file)
                if os.path.isfile(full_archive_file) and archive_file.lower().endswith(
                    ".zip"
                ):  # проверка условий

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
                        unpack_zip(_tmp_archive_file)  # распаковка
                        os.remove(_tmp_archive_file)  # удаление архива
                        for doc_dir in os.listdir(
                            tmpdirname
                        ):  # перебор папок с документами
                            full_doc_dir = join(tmpdirname, doc_dir)
                            for doc_file in os.listdir(full_doc_dir):
                                full_doc_file = join(full_doc_dir, doc_file)
                                if os.path.isfile(full_doc_file):
                                    if doc_file[
                                        :13
                                    ].upper() == "ON_NSCHFDOPPR" and doc_file.upper().endswith(
                                        ".XML"
                                    ):
                                        process_xml(full_doc_file)


# загружаем основной путь к папке с архивами
MAIN_DOC_DIR = os.environ.get("MAIN_DOC_DIR")
BUFFER_DIR = join(MAIN_DOC_DIR, "Буфер")

logger = get_logger()
processing_buffer()
