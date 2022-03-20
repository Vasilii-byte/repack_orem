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
from shutil import copyfile, move
from os.path import join, dirname, exists
from dateutil.parser import parse
from dotenv import load_dotenv
from tika import parser

NOT_RESOLVED = "Не разобрано"
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


def get_document_no_date_xml(root: ET.Element) -> tuple:
    """Получение номера и даты документа xml."""

    _tag = root.find("Документ/СвСчФакт")
    if not _tag is None:
        _number = _tag.get("НомерСчФ")
        _date = parse(_tag.get("ДатаСчФ"))
        return (_number, _date)
    return NOT_RESOLVED, NOT_RESOLVED


def get_document_no_date_pdf(pdf_text: str) -> tuple:
    """Получение номера и даты документа pdf."""

    doc_no_mask = (
        r"АКТ[\s.]ПРИЕМА-ПЕРЕДАЧИ\s\(ПОСТАВКИ\)\sМОЩНОСТИ\s№\s([\d]*?)\s*ОТ\s*(\d{2}\.\d{2}\.\d{4})\s",
        r"АКТ\s*?ПРИЕМА-ПЕРЕДАЧИ\sЭЛЕКТРИЧЕСКОЙ\sЭНЕРГИИ\s*?№\s*?([\d]*?)\s*ОТ\s*(\d{2}\.\d{2}\.\d{4})\s",
    )

    doc_number = NOT_RESOLVED
    for mask in doc_no_mask:
        res0 = re.search(mask, pdf_text)
        if res0 is not None:
            doc_number = res0[1]
            doc_number = doc_number.replace("\\", "_")
            doc_number = doc_number.replace("/", "_")

            doc_date = parse(res0[2])
            return doc_number, doc_date
    return NOT_RESOLVED, NOT_RESOLVED


def get_document_type(root: ET.Element) -> str:
    """Получение типа документа xml."""
    _doc_types = {"СЧФ": "СЧФ", "ДОП": "АПП", "СЧФДОП": "СЧФДОП"}
    type_tag = root.find("Документ")
    if not type_tag is None:
        _doc_type = type_tag.get("Функция").upper()
        if _doc_type in _doc_types:
            return _doc_types[_doc_type]
    return NOT_RESOLVED


def get_market_xml(root: ET.Element) -> str:
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
    market_mask = (
        r"([A-ZА-Я]{3,4})-",
        r"([A-ZА-Я]{3,6})-",
        r"№\s?(Д/УЭГ)/",
        r"[\w\-]+\-(SDD)\-",
    )

    _tag = root.find("Документ/СвПродПер/СвПер/ОснПер")
    if not _tag is None:
        _osn_num = _tag.get("НомОсн").upper()
        for mask in market_mask:
            res = re.search(mask, _osn_num)
            if not res is None:
                eng_market = res[1]
                if eng_market in _markets:
                    return _markets[eng_market]
    return NOT_RESOLVED


def get_market_pdf(pdf_text: str) -> str:
    """Получение типа рынка из файла pdf."""
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
    market_mask = (
        r"№\s?([A-Z]{3,4})-[A-Z\d-]+\sОТ\s\d{2}\.\d{2}\.\d{4}",
        r"СВОБОДНОМУ\sДОГОВОРУ[\w\-\s]*№\s?[\w\-]+\-(SDD)\-",
    )

    for mask in market_mask:
        res0 = re.search(mask, pdf_text)
        if res0 is not None:
            eng_market = res0[1]
            if eng_market in _markets:
                return _markets[eng_market]
    return NOT_RESOLVED


def pack_and_move(_doc_path: str, _dest_path: str):
    """Упаковка файлов в архив и перемещение в целевую папку."""
    _zip_file = zipfile.ZipFile(_doc_path + ".zip", "w")
    for folder, subfolders, files in os.walk(_doc_path):
        for file in files:
            _zip_file.write(
                join(folder, file),
                os.path.relpath(join(folder, file), _doc_path),
                compress_type=zipfile.ZIP_DEFLATED,
            )
        _zip_file.close()
    if not exists(os.path.dirname(_dest_path)):
        os.makedirs(os.path.dirname(_dest_path))
    move(_doc_path + ".zip", _dest_path)


def process_xml(_supplier_path: str, xml_file: str) -> None:
    """Процедура обработки XML."""
    root = ET.parse(xml_file).getroot()

    # определение типа
    doc_type = get_document_type(root)

    # определение рынка
    market_type = get_market_xml(root)

    doc_number, doc_date = get_document_no_date_xml(root)
    doc_number = doc_number.replace("\\", "_")
    doc_number = doc_number.replace("/", "_")
    _date_str = doc_date.strftime(r"%d.%m.%Y")

    if NOT_RESOLVED in (doc_type, market_type, doc_number, doc_date):
        _short_name = os.path.basename(xml_file)
        print(
            f"Ошибка разбора файла {_short_name}. Поставщик {_supplier_path}."
            + f" {market_type}: {doc_type} № {doc_number} от {_date_str}"
        )
        logger.error(
            f"Ошибка разбора файла {_short_name}. Поставщик {_supplier_path}."
            + f" {market_type}: {doc_type} № {doc_number} от {_date_str}"
        )

    else:
        print(f"{market_type}: {doc_type} № {doc_number} от {_date_str}")
        logger.info(
            f"Поставщик {_supplier_path}. {market_type}: {doc_type} № {doc_number} от {_date_str}"
        )

    _dest_path = join(
        MAIN_DOC_DIR,
        doc_date.strftime(r"%Y-%m"),
        "Покупка",
        market_type,
        _supplier_path,
        f"{doc_type} № {doc_number} от {_date_str}.zip",
    )
    pack_and_move(os.path.dirname(xml_file), _dest_path)


def process_pdf(_supplier_path: str, pdf_file: str) -> None:
    """Процедура обработки PDF."""
    page_text = parser.from_file(pdf_file)["content"].upper()

    doc_type = "АПП"
    doc_number = NOT_RESOLVED
    _date_str = NOT_RESOLVED
    doc_number, doc_date = get_document_no_date_pdf(page_text)
    if doc_date != NOT_RESOLVED:
        _date_str = doc_date.strftime(r"%d.%m.%Y")

    # определение рынка
    market_type = get_market_pdf(page_text)

    # print(page_text)
    if NOT_RESOLVED in (doc_type, doc_number, doc_date):
        _short_name = os.path.basename(pdf_file)
        print(
            f"Ошибка разбора файла {_short_name}. Поставщик {_supplier_path}."
            + f" {market_type}: {doc_type} № {doc_number} от {_date_str}"
        )
        logger.error(
            f"Ошибка разбора файла {_short_name}. Поставщик {_supplier_path}."
            + f" {market_type}: {doc_type} № {doc_number} от {_date_str}"
        )

    else:
        print(f"{market_type}: {doc_type} № {doc_number} от {_date_str}")
        logger.info(
            f"Поставщик {_supplier_path}. {market_type}: {doc_type} № {doc_number} от {_date_str}"
        )
    _dest_path = join(
        MAIN_DOC_DIR,
        doc_date.strftime(r"%Y-%m"),
        "Покупка",
        market_type,
        _supplier_path,
        f"{doc_type} № {doc_number} от {_date_str}.zip",
    )
    pack_and_move(os.path.dirname(pdf_file), _dest_path)


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
                                    # документ xml
                                    if doc_file.upper().startswith(
                                        "ON_NSCHFDOPPR"
                                    ) and doc_file.upper().endswith(".XML"):
                                        process_xml(supplier_path, full_doc_file)
                                    elif (
                                        "ПЕРЕДАЧИ" in doc_file.upper()
                                        and doc_file.upper().endswith(".PDF")
                                    ):
                                        process_pdf(supplier_path, full_doc_file)


# загружаем основной путь к папке с архивами
MAIN_DOC_DIR = os.environ.get("MAIN_DOC_DIR")
BUFFER_DIR = join(MAIN_DOC_DIR, "Буфер")

logger = get_logger()
processing_buffer()
print("Загрузка завершена.")
