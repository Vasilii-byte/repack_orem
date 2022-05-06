#!/usr/bin/env python
# coding: utf-8


"""Скрипт по перепаковке архивов из Диадока и СБИСа."""
import datetime as DT
import logging
from os.path import join, dirname, exists
import os
import re
from shutil import copyfile, move, rmtree
import tempfile
import xml.etree.ElementTree as ET
import zipfile

import calendar
from dateutil.parser import parse
from dotenv import load_dotenv

from tika import parser

NOT_RESOLVED = 'Не разобрано'
# Загрузка переменных окружения
dotenv_path = join(dirname(__file__), '.env')
load_dotenv(dotenv_path)


def unpack_zip(archive_file: str) -> None:
    """Распаковка архива."""
    with zipfile.ZipFile(archive_file, 'r') as zip_file:
        for name in zip_file.namelist():
            unicode_name = name.encode('cp437').decode('cp866')
            # недекодированное имя - для чтения в архиве
            with zip_file.open(name) as file_0:
                content = file_0.read()
                fullpath = join(dirname(archive_file), unicode_name)
                if not exists(dirname(fullpath)):
                    os.makedirs(dirname(fullpath))
                with open(fullpath, 'wb') as file_1:
                    file_1.write(content)


def get_logger() -> logging.Logger:
    """Инициализация логгера."""

    _logger = logging.getLogger('repack_orem')
    _logger.setLevel(logging.INFO)

    # Если папки с логом нет, то создаем её
    if not exists('LOG'):
        os.makedirs('LOG')
    # создаем handler файла лога
    _fh = logging.FileHandler(
        'LOG/{}_repack_orem.log'.format(DT.date.today().strftime('%Y-%m-%d'))
    )

    # задаем форматирование
    _formatter = logging.Formatter('%(asctime)s - %(levelname)s: %(message)s')
    _fh.setFormatter(_formatter)

    # добавляем handler в логгер
    _logger.addHandler(_fh)
    return _logger


def get_document_no_date_xml(root: ET.Element) -> tuple:
    """Получение номера и даты документа xml."""

    _tag = root.find('Документ/СвСчФакт')
    if _tag is not None:
        _number = _tag.get('НомерСчФ')
        _date = parse(_tag.get('ДатаСчФ'))
        return (_number, _date)

    _tag = root.find('Документ/СвДокПРУ/ИдентДок')
    if _tag is not None:
        _number = _tag.get('НомДокПРУ')
        _date = parse(_tag.get('ДатаДокПРУ'))
        return (_number, _date)

    _tag = root.find('Документ')
    if _tag is not None:
        _number = _tag.get('Номер')
        _date = parse(_tag.get('Дата'))
        return (_number, _date)

    return NOT_RESOLVED, NOT_RESOLVED


def get_document_no_date_pdf(pdf_text: str, doc_type: str) -> tuple:
    """Получение номера и даты документа pdf."""

    if doc_type == 'АПП':
        doc_no_mask = (
            r'АКТ[\s.]ПРИЕМА-ПЕРЕДАЧИ\s\(ПОСТАВКИ\)\sМОЩНОСТИ\s№\s([\d]*?)\s*ОТ\s*(\d{2}\.\d{2}\.\d{4})\s',         # noqa
            r'АКТ[\s.]ПРИЕМА-ПЕРЕДАЧИ\s\(ПОСТАВКИ\)\sМОЩНОСТИ\s№\s([\d]*?)\s*ОТ\s*(\d{2}\s[А-Я]+\s\d{4})\s',        # noqa
            r'ПРИЕМА\s?[-–]\s?ПЕРЕДАЧИ\s\(ПОСТАВКИ\)\sМОЩНОСТИ\s№\s([\d-]*?)\s*ОТ\s*(\d{2}\.\d{2}\.\d{4})\s',       # noqa
            r'АКТ[\s.]*?ПРИЕМА-ПЕРЕДАЧИ\s\МОЩНОСТИ\s№\s([\d-]*?)\s*ОТ\s*(\d{2}\s[А-Я]+\s\d{4})',                    # noqa
            r'АКТ[\s.]ПРИЕМА-ПЕРЕДАЧИ\s№\s([\d-]*?)\s*ОТ\s*(\d{2}\s[А-Я]+\s\d{4})',
            r'АКТ[\s.]ПРИЕМА\s*?[-–]\s*?ПЕРЕДАЧИ\s[\w\W]{1,100}№\s?([\d\-]+)\sОТ\s(\d{2}\.\d{2}\.\d{4})',           # noqa
            r'АКТ[\s.]ПРИЕМА[-\s]*?ПЕРЕДАЧИ\s[\w\W]{1,100}№\s?([\d\-]+)\sОТ\s(\d{2}\.\d{2}\.\d{4})',                # noqa
            r'АКТ[\s.]ПРИЕМА[-\s]*?ПЕРЕДАЧИ\s№\s?([\d\-]+)\sОТ\s(\d{2}\.\d{2}\.\d{4})',                             # noqa
            r'АКТ\s*?ПРИЕМА-ПЕРЕДАЧИ\sЭЛЕКТРИЧЕСКОЙ\sЭНЕРГИИ\s*?№\s*?([\d]*?)\s*ОТ\s*(\d{2}\.\d{2}\.\d{4})\s',      # noqa
            r'АКТ\s*?ПРИЕМА\s*?[-–]\s*?ПЕРЕДАЧИ\s[\w\W]*?№\s?([\w\-\/]+)\sОТ\s(\d{2}\.\d{2}\.\d{4})\s',             # noqa
            r'ПРИЕМА\s?[-–]\s?ПЕРЕДАЧИ\s*?МОЩНОСТИ\s№\s([\d-]*?)\s*ОТ\s*(\d{2}\.\d{2}\.\d{4})\s',                   # noqa
            r'АКТ[\s.]*?ПРИЕМА-ПЕРЕДАЧИ\s\МОЩНОСТИ[\s.]*?№\s([DVR\d-]*?)\s*ОТ\s*(\d{2}\s[А-Я]+\s\d{4})',            # noqa
            r'АКТ[\s.]ПРИЕМА\s*?[-–]\s*?ПЕРЕДАЧИ\s[А-Я]{1,100}\s№\s?([А-Я\d\-\_\/]+)\sОТ\s(\d{2}\.\d{2}\.\d{4})',   # noqa
        )
    else:
        doc_no_mask = (
            r'№\s?(([A-Z]{4})-[A-Z\d-]+)\s*ОТ',         # noqa
            r'№\s?([\w\-]+\-(SDD)\-[\d]{2})\sОТ',        # noqa
            r'№\s\s?([A-Z-\d\/]+?)\sОТ', # noqa
            r'№\s+?([A-Z]{3,4}-[A-Z\d-]+)\s+?ОТ\s\d{2}\.\d{2}\.\d{2}',
            r'№\s?(([A-Z]{3,4})-[\W\w]+?)\sОТ\s\d{2}\.\d{2}\.\d{4}',
            r'№\s?(([A-ZМ]{3,4})-[\W\w]+?)\sОТ',
            r'(Д/УЭГ[\W\w]+?)\sОТ',
            r'(KOMMOD[\W\w]+?)\sОТ', # noqa
        )

    months_str = {
        'ЯНВАРЯ': '01',
        'ФЕВРАЛЯ': '02',
        'МАРТА': '03',
        'АПРЕЛЯ': '04',
        'МАЯ': '05',
        'ИЮНЯ': '06',
        'ИЮЛЯ': '07',
        'АВГУСТА': '08',
        'СЕНТЯБРЯ': '09',
        'ОКТЯБРЯ': '10',
        'НОЯБРЯ': '11',
        'ДЕКАБРЯ': '12',
    }
    doc_number = NOT_RESOLVED
    for mask in doc_no_mask:
        res0 = re.search(mask, pdf_text)
        if res0 is not None:
            doc_number = res0[1]
            # Так как номер документа будет присутствовать в имени архива,
            # то нужно убрать из номер документа символы, которые недопустимы
            # в имени пути
            doc_number = doc_number.replace('\\', '_')
            doc_number = doc_number.replace('/', '_')

            if doc_type == 'АПП':
                dt_str = res0[2]
                # Заменяем месяц строкой на месяц числом
                for key, value in months_str.items():
                    if key in dt_str:
                        dt_str = dt_str.replace(key, value)
                        break
                # получаем дату из текстовой строки
                doc_date = parse(dt_str)
            else:
                doc_date = DT.datetime.today() + DT.timedelta(days=-30)
                last_day = calendar.monthrange(doc_date.year, doc_date.month)[1]
                doc_date = DT.date(doc_date.year, doc_date.month, last_day)
            return doc_number, doc_date
    return NOT_RESOLVED, NOT_RESOLVED


def get_document_type(root: ET.Element) -> str:
    """Получение типа документа xml."""
    _doc_types = {
        'СЧФ': 'СЧФ',
        'ДОП': 'АПП',
        'СЧФДОП': 'СЧФДОП'
    }
    type_tag = root.find('Документ')
    if type_tag is not None:
        _doc_type = type_tag.get('Функция').upper()
        if _doc_type in _doc_types:
            return _doc_types[_doc_type]
    return NOT_RESOLVED


def get_market_xml(root: ET.Element) -> str:
    """Получение типа рынка."""
    _markets = {
        'RDN': 'РДД',
        'DPMC': 'ДПМ ТЭС',
        'DPMN': 'ДПМ ТЭС',
        'DPMG': 'ДПМ ГА',
        'DPMA': 'ДПМ ГА',
        'DPMV': 'ДПМ ВИЭ',
        'KOM': 'КОМ',
        'КОМ': 'КОМ',
        'DVR': 'ДВР',
        'MNZ': 'НЦЗ',
        'МNZ': 'НЦЗ',
        'Д/УЭГ': 'СДМ',
        'SDMO': 'СДМ',
        'SDD': 'ЭЭ СДД',
        'KOMMOD': 'КОМмод',
    }
    market_mask = (
        r'([A-Z]{3,4})-',
        r'([A-ZА-Я]{3,6})-',
        r'№\s?(Д/УЭГ)/',
        r'\-(SDMO)\-ATS',
        r'[\w\-]+\-(SDD)\-',
    )

    _tag = root.find('Документ/СвПродПер/СвПер/ОснПер')
    if _tag is not None:
        _osn_num = _tag.get('НомОсн')
        if _osn_num is not None:
            _osn_num = _osn_num.upper()
        else:
            _tag = root.find('Документ/ТаблСчФакт/СведТов')
            if _tag is not None:
                _osn_num = _tag.get('НаимТов').upper()
        for mask in market_mask:
            res = re.search(mask, _osn_num)
            if res is not None:
                eng_market = res[1]
                if eng_market in _markets:
                    return _markets[eng_market]

    _tag = root.find('Документ/ТаблСчФакт/СведТов')
    if _tag is not None:
        _osn_num = _tag.get('НаимТов').upper()
        for mask in market_mask:
            res = re.search(mask, _osn_num)
            if res is not None:
                eng_market = res[1]
                if eng_market in _markets:
                    return _markets[eng_market]

    _tag = root.find('Документ/СвСчФакт/ИнфПолФХЖ1/ТекстИнф')
    if _tag is not None:
        _osn_num = _tag.get('Значен').upper()
        for mask in market_mask:
            res = re.search(mask, _osn_num)
            if res is not None:
                eng_market = res[1]
                if eng_market in _markets:
                    return _markets[eng_market]

    _tag = root.find('Документ/СвПродПер/СвПер/ОснПер')
    if _tag is not None:
        _osn_num = _tag.get('НаимОсн').upper()
        for mask in market_mask:
            res = re.search(mask, _osn_num)
            if res is not None:
                eng_market = res[1]
                if eng_market in _markets:
                    return _markets[eng_market]

    _tag = root.find('Документ/СвДокПРУ/СодФХЖ1/ЗагСодОпер')
    if _tag is not None:
        _osn_num = _tag.text.upper()
        for mask in market_mask:
            res = re.search(mask, _osn_num)
            if res is not None:
                eng_market = res[1]
                if eng_market in _markets:
                    return _markets[eng_market]

    _tag = root.find('Документ/ТаблДок/ИтогТабл/Основание')
    if _tag is not None:
        _osn_num = _tag.get('Номер').upper()
        for mask in market_mask:
            res = re.search(mask, _osn_num)
            if res is not None:
                eng_market = res[1]
                if eng_market in _markets:
                    return _markets[eng_market]

    _tag = root.find('Документ/ТаблДок/ИтогТабл/Основание')
    if _tag is not None:
        _osn_num = _tag.get('Название').upper()
        for mask in market_mask:
            res = re.search(mask, _osn_num)
            if res is not None:
                eng_market = res[1]
                if eng_market in _markets:
                    return _markets[eng_market]

    _tag = root.find('Документ/Основание')
    if _tag is not None:
        _osn_num = _tag.get('Номер').upper()
        for mask in market_mask:
            res = re.search(mask, _osn_num)
            if res is not None:
                eng_market = res[1]
                if eng_market in _markets:
                    return _markets[eng_market]

    return NOT_RESOLVED


def get_market_pdf(pdf_text: str, doc_type: str) -> str:
    """Получение типа рынка из файла pdf."""
    _markets = {
        'RDN': 'РДД',
        'DPMC': 'ДПМ ТЭС',
        'DPMN': 'ДПМ ТЭС',
        'DPMG': 'ДПМ ГА',
        'DPMA': 'ДПМ ГА',
        'DPMV': 'ДПМ ВИЭ',
        'KOM': 'КОМ',
        'КОМ': 'КОМ',
        'DVR': 'ДВР',
        'MNZ': 'НЦЗ',
        'МNZ': 'НЦЗ',
        'Д/УЭГ': 'СДМ',
        'SDMO': 'СДМ',
        'SDD': 'ЭЭ СДД',
        'KOMMOD': 'КОМмод',
        '2G-00': 'СДМ',
    }
    if doc_type == 'АПП':
        market_mask = (
            r'№[\s.]?([A-Z]{3,4})-[A-Z\d-]+\s*ОТ\s*\d{2}\.\d{2}\.\d{4}',
            r'№[\s.]?([A-Z]{3,6})-[\s.A-Z\d-]+\s*ОТ\s*\d{2}\.\d{2}\.\d{4}',
            r'№[\s.]*?([A-Z]{3,4})-[A-Z\d-]+\s*ОТ\s*\d{2}\.\d{2}\.\d{4}',
            r'№[\s.]?([A-Z]{3,4})-[A-Z\d-]+\s*ОТ[\s.]*\d{2}\.\d{2}\.\d{4}',
            r'СВОБОДНОМУ\sДОГОВОРУ[\w\-\s]*№\s?[\w\-]+\-(SDD)\-',
            r'[\S]*?(SDMO)\-ATS',
        )
    else:
        market_mask = (
            r'№\s?([A-Z]{4})-[A-Z\d-]+\s*ОТ',         # noqa
            r'№\s?[\w\-]+\-(SDD)\-[\d]{2}\sОТ',        # noqa
            r'№\s*?([A-Z-\d]{3,4})-', # noqa
            r'№\s*?([A-Z-\dМ]{3,4})-', # noqa
            r'№\s*?(2G-00)',
            r'(Д/УЭГ)/',
            r'№\s*?(KOMMOD)-', # noqa
        )

    for mask in market_mask:
        res0 = re.search(mask, pdf_text)
        if res0 is not None:
            eng_market = res0[1]
            if eng_market in _markets:
                return _markets[eng_market]
    return NOT_RESOLVED


def pack_and_move_diadoc(_doc_path: str, _dest_path: str):
    """Упаковка файлов в архив и перемещение в целевую папку."""
    _zip_file = zipfile.ZipFile(_doc_path + '.zip', 'w')
    for folder, _, files in os.walk(_doc_path):
        for file in files:
            _zip_file.write(
                join(folder, file),
                os.path.relpath(join(folder, file), _doc_path),
                compress_type=zipfile.ZIP_DEFLATED,
            )
    _zip_file.close()
    if not exists(os.path.dirname(_dest_path)):
        os.makedirs(os.path.dirname(_dest_path))
    move(_doc_path + '.zip', _dest_path)


def get_property_from_xml(_file: str, _tag_path: str, _tag_prop: str) -> str:
    """Получение типа документа."""
    root = ET.parse(_file).getroot()
    _tag = root.find(_tag_path)
    if _tag is not None:
        return _tag.get(_tag_prop)

    if _tag is not None:
        return _tag.get('Наим')
    return ''


def pack_and_move_sbis(_doc_file: str, _dest_path: str):
    """Упаковка файлов в архив и перемещение в целевую папку."""
    _doc_path = dirname(_doc_file)
    _short_file_name = os.path.basename(_doc_file)
    if _short_file_name.upper().startswith('DP_REZRUISP') or \
            _short_file_name.upper().endswith('.XLS') \
            or (_short_file_name.upper().startswith('MOSEGENE') and _short_file_name.upper().endswith('PDF')) or \
            (_short_file_name.upper().startswith('АКТ СВЕРКИ') and _short_file_name.upper().endswith('PDF')):
        _doc_type = 'ДОП'
    elif 'АКТЫ СВЕРКИ МОСЭНЕРГО' in _doc_file.upper():
        _doc_type = 'АСВ'
    elif 'ON_ACCOUNTS' in _doc_file.upper():
        _doc_type = 'АСВ'
    else:
        _doc_type = get_property_from_xml(
            _doc_file,
            'Документ',
            'Функция'
        ).upper()

    _zip_file = zipfile.ZipFile(join(_doc_path, 'sbis') + '.zip', 'w')
    for folder, _, files in os.walk(_doc_path):
        for file in files:
            if (
                os.path.splitext(_short_file_name)[0].upper() in file.upper()
                    or 'СПРАВКА О ПРОХОЖДЕНИИ' in file.upper()
            ):
                if r'/PDF/' in join(folder, file):
                    os.rename(join(folder, file), join(folder, 'ПЕЧАТНАЯ ФОРМА' + file))
                    file = 'ПЕЧАТНАЯ ФОРМА' + file
                _zip_file.write(
                    join(folder, file),
                    os.path.relpath(join(folder, file), _doc_path),
                    compress_type=zipfile.ZIP_DEFLATED,
                )

            if (
                    file.upper().startswith('DP_IZVPOL')
                    and file.upper().endswith('.XML')
                    and get_property_from_xml(
                        join(folder, file),
                        'Документ/СвИзвПолуч/СведПолФайл',
                        'ИмяПостФайла',
                    ).upper()
                    in _short_file_name.upper()
            ):
                _zip_file.write(
                    join(folder, file),
                    os.path.relpath(join(folder, file), _doc_path),
                    compress_type=zipfile.ZIP_DEFLATED,
                )

                for sig_file in os.listdir(folder):
                    full_sig_file = join(folder, sig_file)
                    if (
                        sig_file.upper().endswith('.SGN')
                            and os.path.splitext(file.upper())[0] in sig_file.upper()
                    ):
                        _zip_file.write(
                            full_sig_file,
                            os.path.relpath(full_sig_file, _doc_path),
                            compress_type=zipfile.ZIP_DEFLATED,
                        )

            if (
                    file.upper().startswith('DP_PDOTPR')
                    and file.upper().endswith('.XML')
                    and get_property_from_xml(
                        join(folder, file),
                        'Документ/СведПодтв/СведОтпрФайл',
                        'ИмяПостФайла',
                    ).upper()
                    in _short_file_name.upper()
            ):
                _zip_file.write(
                    join(folder, file),
                    os.path.relpath(join(folder, file), _doc_path),
                    compress_type=zipfile.ZIP_DEFLATED,
                )

                for sig_file in os.listdir(folder):
                    full_sig_file = join(folder, sig_file)
                    if (
                            sig_file.upper().endswith('.SGN')
                            and os.path.splitext(file.upper())[0] in sig_file.upper()
                    ):
                        _zip_file.write(
                            full_sig_file,
                            os.path.relpath(full_sig_file, _doc_path),
                            compress_type=zipfile.ZIP_DEFLATED,
                        )

            if file.upper().startswith('DP_PDPOL') and _doc_type.upper() == 'СЧФ':
                _zip_file.write(
                    join(folder, file),
                    os.path.relpath(join(folder, file), _doc_path),
                    compress_type=zipfile.ZIP_DEFLATED,
                )

            if (
                    file.upper().startswith('ON_NSCHFDOPPOK')
                    or file.upper().startswith('DP_REZRUZAK')
            ) and _doc_type.upper() == 'ДОП':
                _zip_file.write(
                    join(folder, file),
                    os.path.relpath(join(folder, file), _doc_path),
                    compress_type=zipfile.ZIP_DEFLATED,
                )

            if file.upper().startswith('DP_UVPRIEM') and _doc_type.upper() == 'СЧФ':
                _zip_file.write(
                    join(folder, file),
                    os.path.relpath(join(folder, file), _doc_path),
                    compress_type=zipfile.ZIP_DEFLATED,
                )

    _zip_file.close()
    if not exists(os.path.dirname(_dest_path)):
        os.makedirs(os.path.dirname(_dest_path))
    move(join(_doc_path, 'sbis') + '.zip', _dest_path)


def process_xml(_supplier_path: str, xml_file: str) -> str:
    """Процедура обработки XML."""
    root = ET.parse(xml_file).getroot()

    is_success = False

    # определение типа
    if os.path.basename(xml_file).upper().startswith('DP_REZRUISP'):
        doc_type = 'АПП'
    elif os.path.basename(xml_file).upper().startswith('ON_ACCOUNTS'):
        doc_type = 'АСВ'
    else:
        doc_type = get_document_type(root)

    # определение рынка
    market_type = get_market_xml(root)

    doc_number, doc_date = get_document_no_date_xml(root)
    doc_number = doc_number.replace('\\', '_')
    doc_number = doc_number.replace('/', '_')
    _date_str = doc_date.strftime(r'%d.%m.%Y')

    if NOT_RESOLVED in (doc_type, market_type, doc_number, doc_date):
        _short_name = os.path.basename(xml_file)
        print(
            (f'Ошибка разбора файла {_short_name}. Поставщик {_supplier_path}.'
             f' {market_type}: {doc_type} № {doc_number} от {_date_str}')
        )
        logger.error(
            (f'Ошибка разбора файла {_short_name}. Поставщик {_supplier_path}.'
             f' {market_type}: {doc_type} № {doc_number} от {_date_str}')
        )

    else:
        is_success = True
        print(f'{market_type}: {doc_type} № {doc_number} от {_date_str}')
        logger.info(
            (f'Поставщик {_supplier_path}. {market_type}: '
             f'{doc_type} № {doc_number} от {_date_str}')
        )

    if is_success:
        return join(
            MAIN_DOC_DIR,
            doc_date.strftime(r'%Y-%m'),
            'Покупка',
            market_type,
            _supplier_path,
            f'{doc_type} № {doc_number} от {_date_str}.zip',
        )
    return ''


def process_pdf(_supplier_path: str, pdf_file: str) -> str:
    """Процедура обработки PDF."""
    is_success = False
    _short_name = os.path.basename(pdf_file)

    page_text = parser.from_file(pdf_file)['content'].upper()

    if ('СВЕРКИ' in _short_name.upper() or
            'ВЗАИМОРАСЧЕТОВ' in _short_name.upper() or
            'АКТЫ СВЕРКИ МОСЭНЕРГО' in pdf_file.upper()
            or 'АКТ СВЕРКИ РАСЧЕТОВ' in page_text):
        if page_text.find('МОЩНОСТИ') > 1:
            doc_type = 'АСВ М'
        elif page_text.find('ЭЛЕКТРОЭНЕРГИИ') > 1:
            doc_type = 'АСВ ЭЭ'
        else:
            doc_type = 'АСВ'
    else:
        doc_type = 'АПП'
    doc_number = NOT_RESOLVED
    _date_str = NOT_RESOLVED
    _date_str_1 = NOT_RESOLVED
    doc_number, doc_date = get_document_no_date_pdf(page_text, doc_type)
    if doc_date != NOT_RESOLVED:
        _date_str = doc_date.strftime(r'%d.%m.%Y')
        _date_str_1 = doc_date.strftime(r'%Y-%m')

    # определение рынка
    market_type = get_market_pdf(page_text, doc_type)

    # print(page_text)
    if NOT_RESOLVED in (doc_type, doc_number, doc_date):
        print(
            (f'Ошибка разбора файла {_short_name}. Поставщик {_supplier_path}.'
             f' {market_type}: {doc_type} № {doc_number} от {_date_str}')
        )
        logger.error(
            (f'Ошибка разбора файла {_short_name}. Поставщик {_supplier_path}.'
             f' {market_type}: {doc_type} № {doc_number} от {_date_str}')
        )

    else:
        is_success = True
        print(f'{market_type}: {doc_type} № {doc_number} от {_date_str}')
        logger.info(
            (f'Поставщик {_supplier_path}. {market_type}: '
             f'{doc_type} № {doc_number} от {_date_str}')
        )

    if is_success:
        return join(
            MAIN_DOC_DIR,
            _date_str_1,
            'Покупка',
            market_type,
            _supplier_path,
            f'{doc_type} № {doc_number} от {_date_str}.zip',
        )
    return ''


def repack_diadoc_archive(supplier_path: str, full_archive_file: str) -> bool:
    # распаковка архива во временную папку
    is_success: bool = False
    archive_file = os.path.basename(full_archive_file)
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
                            'ON_NSCHFDOPPR'
                    ) and doc_file.upper().endswith('.XML'):
                        _dest_path = process_xml(
                            supplier_path, full_doc_file
                        )
                        _result = _dest_path != ''
                        if _result:
                            pack_and_move_diadoc(
                                os.path.dirname(full_doc_file),
                                _dest_path,
                            )
                        is_success = is_success and _result
                    elif (
                            (doc_file.upper().endswith('.PDF')
                                and 'ФАКТУРА' not in doc_file.upper()
                                and 'ПЕЧАТНАЯ ФОРМА' in doc_file.upper()
                                and 'ON_NSCHFDOPPR' not in doc_dir.upper())
                            or (doc_file.upper().endswith('.PDF')
                                and 'MOSEGENE_MOSENERG' in doc_file.upper()
                                and r'/PDF/' not in doc_file.upper())
                    ):
                        _dest_path = process_pdf(
                            supplier_path, full_doc_file
                        )
                        _result = _dest_path != ''
                        if _result:
                            pack_and_move_diadoc(
                                os.path.dirname(full_doc_file),
                                _dest_path,
                            )
                        is_success = is_success and _result
    return is_success


def processing_buffer() -> None:
    """Обработка папки-буфера с выгруженными из Диадока и СБИСа архивами."""
    logger.info('------------Старт обработки------------')
    for supplier_path in os.listdir(BUFFER_DIR):  # проход по папкам поставщиков
        full_supplier_path = join(BUFFER_DIR, supplier_path)
        if os.path.isdir(
                full_supplier_path
        ):  # проверка, что это именно папка, а не файл

            # проход по файлам внутри папок поставщиков
            for archive_file in os.listdir(full_supplier_path):
                full_archive_file = join(full_supplier_path, archive_file)
                if (
                        os.path.isfile(full_archive_file)
                        and archive_file.lower().endswith('.zip')
                        and not archive_file.upper().startswith('ОБРАБОТАНО_')
                ):  # Это Диадок

                    is_success = True
                    # если в папке есть не обработанные архивы,
                    # тогда и показываем, что делаем обработку папки
                    logger.info('Обработка папки %s', supplier_path)
                    print(f'----------{supplier_path}----------')

                    logger.info('Распаковка файла %s', archive_file)
                    print(f'Распаковка файла {archive_file}')

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
                                            'ON_NSCHFDOPPR'
                                    ) and doc_file.upper().endswith('.XML'):
                                        _dest_path = process_xml(
                                            supplier_path, full_doc_file
                                        )
                                        _result = _dest_path != ''
                                        if _result:
                                            pack_and_move_diadoc(
                                                os.path.dirname(full_doc_file),
                                                _dest_path,
                                            )
                                        is_success = is_success and _result
                                    elif (
                                            doc_file.upper().endswith('.PDF')
                                            and 'ФАКТУРА' not in doc_file.upper()
                                            and 'ПЕЧАТНАЯ ФОРМА' in doc_file.upper()
                                            and 'ON_NSCHFDOPPR' not in doc_dir.upper()
                                    ):
                                        _dest_path = process_pdf(
                                            supplier_path, full_doc_file
                                        )
                                        _result = _dest_path != ''
                                        if _result:
                                            pack_and_move_diadoc(
                                                os.path.dirname(full_doc_file),
                                                _dest_path,
                                            )
                                        is_success = is_success and _result

                    if is_success:
                        os.rename(
                            full_archive_file,
                            join(full_supplier_path, 'Обработано_' + archive_file),
                        )
                elif os.path.isdir(
                        full_archive_file
                ) and (archive_file.upper().startswith('ПОСТУПЛЕНИЯ') or archive_file.upper().startswith('АКТЫ СВЕРКИ')):  # Это СБИС:
                    is_success = True
                    # если в папке есть не обработанные архивы,
                    # тогда и показываем, что делаем обработку папки
                    logger.info('Обработка папки %s', supplier_path)
                    print(f'----------{supplier_path}----------')

                    for doc_file in os.listdir(full_archive_file):
                        full_doc_file = join(full_archive_file, doc_file)
                        if os.path.isfile(full_doc_file):
                            # документ xml
                            if (
                                    doc_file.upper().startswith('ON_NSCHFDOPPR')
                                    or doc_file.upper().startswith('DP_REZRUISP')
                                    or doc_file.upper().startswith('ON_ACCOUNTS')
                            ) and doc_file.upper().endswith('.XML'):
                                _dest_path = process_xml(supplier_path, full_doc_file)
                                _result = _dest_path != ''
                                if _result:
                                    pack_and_move_sbis(full_doc_file, _dest_path)
                                is_success = is_success and _result
                                # print(doc_file)

                            if doc_file.upper().startswith(
                                    'ON_AKTPP'
                            ) and doc_file.upper().endswith('.XLS'):
                                _dest_path = process_pdf(supplier_path, full_doc_file)
                                _result = _dest_path != ''
                                if _result:
                                    pack_and_move_sbis(full_doc_file, _dest_path)
                                is_success = is_success and _result
                                print(doc_file)

                            if doc_file.upper().startswith(
                                    'ON_ASVER'
                            ) and doc_file.upper().endswith('.XLS'):
                                _dest_path = process_pdf(supplier_path, full_doc_file)
                                _result = _dest_path != ''
                                if _result:
                                    pack_and_move_sbis(full_doc_file, _dest_path)
                                is_success = is_success and _result
                                print(doc_file)

                            if doc_file.upper().startswith(
                                    'АКТ ПО ДОГОВОРАМ'
                            ) and doc_file.upper().endswith('.PDF'):
                                _dest_path = process_pdf(supplier_path, full_doc_file)
                                _result = _dest_path != ''
                                if _result:
                                    pack_and_move_sbis(full_doc_file, _dest_path)
                                is_success = is_success and _result
                                print(doc_file)

                            if doc_file.upper().startswith(
                                    'АКТ СВЕРКИ'
                            ) and doc_file.upper().endswith('.PDF'):
                                _dest_path = process_pdf(supplier_path, full_doc_file)
                                _result = _dest_path != ''
                                if _result:
                                    pack_and_move_sbis(full_doc_file, _dest_path)
                                is_success = is_success and _result
                                print(doc_file)

                            if doc_file.upper().startswith(
                                    'MOSEGENE_MOSENERG'
                            ) and doc_file.upper().endswith('.PDF'):
                                _dest_path = process_pdf(supplier_path, full_doc_file)
                                _result = _dest_path != ''
                                if _result:
                                    pack_and_move_sbis(full_doc_file, _dest_path)
                                is_success = is_success and _result
                                print(doc_file)

                    if is_success:
                        with open(
                                join(full_supplier_path, f'Обработано {archive_file}.txt'),
                                'w+',
                        ) as my_file:
                            my_file.write('')
                            my_file.close()
                        rmtree(full_archive_file)


# загружаем основной путь к папке с архивами
MAIN_DOC_DIR = os.path.normpath(os.environ.get('MAIN_DOC_DIR'))
BUFFER_DIR = join(MAIN_DOC_DIR, 'Буфер')

logger = get_logger()
processing_buffer()
print('Загрузка завершена.')
