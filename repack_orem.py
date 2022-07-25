#!/usr/bin/env python
# coding: utf-8

"""Скрипт по перепаковке архивов из Диадока и СБИСа."""
import calendar
import datetime
import logging
import os
import re
import tempfile
import xml.etree.ElementTree as ElementTree
import zipfile
from os.path import basename, dirname, exists, join, relpath
from shutil import copyfile, move, rmtree

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
        'LOG/{}_repack_orem.log'.format(
            datetime.date.today().strftime('%Y-%m-%d')
        )
    )

    # задаем форматирование
    _formatter = logging.Formatter('%(asctime)s - %(levelname)s: %(message)s')
    _fh.setFormatter(_formatter)

    # добавляем handler в логгер
    _logger.addHandler(_fh)
    return _logger


def get_document_no_date_xml(root: ElementTree.Element) -> tuple:
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


def convert_long_date_to_short_date(dt: str) -> datetime.date:
    """Заменяем месяц строкой на месяц числом."""
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
    for key, value in months_str.items():
        if key in dt:
            return parse(dt.replace(key, value))
    return parse(dt)


def get_last_date_of_previous_month(dt: datetime.date):
    """Получение последней даты предшествующего месяца."""
    dt_prev_month = dt + datetime.timedelta(days=-30)
    last_day = calendar.monthrange(dt_prev_month.year, dt_prev_month.month)[1]
    return datetime.date(dt_prev_month.year, dt_prev_month.month, last_day)


def get_document_no_date_pdf(pdf_text: str, doc_type: str) -> tuple:
    """Получение номера и даты документа pdf."""
    if doc_type == 'АПП':
        # маска номера и даты документа для АПП
        doc_no_mask = (
            r'АКТ[\s.]ПРИЕМА-ПЕРЕДАЧИ\s\(ПОСТАВКИ\)\sМОЩНОСТИ\s№\s([\d]*?)\s*ОТ\s*(\d{2}\.\d{2}\.\d{4})\s',         # noqa
            r'АКТ[\s.]ПРИЕМА-ПЕРЕДАЧИ\s\(ПОСТАВКИ\)\sМОЩНОСТИ\s№\s([\d]*?)\s*ОТ\s*(\d{2}\s[А-Я]+\s\d{4})\s',        # noqa
            r'ПРИЕМА\s?[-–]\s?ПЕРЕДАЧИ\sМОЩНОСТИ\s№\s([\d\/]*?)\s*ОТ\s*(\d{2}\s[А-Я]+\s\d{4})\s',                   # noqa
            r'ПРИЕМА\s?[-–]\s?ПЕРЕДАЧИ\s\(ПОСТАВКИ\)\sМОЩНОСТИ\s№\s([\d-]*?)\s*ОТ\s*(\d{2}\.\d{2}\.\d{4})\s',       # noqa
            r'АКТ[\s.]*?ПРИЕМА-ПЕРЕДАЧИ\s\МОЩНОСТИ\s№\s?([\d\-\/]*?)\s*ОТ\s*(\d{2}\s[А-Я]+\s\d{4})',                # noqa
            r'АКТ[\s.]ПРИЕМА-ПЕРЕДАЧИ\s№\s([\d-]*?)\s*ОТ\s*(\d{2}\s[А-Я]+\s\d{4})',                                 # noqa
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
        # маска номера и даты документа для АСВ
        doc_no_mask = (
            r'№\s?(([A-Z]{4})-[A-Z\d-]+)\s*ОТ',         # noqa
            r'№\s?([\w\-]+\-(SDD)\-[\d]{2})\sОТ',        # noqa
            r'№\s\s?([A-Z-\d\/]+?)\sОТ', # noqa
            r'№\s+?([A-Z]{3,4}-[A-Z\d-]+)\s+?ОТ\s\d{2}\.\d{2}\.\d{2}',
            r'№\s?(([A-Z]{3,4})-[\W\w]+?)\sОТ\s\d{2}\.\d{2}\.\d{4}',
            r'№\s?(([A-ZМ]{3,4})-[\W\w]+?)\sОТ',
            r'(Д/УЭГ[\W\w]+?)\sОТ',
            r'(DVR-[\d]*-[A-Z\d-]+)\s*ОТ',
            r'(KOM-[\d]*-[A-Z\d-]+-VV-\d)',
            r'((DPMC|KOM|RDN|DPMV)-[A-Z\d-]*)\sОТ',
            r'(KOMMOD[\W\w]+?)\sОТ', # noqa
        )

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
                doc_date = convert_long_date_to_short_date(res0[2])
            else:
                doc_date = get_last_date_of_previous_month(
                    datetime.datetime.today()
                )
            return doc_number, doc_date
    return NOT_RESOLVED, NOT_RESOLVED


def get_market_xml(root: ElementTree.Element) -> str:
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

    xml_tag_dict = {
        'Документ/СвПродПер/СвПер/ОснПер': ['НомОсн', 'НаимОсн'],
        'Документ/ТаблСчФакт/СведТов': ['НаимТов'],
        'Документ/СвСчФакт/ИнфПолФХЖ1/ТекстИнф': ['Значен'],
        'Документ/ТаблДок/ИтогТабл/Основание': ['Номер', 'Название'],
        'Документ/Основание': ['Номер'],
        'Файл/Документ/СвДокПРУ/СодФХЖ1/Основание': ['НаимОсн'],
        'Документ/СвДокПРУ/СодФХЖ1/Основание': ['НаимОсн']
    }

    for xml_path, osn_name_list in xml_tag_dict.items():
        _tag = root.find(xml_path)
        if _tag is not None:
            for osn_name in osn_name_list:
                _osn_num = _tag.get(osn_name)
                if _osn_num is not None:
                    _osn_num = _osn_num.upper()
                    for mask in market_mask:
                        res = re.search(mask, _osn_num)
                        if res is not None:
                            eng_market = res[1]
                            if eng_market in _markets:
                                return _markets[eng_market]

    _tag = root.find('Документ/СвДокПРУ/СодФХЖ1/ЗагСодОпер')
    if _tag is not None:
        _osn_num = _tag.get(osn_name)
        for mask in market_mask:
            res = re.search(mask, _osn_num)
            if res is not None:
                eng_market = res[1]
                if eng_market in _markets:
                    return _markets[eng_market]

    _tags = root.findall('Документ/СвСчФакт/ИнфПолФХЖ1/ТекстИнф')
    for _tag in _tags:
        _osn_num = _tag.get('Значен').upper()
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
            r'№\s*?([DVR]{3,4})-',
            r'№[\s.]*?(KOM)-',
            r'[\S]*?(SDMO)\-ATS',
            r'(KOM)-[\d]+-DAGESTEN',
            r'(KOM)-[\dA-Z]+-KURGANGK'
        )
    else:
        market_mask = (
            r'№\s?([A-Z]{4})-[A-Z\d-]+\s*ОТ',         # noqa
            r'№\s?[\w\-]+\-(SDD)\-[\d]{2}\sОТ',        # noqa
            r'№\s*?([A-Z-\d]{3,4})-', # noqa
            r'№\s*?([A-Z-\dМ]{3,4})-', # noqa
            r'№\s*?(2G-00)',
            r'(Д/УЭГ)/',
            r'(DVR)-[\d]*-[A-Z\d-]+\s*ОТ',
            r'(KOM)-[\d]*-[A-Z\d-]+-VV-\d',
            r'((DPMC|KOM|RDN|DPMV))-[A-Z\d-]*\sОТ',
            r'№\s*?(KOMMOD)-', # noqa
        )

    for mask in market_mask:
        res0 = re.search(mask, pdf_text, re.S)
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
                relpath(join(folder, file), _doc_path),
                compress_type=zipfile.ZIP_DEFLATED,
            )
    _zip_file.close()
    if not exists(dirname(_dest_path)):
        os.makedirs(dirname(_dest_path))
    move(_doc_path + '.zip', _dest_path)


def get_property_from_xml(_file: str, _tag_path: str, _tag_prop: str) -> str:
    """Получение типа документа."""
    root = ElementTree.parse(_file).getroot()
    _tag = root.find(_tag_path)
    if _tag is not None:
        return _tag.get(_tag_prop)

    if _tag is not None:
        return _tag.get('Наим')
    return ''


def pack_and_move_sbis(_doc_file: str, _dest_path: str):
    """Упаковка файлов в архив и перемещение в целевую папку."""
    _doc_path = dirname(_doc_file)
    _short_file_name = basename(_doc_file)

    if (_short_file_name.upper().startswith('DP_REZRUISP') or
            _short_file_name.upper().endswith('.XLS')
            or (_short_file_name.upper().startswith('MOSEGENE')
                and _short_file_name.upper().endswith('PDF')) or
            (_short_file_name.upper().startswith('АКТ СВЕРКИ')
             and _short_file_name.upper().endswith('PDF')) or
            (_short_file_name.upper().startswith('АКТ-ПРИЕМА ПЕРЕДАЧИ')
             and _short_file_name.upper().endswith('PDF')) or
            (_short_file_name.upper().startswith(
                'АКТ_ПРИЕМА-ПЕРЕДАЧИ_МОЩНОСТИ'
            )
             and _short_file_name.upper().endswith('PDF'))):
        _doc_type = 'ДОП'
    elif 'АКТЫ СВЕРКИ МОСЭНЕРГО' in _doc_file.upper():
        _doc_type = 'АСВ'
    elif 'ON_ACCOUNTS' in _doc_file.upper():
        _doc_type = 'АСВ'
    elif 'СЧЕТ__№' in _doc_file.upper():
        _doc_type = 'ДОП'
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
                    os.rename(
                        join(folder, file),
                        join(folder, 'ПЕЧАТНАЯ ФОРМА' + file)
                    )
                    file = 'ПЕЧАТНАЯ ФОРМА' + file
                _zip_file.write(
                    join(folder, file),
                    relpath(join(folder, file), _doc_path),
                    compress_type=zipfile.ZIP_DEFLATED,
                )

            if (file.upper().startswith('DP_IZVPOL') and
                    file.upper().endswith('.XML') and
                    get_property_from_xml(
                        join(folder, file),
                        'Документ/СвИзвПолуч/СведПолФайл',
                        'ИмяПостФайла'
                    ).upper() in _short_file_name.upper()):
                _zip_file.write(
                    join(folder, file),
                    relpath(join(folder, file), _doc_path),
                    compress_type=zipfile.ZIP_DEFLATED,
                )

                for sig_file in os.listdir(folder):
                    full_sig_file = join(folder, sig_file)
                    if (sig_file.upper().endswith('.SGN')
                            and os.path.splitext(file.upper())[0]
                            in sig_file.upper()):
                        _zip_file.write(
                            full_sig_file,
                            relpath(full_sig_file, _doc_path),
                            compress_type=zipfile.ZIP_DEFLATED,
                        )

            if (file.upper().startswith('DP_PDOTPR')
                    and file.upper().endswith('.XML')
                    and get_property_from_xml(
                        join(folder, file),
                        'Документ/СведПодтв/СведОтпрФайл',
                        'ИмяПостФайла',
                    ).upper()
                    in _short_file_name.upper()):
                _zip_file.write(
                    join(folder, file),
                    relpath(join(folder, file), _doc_path),
                    compress_type=zipfile.ZIP_DEFLATED,
                )

                for sig_file in os.listdir(folder):
                    full_sig_file = join(folder, sig_file)
                    if (sig_file.upper().endswith('.SGN')
                            and os.path.splitext(file.upper())[0]
                            in sig_file.upper()):
                        _zip_file.write(
                            full_sig_file,
                            relpath(full_sig_file, _doc_path),
                            compress_type=zipfile.ZIP_DEFLATED,
                        )

            if (file.upper().startswith('DP_PDPOL')
                    and _doc_type.upper() == 'СЧФ'):
                _zip_file.write(
                    join(folder, file),
                    relpath(join(folder, file), _doc_path),
                    compress_type=zipfile.ZIP_DEFLATED,
                )

            if ((file.upper().startswith('ON_NSCHFDOPPOK')
                    or file.upper().startswith('DP_REZRUZAK')) and
                    _doc_type.upper() == 'ДОП'):
                _zip_file.write(
                    join(folder, file),
                    relpath(join(folder, file), _doc_path),
                    compress_type=zipfile.ZIP_DEFLATED,
                )

            if (file.upper().startswith('DP_UVPRIEM')
                    and _doc_type.upper() == 'СЧФ'):
                _zip_file.write(
                    join(folder, file),
                    relpath(join(folder, file), _doc_path),
                    compress_type=zipfile.ZIP_DEFLATED,
                )

    _zip_file.close()
    if not exists(dirname(_dest_path)):
        os.makedirs(dirname(_dest_path))
    move(join(_doc_path, 'sbis') + '.zip', _dest_path)


def get_document_type(root: ElementTree.Element, filename='') -> str:
    """Получение типа документа xml."""
    _doc_types = {
        'DP_REZRUISP': 'АПП',
        'ON_ACCOUNTS': 'АСВ',
    }
    for key, value in _doc_types.items():
        if filename.upper().startswith(key):
            return value
        elif filename.upper().startswith(key):
            return value

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


def process_xml(_supplier_path: str, xml_file: str) -> str:
    """Процедура обработки XML."""
    root = ElementTree.parse(xml_file).getroot()
    _short_name = basename(xml_file)

    is_success = False

    # определение типа документа (АСВ, АПП, СЧФ)
    doc_type = get_document_type(root, basename(xml_file))

    # определение рынка
    market_type = get_market_xml(root)

    doc_number, doc_date = get_document_no_date_xml(root)
    doc_number = doc_number.replace('\\', '_')
    doc_number = doc_number.replace('/', '_')
    _date_str = doc_date.strftime(r'%d.%m.%Y')

    message_dict = {
            '_short_name': _short_name,
            '_supplier_path': _supplier_path,
            'market_type': market_type,
            'doc_type': doc_type,
            'doc_number': doc_number,
            '_date_str': _date_str
    }

    if NOT_RESOLVED in (doc_type, market_type, doc_number, doc_date):
        error_str = (
            'Ошибка разбора файла {_short_name}. Поставщик {_supplier_path}.'
            ' {market_type}: {doc_type} № {doc_number} от {_date_str}'
        )

        print(error_str.format(**message_dict))
        logger.error(error_str.format(**message_dict))

    else:
        is_success = True
        log_str = '{market_type}: {doc_type} № {doc_number} от {_date_str}'
        print(log_str.format(**message_dict))
        log_str = (
            'Поставщик {_supplier_path}. {market_type}: '
            '{doc_type} № {doc_number} от {_date_str}'
        )
        logger.info(log_str.format(**message_dict))

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
    _short_name = basename(pdf_file)

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

    message_dict = {
            '_short_name': _short_name,
            '_supplier_path': _supplier_path,
            'market_type': market_type,
            'doc_type': doc_type,
            'doc_number': doc_number,
            '_date_str': _date_str
    }
    if NOT_RESOLVED in (doc_type, doc_number, doc_date, market_type):
        error_string = (
            'Ошибка разбора файла {_short_name}. Поставщик {_supplier_path}.'
            ' {market_type}: {doc_type} № {doc_number} от {_date_str}'
        )

        print(error_string.format(**message_dict))
        logger.error(error_string.format(**message_dict))

    else:
        is_success = True
        log_str = '{market_type}: {doc_type} № {doc_number} от {_date_str}'
        print(log_str.format(**message_dict))
        log_str = (
            'Поставщик {_supplier_path}. {market_type}: '
            '{doc_type} № {doc_number} от {_date_str}'
        )

        logger.info(log_str.format(**message_dict))

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
    """Распаковка архива во временную папку."""
    is_success: bool = False
    archive_file = basename(full_archive_file)
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
                                dirname(full_doc_file),
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
                            or (doc_file.upper().endswith('.PDF')
                                and 'АКТ-ПРИЕМА ПЕРЕДАЧИ' in doc_file.upper()
                                and r'/PDF/' not in doc_file.upper())
                    ):
                        _dest_path = process_pdf(
                            supplier_path, full_doc_file
                        )
                        _result = _dest_path != ''
                        if _result:
                            pack_and_move_diadoc(
                                dirname(full_doc_file),
                                _dest_path,
                            )
                        is_success = is_success and _result
    return is_success


def is_diadoc_archive(full_path: str):
    """Функция проверяет, относится ли папка к Диадоку."""
    path = basename(full_path)
    return (os.path.isfile(full_path)
            and path.lower().endswith('.zip')
            and not path.upper().startswith('ОБРАБОТАНО_'))


def is_sbis_dir(full_path: str):
    """Функция проверяет, относится ли папка к СБИСу."""
    path = basename(full_path)
    return (os.path.isdir(full_path)
            and (path.upper().startswith('ПОСТУПЛЕНИЯ')
            or path.upper().startswith('АКТЫ СВЕРКИ')))


def is_sbis_doc_type(full_path: str):
    """Проверка типа документа (xml/pdf) для СБИС."""
    path = basename(full_path)
    xml_list = (
        'ON_NSCHFDOPPR',
        'DP_REZRUISP',
        'ON_ACCOUNTS',
    )

    for elem in xml_list:
        if path.upper().startswith(elem) and path.upper().endswith('.XML'):
            return 'XML'

    pdf_dict = {
        'ON_AKTPP': '.XLS',
        'ON_ASVER': '.XLS',
        'АКТ ПО ДОГОВОРАМ': '.PDF',
        'АКТ СВЕРКИ': '.PDF',
        'MOSEGENE_MOSENERG': '.PDF',
        'АКТ-ПРИЕМА ПЕРЕДАЧИ': '.PDF',
        'АКТ_ПРИЕМА-ПЕРЕДАЧИ_МОЩНОСТИ': '.PDF'
    }
    for mask, ext in pdf_dict.items():
        if path.upper().startswith(mask) and path.upper().endswith(ext):
            return 'PDF'
    return ''


def processing_buffer() -> None:
    """Обработка папки-буфера с выгруженными из Диадока и СБИСа архивами."""
    logger.info('------------Старт обработки------------')
    # проход по папкам поставщиков
    for supplier_path in os.listdir(BUFFER_DIR):
        full_supplier_path = join(BUFFER_DIR, supplier_path)
        if os.path.isdir(
                full_supplier_path
        ):  # проверка, что это именно папка, а не файл

            # проход по файлам внутри папок поставщиков
            for archive_file in os.listdir(full_supplier_path):
                full_archive_file = join(full_supplier_path, archive_file)
                if is_diadoc_archive(full_archive_file):  # Это Диадок
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
                                                dirname(full_doc_file),
                                                _dest_path,
                                            )
                                        is_success = is_success and _result
                                    elif (doc_file.upper().endswith('.PDF')
                                            and
                                            'ФАКТУРА' not in doc_file.upper()
                                            and
                                            'ПЕЧАТНАЯ ФОРМА' in doc_file.upper()
                                            and
                                            'ON_NSCHFDOPPR' not in doc_dir.upper()
                                          ):
                                        _dest_path = process_pdf(
                                            supplier_path, full_doc_file
                                        )
                                        _result = _dest_path != ''
                                        if _result:
                                            pack_and_move_diadoc(
                                                dirname(full_doc_file),
                                                _dest_path,
                                            )
                                        is_success = is_success and _result

                    if is_success:
                        os.rename(
                            full_archive_file,
                            join(full_supplier_path,
                                 'Обработано_' + archive_file
                                 ),
                        )
                elif is_sbis_dir(full_archive_file):  # Это СБИС:
                    is_success = True
                    # если в папке есть не обработанные архивы,
                    # тогда и показываем, что делаем обработку папки
                    logger.info('Обработка папки %s', supplier_path)
                    print(f'----------{supplier_path}----------')

                    for doc_file in os.listdir(full_archive_file):
                        full_doc_file = join(full_archive_file, doc_file)
                        if os.path.isfile(full_doc_file):
                            sbis_doc_type = is_sbis_doc_type(full_doc_file)

                            if sbis_doc_type == 'XML':
                                _dest_path = process_xml(
                                    supplier_path,
                                    full_doc_file
                                )
                                _result = _dest_path != ''
                                if _result:
                                    pack_and_move_sbis(
                                        full_doc_file,
                                        _dest_path
                                    )
                                is_success = is_success and _result
                            elif sbis_doc_type == 'PDF':
                                _dest_path = process_pdf(
                                    supplier_path,
                                    full_doc_file
                                )
                                _result = _dest_path != ''
                                if _result:
                                    pack_and_move_sbis(
                                        full_doc_file,
                                        _dest_path
                                    )
                                is_success = is_success and _result
                            else:
                                _dest_path = ''

                    if is_success:
                        with open(
                                join(
                                    full_supplier_path,
                                    f'Обработано {archive_file}.txt'
                                ),
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
