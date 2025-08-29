# -*- coding: utf-8 -*-
"""
Created on Mon Aug 25 09:45:22 2025

@author: a.karabedyan
"""

import zipfile
from io import BytesIO
from pathlib import Path
from typing import List, Iterable
import math
import re, os
from colorama import init, Fore, Style
import pandas as pd
from typing import Literal, Optional
from itertools import product

init(autoreset=True)
from custom_errors import IncorrectFolderOrFilesPath
# Глобальные константы
MAX_EXCEL_ROWS = 1_000_000
COLUMN_PREFIXES = ('Операция_', 'Содержание_', 'Документ_', 'Аналитика Дт_', 'Субконто Дт_', 'Аналитика Кт_', 'Субконто Кт_', 'Level_')



def sort_columns(df: pd.DataFrame, desired_order: List[str]) -> pd.DataFrame:
    cols = df.columns.tolist()
    
    # 1. Фиксированные колонки (не имеющие числового суффикса)
    fixed_cols = [
        col for col in desired_order 
        if col in cols and not any(col.startswith(prefix) for prefix in COLUMN_PREFIXES)
    ]

    # 2. Группируем колонки по префиксам
    grouped_cols = {}
    for prefix in COLUMN_PREFIXES:
        prefix_cols = [col for col in cols if col.startswith(prefix)]
        grouped_cols[prefix] = sorted(
            prefix_cols, 
            key=lambda x: int(re.search(rf"{re.escape(prefix)}(\d+)", x).group(1) or 0)
        )
    
    # --- Новая часть для обработки колонок вида:
    # [Количество_|ВалютнаяСумма_]<число или число.число или число.буквы>_<до|ко>
    
    # Определяем префиксы для группировки
    special_prefixes = ['', 'Количество_', 'ВалютнаяСумма_']
    # Суффиксы, которые идут в конце
    suffixes = ['_до', '_ко']
    
    # Функция для разбора ключа сортировки
    def parse_key(col):
        # Сначала выделим префикс из special_prefixes
        prefix = ''
        for sp in special_prefixes:
            if col.startswith(sp):
                prefix = sp
                break
        # Уберем префикс
        remainder = col[len(prefix):]
        
        # Проверим, что remainder заканчивается на _до или _ко
        for suf in suffixes:
            if remainder.endswith(suf):
                number_part = remainder[:-len(suf)]
                suffix_part = suf
                break
        else:
            # Если не подходит под шаблон, возвращаем None
            return None
        
        # Теперь number_part может быть число, число.число или число.буквы
        # Разберём число и буквенную часть
        # Например: 60, 8.0, 76.ФВ
        m = re.match(r"(\d+(?:\.\d+)?)(?:\.([А-Я]+))?$", number_part)
        if m:
            number = float(m.group(1))
            letters = m.group(2) or ''
        else:
            # Если не подошло, ставим очень большой ключ, чтобы в конец
            number = float('inf')
            letters = ''
        
        # Возвращаем кортеж для сортировки:
        # 1) индекс префикса в special_prefixes (чтобы сортировать по префиксу)
        # 2) число
        # 3) буквы
        # 4) суффикс _до раньше _ко (например)
        suf_order = {'_до': 0, '_ко': 1}
        
        return (special_prefixes.index(prefix), number, letters, suf_order.get(suffix_part, 10))
    
    # Собираем все колонки, которые подходят под этот шаблон
    special_cols = [col for col in cols if parse_key(col) is not None]
    # Сортируем их по ключу
    special_cols_sorted = sorted(special_cols, key=parse_key)
    
    # 3. Собираем итоговый порядок
    ordered_cols = fixed_cols[:]
    for prefix in COLUMN_PREFIXES:
        ordered_cols.extend(grouped_cols.get(prefix, []))
    ordered_cols.extend(special_cols_sorted)
    
    # 4. Добавляем остальные
    other_cols = set(cols) - set(ordered_cols)
    ordered_cols.extend(other_cols)
    
    return df[ordered_cols]


def write_df_in_chunks(
    writer: pd.ExcelWriter,
    df: pd.DataFrame,
    base_sheet_name: str,
    max_rows: int = MAX_EXCEL_ROWS
) -> None:
    """Записывает DataFrame в Excel частями с учетом ограничения на строки"""
    if df.empty:
        return
        
    n_chunks = math.ceil(len(df) / max_rows)
    
    for i in range(n_chunks):
        start = i * max_rows
        end = min((i + 1) * max_rows, len(df))
        sheet_name = f"{base_sheet_name}{i + 1}" if n_chunks > 1 else base_sheet_name
        
        df.iloc[start:end].to_excel(
            writer,
            sheet_name=sheet_name[:31],  # Ограничение длины имени листа
            index=False
        )

def print_start_text() -> None:
    """Выводит стартовое сообщение"""

    title = "Обработчик самых популярных регистров 1С"
    subtitle = "Формирует удобную для анализа плоскую таблицу"
    
    header = f"{Fore.CYAN}{'=' * 60}\n{title.center(60)}\n{subtitle.center(60)}\n{'=' * 60}{Style.RESET_ALL}"
    
    sections = [
        (
            Fore.YELLOW + "Важные моменты:", 
            [
                "1) Выгружайте только формат .xlsx:",
                "   - Текст 1",
                "   - Текст 2\n",
                "2) Момент 2:",
                "   - Тест 3",
                "   - Текст 4\n"
            ]
        ),
    ]
    clear_console()  # Очищаем консоль перед выводом
    print(header)
    # for header, content in sections:
    #     print(header)
    #     for line in content:
    #         print(Style.RESET_ALL + line)


def print_instruction_color(type_registr: Literal['posting', 'card', 'analisys', 'turnover']) -> None:
    """Выводит цветную инструкцию по использованию программы"""
    REGISTER_DISPLAY_NAMES = {
        'posting': 'Отчета по проводкам',
        'card': 'Карточки счета',
        'analisys': 'Анализа счета',
        'turnover': 'Оборотов счета'
    }
    
    REGISTER_HEADERS_MAP = {
        'posting': ['|Дата|Документ|Содержание|',
                    '|Период|Документ|Аналитика Дт|Аналитика Кт|'],
        'card': ['|Дата|Документ|Операция|',
                 '|Период|Документ|Аналитика Дт|Аналитика Кт|'],
        'analisys': ['Счет|Кор.счет|С кред. счетов|В дебет счетов',
                     'Счет|Кор. Счет|Дебет|Кредит'],
        'turnover': ['Субконто|Нач. сальдо деб.|Нач. сальдо кред.|Деб. оборот|Кред. оборот|Кон. сальдо деб.|Кон. сальдо кред.',
                     'Счет|Начальное сальдо Дт|Начальное сальдо Кт|Оборот Дт|Оборот Кт|Конечное сальдо Дт|Конечное сальдо Кт']
    }
    
    # Определяем особенности в зависимости от типа регистра
    FEATURES_MAP = {
        'posting': [
            "- Результаты пакетной обработки сохраняются на листах UPP и Non_UPP",
            "- Таблицы >1 млн строк разбиваются на части",
            "- Ожидаемое время обработки: 500 тыс. строк ~ 2 мин\n"
        ],
        'card': [
            "- Результаты пакетной обработки сохраняются на листах UPP и Non_UPP",
            "- Таблицы >1 млн строк разбиваются на части",
            "- Ожидаемое время обработки: 500 тыс. строк ~ 2 мин\n"
        ],
        'analisys': [
            "- Выгрузки должны иметь иерархическую структуру (+ и - на полях слева)",
            "- Регистры из 1С: Предприятие 8.3 (Конфигурация Управление производственным предприятием) должны быть выгружены с установленной опцией 'по субсчетам'",
            "- Аналитика для основного счета - без ограничений уровня вложенности",
            "- Субконто корреспондирующих счетов не должно превышать одного уровня вложенности",
            "- Для не УПП баз 1с для корректного отражения данных в валюте в настройках на вкладке Группировка устанавливайте пункт Валюта, Показатели - Валютная сумма",
            "- Расшифровка аналитики (субконто) должна включать в себя только элементы, не устанавливайте опции 'иерархия', 'только иерархия', 'по группам' и т.д.\n"
        ],
        'turnover': [
            "- Выгрузки должны иметь иерархическую структуру (+ и - на полях слева)",
            "- Для УПП баз 1с для корректного отражения данных в валюте в настройках на вкладке Общие - Выводить данные устанавливайте пункт Валютам",
            "- Не используйте опцию По субсчетам для корреспондирующих счетов",
            "- Регистры из 1С: Предприятие 8.3 (Конфигурация Управление производственным предприятием) должны быть выгружены с установленной опцией 'по субсчетам'",
        ]
    }

    title = f"Обработчик {REGISTER_DISPLAY_NAMES.get(type_registr)} 1С"
    subtitle = "Формирует удобную для анализа плоскую таблицу"
    
    header = f"{Fore.CYAN}{'=' * 60}\n{title.center(60)}\n{subtitle.center(60)}\n{'=' * 60}{Style.RESET_ALL}"
    
    sections = [
        (
            Fore.YELLOW + "Режимы работы:", 
            [
                "1) Обработка регистров по отдельности:",
                "   - Перетягивайте файл в окно программы",
                "   - Результат откроется в отдельном Excel-файле\n",
                "2) Пакетная обработка (сводная таблица):",
                "   - Перетягивайте папку с файлами",
                "   - Результаты будут в одном Excel-файле\n"
            ]
        ),
        (
            Fore.YELLOW + "Поддерживаемые версии 1С:", 
            [
                "1) Управление производственным предприятием (1С 8.3):",
                f"   - Заголовки регистра: {REGISTER_HEADERS_MAP[type_registr][0]}\n",
                "2) Бухгалтерия предприятия, ERP Агропромышленный комплекс,",
                "   ERP Управление предприятием 2:",
                f"   - Заголовки регистра: {REGISTER_HEADERS_MAP[type_registr][1]}\n",
                "3) Прочие версии с заголовками из пунктов выше.\n"
            ]
        ),
        (
            Fore.YELLOW + "Особенности:", 
            FEATURES_MAP[type_registr]  # Используем соответствующий список особенностей
        )
    ]
    
    clear_console()  # Очищаем консоль перед выводом
    print(header)
    for header, content in sections:
        print(header)
        for line in content:
            print(Style.RESET_ALL + line)


def validate_paths(paths: Iterable[Path]) -> List[Path]:
    """Проверяет валидность путей и возвращает список исправленных путей.
    Генерирует ошибку, если хоть один путь не существует и не может быть исправлен."""
    
    if not paths:
        raise IncorrectFolderOrFilesPath('Неверные пути к папке или файлу/файлам.')
    
    normalized_paths = []
    invalid_paths = []
    
    for path in paths:
        try:
            resolved_path = path.resolve()
            normalized = normalize_path(resolved_path)
            
            if normalized and normalized.exists():
                normalized_paths.append(normalized)
            else:
                # Запоминаем невалидный путь
                invalid_paths.append(str(resolved_path))
                normalized_paths.append(resolved_path)  # Все равно добавляем для возврата
                
        except (OSError, AttributeError) as e:
            # Запоминаем невалидный путь с информацией об ошибке
            invalid_paths.append(f"{path} (ошибка: {str(e)})")
            normalized_paths.append(path)  # Все равно добавляем для возврата
    
    # Если есть невалидные пути, генерируем ошибку
    if invalid_paths:
        error_message = 'Неверные пути к папке или файлу/файлам:\n' + '\n'.join(invalid_paths)
        raise IncorrectFolderOrFilesPath(error_message)
    
    return normalized_paths

def fix_1c_excel_case(file_path: Path) -> BytesIO:
    """Исправляет регистр имен в xlsx-архивах 1С"""
    try:
        with zipfile.ZipFile(file_path, 'r') as z:
            new_zip = BytesIO()
            
            with zipfile.ZipFile(new_zip, 'w') as new_z:
                for item in z.infolist():
                    # Исправляем только проблемные имена
                    new_name = (
                        'xl/sharedStrings.xml' 
                        if item.filename == 'xl/SharedStrings.xml' 
                        else item.filename
                    )
                    new_z.writestr(new_name, z.read(item))
        
        new_zip.seek(0)
        return new_zip
        
    except PermissionError as e:
        raise PermissionError(
            f"Файл {file_path.name} открыт в другой программе. Закройте его."
        ) from e
    except Exception as e:
        raise RuntimeError(
            f"Ошибка обработки файла {file_path.name}: {str(e)}"
        ) from e



def generate_dash_combinations(path_str: str) -> List[str]:
    """Генерирует все комбинации тире в строке."""
    # Находим индексы тире
    dash_indices = [i for i, char in enumerate(path_str) if char in ('-', '—')]
    
    if not dash_indices:
        return [path_str]  # Если нет тире, возвращаем оригинальную строку

    # Генерируем все комбинации замены
    combinations = []
    for combo in product(*[(path_str[i:i+1].replace('-', '—'), path_str[i:i+1].replace('—', '-')) 
                         if i in dash_indices else (path_str[i:i+1],) 
                         for i in range(len(path_str))]):
        combinations.append(''.join(combo))
    
    return combinations

def normalize_path(original_path: Path) -> Optional[Path]:
    """Находит валидный путь, перебирая комбинации тире."""
    # Проверяем изначальную валидность
    if original_path.exists():
        return original_path

    # Преобразуем объект Path в строку
    path_str = str(original_path)

    # Генерируем все комбинации тире
    for new_path_str in generate_dash_combinations(path_str):
        new_path = Path(new_path_str)

        # Проверяем валидность нового пути
        if new_path.exists():
            return new_path

    return None  # Если валидный путь не найден


def clear_console():
    """Очищает консоль"""
    os.system('cls' if os.name == 'nt' else 'clear')
    
