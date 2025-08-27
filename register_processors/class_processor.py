# -*- coding: utf-8 -*-
"""
Created on Mon Aug 25 12:22:19 2025

@author: a.karabedyan
"""

from abc import ABC, abstractmethod
from pathlib import Path
from io import BytesIO
from typing import Union
from colorama import init, Fore
from typing import Literal, List

import pandas as pd
import openpyxl

from custom_errors import RegisterProcessingError
init(autoreset=True)
DESIRED_ORDER = {'card':{
                    'upp': [
                        'Имя_файла', 'Дата', 'Документ', 'Дебет', 'Дебет_значение', 'Кредит', 
                        'Кредит_значение', 'Текущее сальдо', 'Текущее сальдо_значение', 
                        'Операция_1', 'Операция_2', 'Операция_3', 'Операция_4', 'Операция_5', 
                        'Операция_6', 'Дт_количество', 'Кт_количество', 'Дт_валюта', 
                        'Дт_валютая_сумма', 'Кт_валюта', 'Кт_валютая_сумма'
                    ],
                    'not_upp': [
                        'Имя_файла', 'Период', 'Дебет', 'Дебет_значение', 'Кредит', 
                        'Кредит_значение', 'Текущее сальдо', 'Текущее сальдо_значение', 
                        'Документ_1', 'Документ_2', 'Аналитика Дт_1', 'Аналитика Дт_2', 
                        'Аналитика Дт_3', 'Аналитика Дт_4', 'Аналитика Кт_1', 'Аналитика Кт_2', 
                        'Аналитика Кт_3', 'Аналитика Кт_4', 'Дебет_количество', 
                        'Кредит_количество', 'Дебет_валюта', 'Дебет_валютное_количество', 
                        'Кредит_валюта', 'Кредит_валютное_количество'
                    ]},
                'posting':{
                    'upp': [
                        'Имя_файла', 'Дата', 'Документ', 'Дт', 'Кт', 'Сумма', 
                        'Содержание_1', 'Содержание_2', 'Содержание_3',
                        'Субконто Дт1', 'Субконто Дт2', 'Субконто Дт3',
                        'Субконто Кт1', 'Субконто Кт2', 'Субконто Кт3',
                        'Дт_количество', 'Кт_количество',
                        'Дт_валюта', 'Дт_валюта_количество',
                        'Кт_валюта', 'Кт_валюта_количество'
                        ],
                    'not_upp': [
                        'Имя_файла', 'Период', 'Дебет', 'Дебет_значение', 'Кредит', 'Кредит_значение',
                        'Документ_1', 'Документ_2',
                        'Аналитика Дт_1', 'Аналитика Дт_2', 'Аналитика Дт_3', 'Аналитика Дт_4',
                        'Аналитика Кт_1', 'Аналитика Кт_2', 'Аналитика Кт_3', 'Аналитика Кт_4',
                        'Дебет_количество', 'Кредит_количество',
                        'Дебет_валюта', 'Дебет_валютное_количество',
                        'Кредит_валюта', 'Кредит_валютное_количество'
                    ]},
                'analisys':{
                    'upp': [
                        'Исх.файл', 'Субсчет', 'Аналитика', 'Корр_счет', 'Субконто_корр_счета', 'Вид связи КА за период','С кред. счетов', 
                        'С кред. счетов_КОЛ', 'В дебет счетов', 'В дебет счетов_КОЛ',
                        'Level_0', 'Level_1', 'Level_2'
                        ],
                    'not_upp': [
                        'Исх.файл', 'Субсчет', 'Аналитика', 'Корр_счет', 'Субконто_корр_счета', 'Вид связи КА за период','С кред. счетов', 
                        'С кред. счетов_КОЛ', 'В дебет счетов', 'В дебет счетов_КОЛ',
                        'Level_0', 'Level_1', 'Level_2'
                    ]}
}

class FileProcessor(ABC):
    """Абстрактный базовый класс для обработчиков файлов"""
    def __init__(self):
        self.table_for_check = pd.DataFrame()  # для хранения данных по оборотам до обработки в таблицах
        self.file = 'NoNameFile'
    
    @staticmethod
    def _is_parent(account: str, accounts: List[str]) -> bool:
        """
        Проверяет, есть ли субсчета у заданного счета.
    
        :param account: Счет, для которого проверяются субсчета.
        :param accounts: Список всех счетов.
        :return: True, если есть хотя бы один субсчет; иначе False.
        """
        return any(acc.startswith(account + '.') and acc != account for acc in accounts)
    
    @staticmethod
    def _is_accounting_code_vectorized(series: pd.Series) -> pd.Series:
        """
        Векторизованная версия для работы с целыми сериями.
        """
        # Конвертируем в строку
        str_series = series.astype(str)
        
        # Быстрые проверки
        result = pd.Series(False, index=series.index)
        
        # Специальные значения
        special_mask = str_series.isin(["0", "00", "000"])
        result[special_mask] = True
        
        # Проверяем наличие точки
        has_dot = str_series.str.contains('.', regex=False)
        
        # Для значений без точки - простые цифровые проверки
        no_dot_mask = ~has_dot
        numeric_no_dot = str_series[no_dot_mask].str.isdigit()
        valid_length_no_dot = str_series[no_dot_mask].str.len() <= 2
        result[no_dot_mask & numeric_no_dot & valid_length_no_dot] = True
        
        # Для значений с точкой - сложная проверка
        dot_values = str_series[has_dot]
        if not dot_values.empty:
            # Разделяем на части
            parts = dot_values.str.split('.')
            
            # Проверяем каждую часть
            valid_parts = parts.apply(lambda x: all(
                (p.isdigit() and len(p) <= 2) or (p.isalpha() and len(p) <= 2)
                for p in x if p  # Пропускаем пустые части
            ))
            
            # Проверяем наличие хотя бы одной цифровой части
            has_digit = parts.apply(lambda x: any(p.isdigit() for p in x))
            
            # Приведение к типу bool с использованием .to_numpy()
            result[has_dot] = (valid_parts & has_digit).to_numpy().astype(bool)
        
        return result

    
    @staticmethod
    def _is_accounting_code(value: Union[str, int, float]) -> bool:
        """
        Проверяет, является ли значение бухгалтерским счетом. На вход - строка.
        """
        if value:
            if str(value) in ["00", "000"]:
                return True
            try:
                parts = str(value).split('.')
                has_digit = any(part.isdigit() for part in parts)
                # Проверка, состоит ли каждая часть только из цифр (длиной 1 или 2) или (если длина меньше 3) только из букв
                return has_digit and all(
                    (part.isdigit() and len(part) <= 2) or (len(part) < 3 and part.isalpha()) for part in parts)
            except TypeError:
                return False
        else:
            return False
    
    @staticmethod
    def _preprocessor_openpyxl(file_like_object: BytesIO) -> pd.DataFrame:
        """Читает файлы Excel и добавляет столбцы с группировками и курсивом без сохранения файлов"""

        # Загружаем книгу из BytesIO
        workbook = openpyxl.load_workbook(file_like_object)
        
        sheet = workbook.active
        
        # Получаем данные об уровнях группировок
        grouping_levels = []
        for row_index in range(1, sheet.max_row + 1):
            grouping_levels.append(sheet.row_dimensions[row_index].outline_level)
        
        # Получаем данные о курсивных ячейках
        italic_flags = [0] * sheet.max_row  # Инициализируем нулями
        
        # Ищем столбец с определенными значениями
        found_kor_schet_col = None
        for row in sheet.iter_rows(values_only=True):
            found_value = next((value for value in ['Кор. Счет', 'Кор.счет'] if value in row), None)
            if found_value is not None:
                found_kor_schet_col = row.index(found_value) + 1
                break
        
        # Если нашли нужный столбец, проверяем курсив
        if found_kor_schet_col:
            for row_index in range(2, sheet.max_row + 1):
                kor_schet_cell = sheet.cell(row=row_index, column=found_kor_schet_col)
                italic_flags[row_index - 1] = 1 if kor_schet_cell.font and kor_schet_cell.font.italic else 0
        
        # Читаем данные в DataFrame
        data = []
        for row in sheet.iter_rows(values_only=True):
            data.append(row)
        
        df = pd.DataFrame(data[1:], columns=data[0])  # Пропускаем заголовок
        
        # Добавляем столбцы с группировками и курсивом
        df.insert(0, 'Уровень_группировки', grouping_levels[1:])  # Пропускаем заголовок
        df.insert(1, 'Курсив', italic_flags[1:])  # Пропускаем заголовок
        
        workbook.close()
        
        return df
    
    @staticmethod
    def _process_dataframe_optimized(df: pd.DataFrame) -> pd.DataFrame:
        # Поиск строки с "Дата"
        first_col = df.iloc[:, 0].astype(str).str.lower()
        mask = first_col.str.contains('дата')
        
        if not mask.any():
            raise RegisterProcessingError(Fore.RED + 'Файл не является Отчетом по проводкам 1с.\n')
        
        date_row_idx = mask.idxmax()
        
        # Установка заголовков и очистка
        df.columns = df.iloc[date_row_idx].str.strip()
        df = df.iloc[date_row_idx + 1:].copy()
        
        # Преобразование даты
        # df['Дата'] = pd.to_datetime(df['Дата'], dayfirst=True, errors='coerce')
        df['Дата'] = pd.to_datetime(df['Дата'], format='%d.%m.%Y', errors='coerce')
        
        
        
        # Добавляем порядковый номер к повторяющимся значениям документов
        mask = df['Документ'].notna()
        df.loc[mask, 'Документ'] = (
            df.loc[mask, 'Документ'] 
            + '_end' 
            + df.loc[mask].groupby('Документ').cumcount().add(1).astype(str)
        )
        
        # Заполнение пропусков
        df['Дата'] = df['Дата'].ffill()
        df['Документ'] = df['Документ'].infer_objects()
        df['Документ'] = df['Документ'].ffill()
        
        # Переименовываем пустые или NaN заголовки
        df.columns = [
            f'NoNameCol {i+1}' if pd.isna(col) or col == '' else col
            for i, col in enumerate(df.columns)
        ]
        
        # Удаление пустых строк и столбцов
        df = df[df['Дата'].notna()].copy()
        df = df.dropna(how='all').dropna(how='all', axis=1)
        
        return df
    
    def shiftable_level(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Выравнивает столбцы таким образом, чтобы бухгалтерские счета находились в одном столбце.
        """
        if not df.empty:
            list_lev = [i for i in df.columns.to_list() if 'Level' in i]
            continue_shifting = True
            iteration = 0
    
            while continue_shifting:
                continue_shifting = False
                iteration += 1
                previous_table_state = df.copy()
    
                for i in list_lev:
                    # Проверяем, есть ли в столбце и субсчет, и субконто
                    if self._is_accounting_code_vectorized(df[i]).nunique() == 2:
                        lm = int(i.split('_')[-1])
                        new_list_lev = list_lev[lm:]
                        
                        # Создаем булеву маску для строк, где первый столбец new_list_lev содержит бухгалтерский код
                        mask = self._is_accounting_code_vectorized(df[new_list_lev[0]])
                        
                        # Применяем сдвиг только к строкам, где mask == True
                        df.loc[mask, new_list_lev] = df.loc[mask, new_list_lev].values
                        
                        # Для остальных строк сдвигаем по-другому (если нужно)
                        if lm > 0:
                            other_cols = list_lev[lm - 1:-1]
                            df.loc[~mask, new_list_lev[:len(other_cols)]] = df.loc[~mask, other_cols].values
                        
                        continue_shifting = True
    
                if previous_table_state.equals(df):
                    break
        return df

    
    def find_max_level_column(self, df: pd.DataFrame) -> str:
        """
        Находит столбец Level_ с максимальным индексом, все значения которого возвращают True
        в методе _is_accounting_code_vectorized.
        """
        # Получаем все столбцы, начинающиеся с 'Level_'
        level_columns = [col for col in df.columns if col.startswith('Level_')]
        
        max_level_column = None
        max_index = -1
    
        for col in level_columns:
            # Проверяем, все ли значения в столбце возвращают True
            if self._is_accounting_code_vectorized(df[col]).all():
                # Извлекаем индекс из имени столбца
                index = int(col.split('_')[1])
                # Обновляем максимальный индекс и соответствующий столбец
                if index > max_index:
                    max_index = index
                    max_level_column = col
    
        return max_level_column


    @abstractmethod
    def process_file(self, file_path: Path) -> pd.DataFrame:
        pass