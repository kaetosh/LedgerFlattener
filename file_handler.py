# # -*- coding: utf-8 -*-
# """
# Created on Mon Aug 25 10:44:30 2025

# @author: a.karabedyan
# """

'''
Однопроцессорность
'''

import sys
import os
import subprocess
import tempfile
from pathlib import Path
from typing import List, Literal, Dict, Tuple, Optional

import pandas as pd
from tqdm import tqdm
from colorama import init, Fore

from halo import Halo



from multiprocessing import Pool


# Импорт процессоров
from register_processors.class_processor import FileProcessor, DESIRED_ORDER
from register_processors.card_processor import Card_UPPFileProcessor, Card_NonUPPFileProcessor
from register_processors.posting_processor import Posting_UPPFileProcessor, Posting_NonUPPFileProcessor
from register_processors.analisys_processor import Analisys_UPPFileProcessor, Analisys_NonUPPFileProcessor
from register_processors.turnover_processor import Turnover_UPPFileProcessor, Turnover_NonUPPFileProcessor
from register_processors.accountosv_processor import AccountOSV_UPPFileProcessor, AccountOSV_NonUPPFileProcessor
from register_processors.generalosv_processor import GeneralOSV_UPPFileProcessor, GeneralOSV_NonUPPFileProcessor

from support_functions import fix_1c_excel_case, sort_columns, write_df_in_chunks
from custom_errors import RegisterProcessingError, NoRegisterFilesFoundError, NoExcelFilesFoundError

# Настройка pandas
pd.set_option('display.max_columns', None)
pd.set_option('display.expand_frame_repr', False)
init(autoreset=True)


class ExcelValidator:
    """Валидатор Excel файлов"""
    @staticmethod
    def is_valid_excel(file_path: Path) -> bool:
        return file_path.suffix.lower() == '.xlsx'


class FileProcessorFactory:
    """Фабрика для создания обработчиков файлов"""
    
    REGISTER_DISPLAY_NAMES = {
        'posting': 'Отчет по проводкам',
        'card': 'Карточка счета', 
        'analisys': 'Анализ счета',
        'turnover': 'Обороты счета',
        'accountosv': 'ОСВ счета',
        'generalosv': 'общая ОСВ'
    }
    
    REGISTER_PATTERNS = {
        'card': [
            {'pattern': {'дата', 'документ', 'операция'}, 'processor': Card_UPPFileProcessor},
            {'pattern': {'период', 'аналитика дт', 'аналитика кт'}, 'processor': Card_NonUPPFileProcessor}
        ],
        'posting': [
            {'pattern': {'дата', 'документ', 'содержание', 'дт', 'кт', 'сумма'}, 'processor': Posting_UPPFileProcessor},
            {'pattern': {'период', 'аналитика дт', 'аналитика кт'}, 'processor': Posting_NonUPPFileProcessor}
        ],
        'analisys': [
            {'pattern': {'счет', 'кор.счет', 'с кред. счетов', 'в дебет счетов'}, 'processor': Analisys_UPPFileProcessor},
            {'pattern': {'счет', 'кор. счет', 'дебет', 'кредит'}, 'processor': Analisys_NonUPPFileProcessor}
        ],
        'turnover': [
            {'pattern': {'субконто', 'нач. сальдо деб.', 'нач. сальдо кред.', 'деб. оборот', 'кред. оборот', 'кон. сальдо деб.', 'кон. сальдо кред.'}, 'processor': Turnover_UPPFileProcessor},
            {'pattern': {'счет', 'начальное сальдо дт', 'начальное сальдо кт', 'оборот дт', 'оборот кт', 'конечное сальдо дт', 'конечное сальдо кт'}, 'processor': Turnover_NonUPPFileProcessor}
        ],
        'accountosv': [
            {'pattern': {'субконто', 'сальдо на начало периода', 'оборот за период', 'сальдо на конец периода'}, 'processor': AccountOSV_UPPFileProcessor},
            {'pattern': {'счет', 'сальдо на начало периода', 'обороты за период', 'сальдо на конец периода'}, 'processor': AccountOSV_NonUPPFileProcessor}
        ],
        'generalosv': [
            {'pattern': {'счет', 'сальдо на начало периода', 'оборот за период', 'сальдо на конец периода'}, 'processor': GeneralOSV_UPPFileProcessor},
            {'pattern': {'счет', 'наименование счета', 'сальдо на начало периода', 'обороты за период', 'сальдо на конец периода'}, 'processor': GeneralOSV_NonUPPFileProcessor}
        ]
    }

    @classmethod
    def get_processor(cls, file_path: Path, type_register: str) -> FileProcessor:
        """Получить процессор для файла на основе шаблонов заголовков"""
        fixed_data = fix_1c_excel_case(file_path)
        
        # Чтение только первых строк для анализа
        df = pd.read_excel(fixed_data, header=None, nrows=20)
        str_df = df.map(lambda x: str(x).strip().lower() if pd.notna(x) else '')
        
        for pattern_config in cls.REGISTER_PATTERNS[type_register]:
            if cls._contains_pattern(str_df, pattern_config['pattern']):
                return pattern_config['processor']()
        
        raise RegisterProcessingError("Файл не является корректным регистром из 1С.")

    @staticmethod
    def _contains_pattern(df: pd.DataFrame, pattern: set) -> bool:
        """Проверить, содержит ли DataFrame заданный паттерн"""
        for _, row in df.iterrows():
            if pattern.issubset(set(row)):
                return True
        return False


class ResultCollector:
    """Класс для сбора и управления результатами обработки"""
    
    def __init__(self):
        self.upp_results = []
        self.non_upp_results = []
        
        self.analisys_results = []
        self.analisys_processor = None
        self.analisys_checks = []
        
        self.turnover_results = []
        self.turnover_processor = None
        self.turnover_checks = []
        
        self.accountosv_results = []
        self.accountosv_processor = None
        self.accountosv_checks = []
        
        self.generalosv_results = []
        self.generalosv_checks = []
    
    def add_result(self, processor: FileProcessor, result: pd.DataFrame, check: pd.DataFrame):
        """Добавить результат в соответствующую категорию"""
        processor_type = type(processor)
        
        if processor_type in (Card_UPPFileProcessor, Posting_UPPFileProcessor):
            self.upp_results.append(result)
        elif processor_type in (Analisys_UPPFileProcessor, Analisys_NonUPPFileProcessor):
            self.analisys_results.append(result)
            self.analisys_checks.append(check)
            self.analisys_processor = processor
        elif processor_type in (Turnover_UPPFileProcessor, Turnover_NonUPPFileProcessor):
            self.turnover_results.append(result)
            self.turnover_checks.append(check)
            self.turnover_processor = processor
        elif processor_type in (AccountOSV_UPPFileProcessor, AccountOSV_NonUPPFileProcessor):
            self.accountosv_results.append(result)
            self.accountosv_checks.append(check)
            self.accountosv_processor = processor
        elif processor_type in (GeneralOSV_UPPFileProcessor, GeneralOSV_NonUPPFileProcessor):
            self.generalosv_results.append(result)
            self.generalosv_checks.append(check)
        else:
            self.non_upp_results.append(result)
    
    def get_all_results(self) -> Dict[str, Tuple[pd.DataFrame, pd.DataFrame]]:
        """Получить все результаты в структурированном виде"""
        return {
            'upp': (self._concat_and_sort(self.upp_results, 'upp'), pd.DataFrame()),
            
            'non_upp': (self._concat_and_sort(self.non_upp_results, 'not_upp'), pd.DataFrame()),
            
            'analisys': (self._process_analisys_like(self.analisys_results, self.analisys_processor), 
                        pd.concat(self.analisys_checks) if self.analisys_checks else pd.DataFrame()),
            
            'turnover': (self._process_analisys_like(self.turnover_results, self.turnover_processor),
                        pd.concat(self.turnover_checks) if self.turnover_checks else pd.DataFrame()),
            
            'accountosv': (self._process_analisys_like(self.accountosv_results, self.accountosv_processor),
                          pd.concat(self.accountosv_checks) if self.accountosv_checks else pd.DataFrame()),
            
            'generalosv': (self._process_analisys_like(self.generalosv_results),
                          pd.concat(self.generalosv_checks) if self.generalosv_checks else pd.DataFrame())
        }
    
    def _concat_and_sort(self, results: List[pd.DataFrame], processor_type: str) -> pd.DataFrame:
        """Объединить и отсортировать результаты"""
        if not results:
            return pd.DataFrame()
        return sort_columns(pd.concat(results), DESIRED_ORDER[self.type_register][processor_type])
    
    def _process_analisys_like(self, results: List[pd.DataFrame], processor = None) -> pd.DataFrame:
        """Обработать результаты анализов и подобных регистров"""
        if not results:
            return pd.DataFrame()
        df = sort_columns(pd.concat(results), DESIRED_ORDER[self.type_register]['upp'])
        # Предполагаем, что у всех процессоров есть метод shiftable_level
        return processor.shiftable_level(df) if hasattr(processor, 'shiftable_level') else df


class FileHandler:
    """Обработчик файлов с последовательной обработкой"""
    
    def __init__(self, verbose: bool = True, max_workers: Optional[int] = None):
        self.validator = ExcelValidator()
        self.processor_factory = FileProcessorFactory()
        self.verbose = verbose
        # max_workers больше не используется, но оставлено для совместимости
        
        self.not_correct_files = {}
        self.storage_processed_registers = {}
        self.check = {}
        self.type_register = None

    def handle_input(self, input_path: Path, 
                    type_register: Literal['posting', 'card', 'analisys', 'account', 'generalosv']) -> None:
        """Обработать входной путь (файл или директория)"""
        self.type_register = type_register
        
        if input_path.is_file():
            print('Принят файл...', end='\r')
            self._process_single_file(input_path)
        elif input_path.is_dir():
            print('Принята папка...', end='\r')
            self._process_directory(input_path)
            
    
    @Halo(text='Обрабатываем файл...',
          text_color = 'blue',
          spinner={
                'interval': 500,
                'frames': ["⠋","⠙","⠹","⠸","⠼","⠴","⠦","⠧","⠇","⠏"]})
    def _process_single_file(self, file_path: Path) -> None:
        """Обработать одиночный файл со спиннером"""
        if not self.validator.is_valid_excel(file_path):
            self.not_correct_files[file_path.name] = 'не является .xlsx'
            return
        
        try:
            processor = self.processor_factory.get_processor(file_path, self.type_register)
            result, check = processor.process_file(file_path)
            
            self.storage_processed_registers[file_path.name] = result
            self.check[file_path.name] = check
        
        except Exception as e:
            self.not_correct_files[file_path.name] = str(e)

    def _process_directory(self, dir_path: Path) -> None:
        """Обработать директорию с файлами последовательно"""
        original_verbose = self.verbose
        self.verbose = False
        
        try:
            excel_files = self._get_excel_files(dir_path)
            result_collector = ResultCollector()
            result_collector.type_register = self.type_register
            
            # Последовательная обработка с tqdm
            for file_path in tqdm(excel_files, desc="Обработка файлов"):
                try:
                    processor, result, check = self._process_single_file_parallel(file_path, self.type_register)
                    if result is not None:
                        result_collector.add_result(processor, result, check)
                    else:
                        self.not_correct_files[file_path.name] = "Не удалось обработать файл"
                except Exception as e:
                    self.not_correct_files[file_path.name] = str(e)
            
            self._save_combined_results(result_collector)
            
        finally:
            self.verbose = original_verbose

    def _process_single_file_parallel(self, file_path: Path, type_register: str) -> Tuple[FileProcessor, pd.DataFrame, pd.DataFrame]:
        """Обработать файл последовательно (переименовано для совместимости)"""
        if not ExcelValidator.is_valid_excel(file_path):
            return None, None, None
        
        try:
            processor = FileProcessorFactory.get_processor(file_path, type_register)
            result, check = processor.process_file(file_path)
            return processor, result, check
        except Exception as e:
            raise e
    

     
    def print_processing_summary(self) -> None:
        """Вывести сводку по обработке файлов"""
        total_processed = len(self.storage_processed_registers) + len(self.check)
        total_errors = len(self.not_correct_files)
        
        print(f"\n{Fore.GREEN}Обработано файлов: {total_processed}")
        if total_errors > 0:
            print(f"{Fore.RED}Не обработано файлов: {total_errors}")
            print(f"{Fore.YELLOW}Список необработанных файлов и причин:")
            for filename, reason in self.not_correct_files.items():
                print(f"  {filename}: {reason}")
        else:
            print(f"{Fore.GREEN}Все файлы успешно обработаны!")

    @staticmethod
    def _get_excel_files(dir_path: Path) -> List[Path]:
        """Получить список Excel файлов в директории"""
        files = list(dir_path.glob("*.xlsx"))
        if not files:
            raise NoExcelFilesFoundError(Fore.RED + "В папке нет файлов Excel.")
        return files

    @Halo(text='Сохраняем и открываем файл...',
          text_color = 'blue',
          spinner={
                'interval': 500,
                'frames': ["⠋","⠙","⠹","⠸","⠼","⠴","⠦","⠧","⠇","⠏"]})
    def _save_combined_results(self, result_collector: ResultCollector) -> None:
        """Сохранить объединенные результаты"""
        results = result_collector.get_all_results()
        
        # Проверка наличия результатов
        if not any(not df.empty for df, _ in results.values()):
            raise NoRegisterFilesFoundError(Fore.RED + 'В папке не найдены регистры 1С.')
        
        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
            temp_filename = tmp.name
        
        # print('Сохраняем свод в файл...', end='\r')
        
        with pd.ExcelWriter(temp_filename, engine='openpyxl') as writer:
            sheet_mapping = {
                'upp': 'UPP',
                'non_upp': 'Non_UPP', 
                'analisys': ('analisys', 'analisys_check'),
                'turnover': ('turnover', 'turnover_check'),
                'accountosv': ('accountosv', 'accountosv_check'),
                'generalosv': ('generalosv', 'generalosv_check')
            }
            
            for key, sheets in sheet_mapping.items():
                df, df_check = results[key]
                if not df.empty:
                    write_df_in_chunks(writer, df, sheets[0] if isinstance(sheets, tuple) else sheets)
                if isinstance(sheets, tuple) and not df_check.empty:
                    write_df_in_chunks(writer, df_check, sheets[1])
        
        self._open_file(temp_filename)
        # print('Обработка завершена.' + ' ' * 20)

    @staticmethod
    def _open_file(file_path: str) -> None:
        """Открыть файл в ассоциированном приложении"""
        if sys.platform == "win32":
            os.startfile(file_path)
        elif sys.platform == "darwin":
            subprocess.run(["open", file_path], check=False)
        else:
            subprocess.run(["xdg-open", file_path], check=False)
    
    # @Halo(text='Сохраняем и открываем файл...', 
    #       spinner={
    #             'interval': 500,
    #             'frames': ["⠋","⠙","⠹","⠸","⠼","⠴","⠦","⠧","⠇","⠏"]})
    # def _save_and_open_batch_result(self) -> None:
    #     """Сохранить и открыть результаты пакетной обработки"""
    #     if not self.storage_processed_registers and not self.check:
    #         return
        
    #     # Ограничение на количество строк в Excel (для .xlsx формата)
    #     MAX_EXCEL_ROWS = 1048576
        
    #     # Проверка количества строк во всех датафреймах
    #     if self.storage_processed_registers:
    #         for sheet_name, df in self.storage_processed_registers.items():
    #             if len(df) > MAX_EXCEL_ROWS:
    #                 raise ValueError(
    #                     f"Лист '{sheet_name}' содержит {len(df)} строк, "
    #                     f"что превышает максимально допустимое количество "
    #                     f"в Excel ({MAX_EXCEL_ROWS} строк). "
    #                     # f"Пожалуйста, разбейте данные на несколько листов."
    #                 )
        
    #     if self.check:
    #         for sheet_name, df in self.check.items():
    #             if len(df) > MAX_EXCEL_ROWS:
    #                 raise ValueError(
    #                     f"Лист 'Проверка_{sheet_name}' содержит {len(df)} строк, "
    #                     f"что превышает максимально допустимое количество "
    #                     f"в Excel ({MAX_EXCEL_ROWS} строк). "
    #                     # f"Пожалуйста, разбейте данные на несколько листов."
    #                 )
        
    #     with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
    #         temp_filename = tmp.name
        
        # if self.verbose:
        #     print('Сохраняем файл...', end='\r')
        
        # Создаем книгу pyexcelerate
        # from pyexcelerate import Workbook
        # import numpy as np
        
        # wb = Workbook()
        
        # Функция для конвертации DataFrame в формат pyexcelerate
        # def _df_to_pyexcelerate_data(df):
        #     # Преобразуем заголовки
        #     # result = df.replace(np.nan, None)
        #     # data = [result.columns.tolist()]
            
        #     data = [df.columns.tolist()]
            
        #     # Преобразуем данные
        #     for _, row in df.iterrows():
        #         data_row = []
        #         for value in row:
        #             # Преобразуем None в пустые строки
        #             if pd.isna(value):
        #                 data_row.append('')
        #             else:
        #                 data_row.append(value)
        #         data.append(data_row)
            
        #     return data
        
        # Основные данные
        # for sheet_name, df in self.storage_processed_registers.items():
        #     safe_name = sheet_name[:31]
        #     data = _df_to_pyexcelerate_data(df)
        #     wb.new_sheet(safe_name, data=data)
        
        # Данные проверки
        # for sheet_name, df in self.check.items():
        #     safe_name = f"Проверка_{sheet_name}"[:31]
        #     data = _df_to_pyexcelerate_data(df)
        #     wb.new_sheet(safe_name, data=data)
        
        # Сохраняем файл
        # wb.save(temp_filename)
        
        # if self.verbose:
        #     print('Открываем файл...', end='\r')
        
        # self._open_file(temp_filename)
        
        # if self.verbose:
        #     print('Обработка завершена.' + ' ' * 20)
    
    @Halo(text='Сохраняем и открываем файл...',
          text_color = 'blue',
          spinner={
                'interval': 500,
                'frames': ["⠋","⠙","⠹","⠸","⠼","⠴","⠦","⠧","⠇","⠏"]})
    def _save_and_open_batch_result(self) -> None:
        """Сохранить и открыть результаты пакетной обработки"""
        if not self.storage_processed_registers and not self.check:
            return
            
        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
            temp_filename = tmp.name
        
        # if self.verbose:
        #     print('Сохраняем файл...', end='\r')
        
        with pd.ExcelWriter(temp_filename, engine='openpyxl') as writer:
            # Основные данные
            for sheet_name, df in self.storage_processed_registers.items():
                safe_name = sheet_name[:31]
                df.to_excel(writer, sheet_name=safe_name, index=False)
            
            # Данные проверки
            for sheet_name, df in self.check.items():
                safe_name = f"Проверка_{sheet_name}"[:31]
                df.to_excel(writer, sheet_name=safe_name, index=False)
        
        # if self.verbose:
        #     print('Открываем файл...', end='\r')
        
        self._open_file(temp_filename)
        
        # if self.verbose:
        #     print('Обработка завершена.' + ' ' * 20)

