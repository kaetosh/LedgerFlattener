# -*- coding: utf-8 -*-
"""
Created on Mon Aug 25 10:44:30 2025

@author: a.karabedyan
"""

# import sys, os, subprocess, tempfile
# import pandas as pd

# from tqdm import tqdm
# from multiprocessing import Pool

# import time


# pd.set_option('display.max_columns', None)  # Установите None, чтобы отображать все столбцы
# pd.set_option('display.expand_frame_repr', False)  # Чтобы не разбивать длинные DataFrame на несколько строк

# from pathlib import Path
# from colorama import init, Fore
# from typing import List, Literal

# from register_processors.class_processor import FileProcessor, DESIRED_ORDER

# from register_processors.card_processor import (Card_UPPFileProcessor,
#                                                 Card_NonUPPFileProcessor)

# from register_processors.posting_processor import (Posting_UPPFileProcessor,
#                                                    Posting_NonUPPFileProcessor)

# from register_processors.analisys_processor import (Analisys_UPPFileProcessor,
#                                                    Analisys_NonUPPFileProcessor)

# from register_processors.turnover_processor import (Turnover_UPPFileProcessor,
#                                                    Turnover_NonUPPFileProcessor)

# from register_processors.accountosv_processor import (AccountOSV_UPPFileProcessor,
#                                                     AccountOSV_NonUPPFileProcessor)

# from register_processors.generalosv_processor import (GeneralOSV_UPPFileProcessor,
#                                                     GeneralOSV_NonUPPFileProcessor)

# from support_functions import (fix_1c_excel_case,
#                                sort_columns,
#                                write_df_in_chunks)

# from custom_errors import (RegisterProcessingError,
#                            NoRegisterFilesFoundError,
#                            NoExcelFilesFoundError)

# init(autoreset=True)



# class ExcelValidator:
#     @staticmethod
#     def is_valid_excel(file_path: Path) -> bool:
#         return file_path.suffix.lower() == '.xlsx'



# class FileProcessorFactory:
#     """Фабрика для создания обработчиков файлов"""
    
#     REGISTER_DISPLAY_NAMES = {
#         'posting': 'Отчет по проводкам',
#         'card': 'Карточка счета',
#         'analisys': 'Анализ счета',
#         'turnover': 'Обороты счета',
#         'accountosv': 'ОСВ счета',
#         'generalosv': 'общая ОСВ'
#     }
    
#     REGISTER_PATTERNS = {
#         'card': [
#             {
#                 'pattern': {'дата', 'документ', 'операция'},
#                 'processor': Card_UPPFileProcessor
#             },
#             {
#                 'pattern': {'период', 'аналитика дт', 'аналитика кт'},
#                 'processor': Card_NonUPPFileProcessor
#             }
#         ],
#         'posting': [
#             {
#                 'pattern': {'дата', 'документ', 'содержание', 'дт', 'кт', 'сумма'},
#                 'processor': Posting_UPPFileProcessor
#             },
#             {
#                 'pattern': {'период', 'аналитика дт', 'аналитика кт'},
#                 'processor': Posting_NonUPPFileProcessor
#             }
#         ],
#         'analisys': [
#             {
#                 'pattern': {'счет', 'кор.счет', 'с кред. счетов', 'в дебет счетов'},
#                 'processor': Analisys_UPPFileProcessor
#                 },
#             {
#                 'pattern': {'счет', 'кор. счет', 'дебет', 'кредит'},
#                 'processor': Analisys_NonUPPFileProcessor
#                 }
#             ],
#         'turnover': [
#             {
#                 'pattern': {'субконто', 'нач. сальдо деб.', 'нач. сальдо кред.', 'деб. оборот', 'кред. оборот', 'кон. сальдо деб.', 'кон. сальдо кред.'},
#                 'processor': Turnover_UPPFileProcessor
#                 },
#             {
#                 'pattern': {'счет', 'начальное сальдо дт', 'начальное сальдо кт', 'оборот дт', 'оборот кт', 'конечное сальдо дт', 'конечное сальдо кт'},
#                 'processor': Turnover_NonUPPFileProcessor
#                 }
#             ],
#         'accountosv': [
#             {
#                 'pattern': {'субконто', 'сальдо на начало периода', 'оборот за период', 'сальдо на конец периода'},
#                 'processor': AccountOSV_UPPFileProcessor
#                 },
#             {
#                 'pattern': {'счет', 'сальдо на начало периода', 'обороты за период', 'сальдо на конец периода'},
#                 'processor': AccountOSV_NonUPPFileProcessor
#                 }
#             ],
#         'generalosv': [
#             {
#                 'pattern': {'счет', 'сальдо на начало периода', 'оборот за период', 'сальдо на конец периода'},
#                 'processor': GeneralOSV_UPPFileProcessor
#                 },
#             {
#                 'pattern': {'счет', 'наименование счета', 'сальдо на начало периода', 'обороты за период', 'сальдо на конец периода'},
#                 'processor': GeneralOSV_NonUPPFileProcessor
#                 }
#             ]
#     }

#     @staticmethod
#     def get_processor(file_path: Path, type_registr: Literal['posting', 'card', 'analisys', 'turnovers', 'accountosv', 'generalosv']) -> FileProcessor:
        
#         fixed_data = fix_1c_excel_case(file_path)
        
#         df = pd.read_excel(fixed_data, header=None, nrows=20)
        
        
        
#         # Преобразуем все данные в нижний регистр
#         str_df = df.map(lambda x: str(x).strip().lower() if pd.notna(x) else '')
        
        
#         for pattern_config in FileProcessorFactory.REGISTER_PATTERNS[type_registr]:
#             for _, row in str_df.iterrows():
#                 row_set = set(row)
#                 if pattern_config['pattern'].issubset(row_set):
#                     return pattern_config['processor']()
        
#         raise RegisterProcessingError("Файл не является корректным регистром из 1С.")

# class FileHandler:
#     def __init__(self, verbose: bool = True):
#         self.validator = ExcelValidator()
#         self.processor_factory = FileProcessorFactory()
#         self.verbose = verbose
#         self.not_correct_files = {}
#         self.storage_processed_registers = {}
#         self.check = {}
    
#     def handle_input(self, input_path: Path, type_registr: Literal['posting', 'card', 'analisys', 'account', 'generalosv']) -> None:
#         self.type_registr = type_registr
#         if input_path.is_file():
#             print('Принят файл...', end='\r')
#             self._process_single_file(input_path)
#         elif input_path.is_dir():
#             print('Принята папка...', end='\r')
#             self._process_directory(input_path)
    
    
    

    
#     # Функция-обёртка для multiprocessing (должна быть на уровне модуля для сериализации)
#     def _process_wrapper(self, file_path: Path, processor_factory, type_registr):
#         """
#         Обёртка для запуска процессинга в отдельном процессе.
#         processor_factory и type_registr должны быть сериализуемыми.
#         """
#         processor = processor_factory.get_processor(file_path, type_registr)
#         return processor.process_file(file_path)
    
#     def _process_single_file(self, file_path: Path) -> None:
#         if not self.validator.is_valid_excel(file_path):
#             self.not_correct_files[file_path.name] = 'не является .xlsx'
#             return
        
#         try:
#             # Инициализируем tqdm как спиннер с описанием
#             with tqdm(total=None, desc="Файл в обработке", ascii=True, bar_format='{desc}: {elapsed}') as pbar:
#                 with Pool(processes=1) as pool:
#                     async_result = pool.apply_async(self._process_wrapper, (file_path, self.processor_factory, self.type_registr))
                    
#                     # Цикл ожидания: обновляем спиннер каждые 0.1 сек, чтобы анимировать
#                     while not async_result.ready():
#                         pbar.update()  # Обновляет анимацию (символы | / - \)
#                         time.sleep(0.1)  # Небольшая задержка для плавности
                    
#                     result, check = async_result.get()
            
#             # После завершения: tqdm автоматически закроется, выведем успех
#             # tqdm.write("Обработка завершена!")  # Или используйте tqdm.write() для сообщения
#             self.storage_processed_registers[file_path.name] = result
#             self.check[file_path.name] = check
        
#         except RegisterProcessingError as error_description:
#             self.not_correct_files[file_path.name] = error_description
#         except Exception as e:
#             # import traceback
#             # traceback.print_exc()
#             print(f"Неучтенная ошибка: {e}")



#     # def _process_single_file(self, file_path: Path) -> None:
#     #     if not self.validator.is_valid_excel(file_path):
#     #         self.not_correct_files[file_path.name] = 'не является .xlsx'
#     #         return
#     #     try:
            
#     #         processor = self.processor_factory.get_processor(file_path, self.type_registr)
#     #         if self.verbose:
#     #             print('Файл в обработке...', end='\r')
#     #         result, check = processor.process_file(file_path)
#     #         self.storage_processed_registers[file_path.name] = result
#     #         self.check[file_path.name] = check
#     #     except RegisterProcessingError as error_description:
#     #         self.not_correct_files[file_path.name] = error_description
    
#     def _process_directory(self, dir_path: Path) -> None:
#         original_verbose = self.verbose
#         self.verbose = False
        
#         try:
#             excel_files = self._get_excel_files(dir_path)
            
#             upp_results = [] # для карточки и отчета проводкам
#             non_upp_results = [] # для карточки и отчета проводкам
#             analisys_result = [] # для анализов (и для УПП и неУПП)
#             analisys_result_check = [] # для результатов сверки до и после обработки анализов (и для УПП и неУПП)
#             turnover_result = [] # для оборотов (и для УПП и неУПП)
#             turnover_result_check = [] # для результатов сверки до и после обработки оборотов (и для УПП и неУПП)
#             accountosv_result = [] # для осв счета (и для УПП и неУПП)
#             accountosv_result_check = [] # для результатов сверки до и после обработки осв счета (и для УПП и неУПП)
#             generalosv_result = [] # для осв общей (и для УПП и неУПП)
#             generalosv_result_check = [] # для результатов сверки до и после обработки осв общей (и для УПП и неУПП)

#             for file_path in tqdm(excel_files, leave=False, desc="Обработка файлов"):
#                 try:
                    
#                     processor = self.processor_factory.get_processor(file_path, self.type_registr)
#                     result, check = processor.process_file(file_path)

#                     if isinstance(processor, (Card_UPPFileProcessor,
#                                               Posting_UPPFileProcessor,
#                                               )):
#                         upp_results.append(result)
#                     elif isinstance(processor, (Analisys_UPPFileProcessor,
#                                                 Analisys_NonUPPFileProcessor)):
#                         analisys_result.append(result)
#                         analisys_result_check.append(check)
#                     elif isinstance(processor, (Turnover_UPPFileProcessor,
#                                                 Turnover_NonUPPFileProcessor)):
#                         turnover_result.append(result)
#                         turnover_result_check.append(check)
#                     elif isinstance(processor, (AccountOSV_UPPFileProcessor,
#                                                 AccountOSV_NonUPPFileProcessor)):
#                         accountosv_result.append(result)
#                         accountosv_result_check.append(check)
#                     elif isinstance(processor, (GeneralOSV_UPPFileProcessor,
#                                                 GeneralOSV_NonUPPFileProcessor)):
#                         generalosv_result.append(result)
#                         generalosv_result_check.append(check)
#                     else:
#                         non_upp_results.append(result)
#                 except Exception as error_description:
#                     self.not_correct_files[file_path.name] = error_description
            
#             # Обработка результатов
#             print('Упорядочиваем столбцы в своде...', end='\r')
#             df_pivot_upp = sort_columns(
#                 pd.concat(upp_results), 
#                 DESIRED_ORDER[self.type_registr]['upp']
#             ) if upp_results else pd.DataFrame()
            
#             df_pivot_non_upp = sort_columns(
#                 pd.concat(non_upp_results), 
#                 DESIRED_ORDER[self.type_registr]['not_upp']
#             ) if non_upp_results else pd.DataFrame()
            
#             df_pivot_analisys = sort_columns(
#                 pd.concat(analisys_result), 
#                 DESIRED_ORDER[self.type_registr]['upp']
#             ) if analisys_result else pd.DataFrame()
#             df_pivot_analisys_check = pd.concat(analisys_result_check) if analisys_result_check else pd.DataFrame()
            
#             df_pivot_analisys = processor.shiftable_level(df_pivot_analisys)
            
#             df_pivot_turnover = sort_columns(
#                 pd.concat(turnover_result), 
#                 DESIRED_ORDER[self.type_registr]['upp']
#             ) if turnover_result else pd.DataFrame()
#             df_pivot_turnover_check = pd.concat(turnover_result_check) if turnover_result_check else pd.DataFrame()
            
#             df_pivot_turnover = processor.shiftable_level(df_pivot_turnover)
            
#             df_pivot_accountosv = sort_columns(
#                 pd.concat(accountosv_result), 
#                 DESIRED_ORDER[self.type_registr]['upp']
#             ) if accountosv_result else pd.DataFrame()
#             df_pivot_accountosv_check = pd.concat(accountosv_result_check) if accountosv_result_check else pd.DataFrame()
            
#             df_pivot_accountosv = processor.shiftable_level(df_pivot_accountosv)
            
#             df_pivot_generalosv = sort_columns(
#                 pd.concat(generalosv_result), 
#                 DESIRED_ORDER[self.type_registr]['upp']
#             ) if generalosv_result else pd.DataFrame()
#             df_pivot_generalosv_check = pd.concat(generalosv_result_check) if generalosv_result_check else pd.DataFrame()
            
#             df_pivot_generalosv = processor.shiftable_level(df_pivot_generalosv)
            
#             if (not upp_results 
#                 and not non_upp_results
#                 and not analisys_result
#                 and not turnover_result
#                 and not accountosv_result
#                 and not generalosv_result):
#                 raise NoRegisterFilesFoundError(Fore.RED + 'В папке не найдены регистры 1С.')
            
#             self._save_combined_results(df_pivot_upp,
#                                         df_pivot_non_upp,
#                                         df_pivot_analisys,
#                                         df_pivot_analisys_check,
#                                         df_pivot_turnover,
#                                         df_pivot_turnover_check,
#                                         df_pivot_accountosv,
#                                         df_pivot_accountosv_check,
#                                         df_pivot_generalosv,
#                                         df_pivot_generalosv_check,)
#         finally:
#             self.verbose = original_verbose
    
#     @staticmethod
#     def _get_excel_files(dir_path: Path) -> List[Path]:
#         files = [f for f in dir_path.iterdir() if f.is_file() and f.suffix.lower() == '.xlsx']
#         if not files:
#             raise NoExcelFilesFoundError(Fore.RED + "В папке нет файлов Excel.")
#         return files

#     def _save_and_open_batch_result(self) -> None:
#         if not self.storage_processed_registers and not (hasattr(self, 'check') and self.check):
#             return
            
#         with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
#             temp_filename = tmp.name
        
#         if self.verbose:
#             print('Сохраняем файл...   ', end='\r')
        
#         with pd.ExcelWriter(temp_filename, engine='openpyxl') as writer:
#             # Сохраняем данные из storage_processed_registers
#             if self.storage_processed_registers:
#                 for sheet_name, df in self.storage_processed_registers.items():
#                     safe_name = sheet_name[:31]
#                     df.to_excel(writer, sheet_name=safe_name, index=False)
            
#             # Сохраняем данные из self.check
#             if hasattr(self, 'check') and self.check:
#                 for sheet_name, df in self.check.items():
#                     # Добавляем префикс "Проверка_" для листов из check
#                     safe_name = f"Проверка_{sheet_name}"[:31]
#                     df.to_excel(writer, sheet_name=safe_name, index=False)
        
#         if self.verbose:
#             print('Открываем файл...   ', end='\r')
        
#         if sys.platform == "win32":
#             os.startfile(temp_filename)
#         elif sys.platform == "darwin":
#             subprocess.run(["open", temp_filename])
#         else:
#             subprocess.run(["xdg-open", temp_filename])
        
#         if self.verbose:
#             print('Обработка завершена.   ')


#     @staticmethod
#     def _save_combined_results(df_upp: pd.DataFrame,
#                                df_non_upp: pd.DataFrame,
#                                df_analisys: pd.DataFrame,
#                                df_analisys_check: pd.DataFrame,
#                                df_turnover: pd.DataFrame,
#                                df_turnover_check: pd.DataFrame,
#                                df_accountosv: pd.DataFrame,
#                                df_accountosv_check: pd.DataFrame,
#                                df_generalosv: pd.DataFrame,
#                                df_generalosv_check: pd.DataFrame) -> None:
#         with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
#             temp_filename = tmp.name
#         print('Сохраняем свод в файл...         ', end='\r')
#         with pd.ExcelWriter(temp_filename, engine='openpyxl') as writer:
#             if not df_upp.empty:
#                 write_df_in_chunks(writer, df_upp, 'UPP')
#             if not df_non_upp.empty:
#                 write_df_in_chunks(writer, df_non_upp, 'Non_UPP')
#             if not df_analisys.empty:
#                 write_df_in_chunks(writer, df_analisys, 'analisys')
#             if not df_analisys_check.empty:
#                 write_df_in_chunks(writer, df_analisys_check, 'analisys_check')
#             if not df_turnover.empty:
#                 write_df_in_chunks(writer, df_turnover, 'turnover')
#             if not df_turnover_check.empty:
#                 write_df_in_chunks(writer, df_turnover_check, 'turnover_check')
#             if not df_accountosv.empty:
#                 write_df_in_chunks(writer, df_accountosv, 'accountosv')
#             if not df_accountosv_check.empty:
#                 write_df_in_chunks(writer, df_accountosv_check, 'accountosv_check')
#             if not df_generalosv.empty:
#                 write_df_in_chunks(writer, df_generalosv, 'generalosv')
#             if not df_generalosv_check.empty:
#                 write_df_in_chunks(writer, df_generalosv_check, 'generalosv_check')
#         print('Открываем сводный файл...          ', end='\r')
#         if sys.platform == "win32":
#             os.startfile(temp_filename)
#         elif sys.platform == "darwin":
#             subprocess.run(["open", temp_filename])
#         else:
#             subprocess.run(["xdg-open", temp_filename])
#         print('Обработка завершена.               ')

import sys
import os
import subprocess
import tempfile
import time
from pathlib import Path
from typing import List, Literal, Dict, Tuple, Optional
from multiprocessing import Pool
from concurrent.futures import ProcessPoolExecutor, as_completed

import pandas as pd
from tqdm import tqdm
from colorama import init, Fore

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
        self.analisys_checks = []
        self.turnover_results = []
        self.turnover_checks = []
        self.accountosv_results = []
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
        elif processor_type in (Turnover_UPPFileProcessor, Turnover_NonUPPFileProcessor):
            self.turnover_results.append(result)
            self.turnover_checks.append(check)
        elif processor_type in (AccountOSV_UPPFileProcessor, AccountOSV_NonUPPFileProcessor):
            self.accountosv_results.append(result)
            self.accountosv_checks.append(check)
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
            'analisys': (self._process_analisys_like(self.analisys_results), 
                        pd.concat(self.analisys_checks) if self.analisys_checks else pd.DataFrame()),
            'turnover': (self._process_analisys_like(self.turnover_results),
                        pd.concat(self.turnover_checks) if self.turnover_checks else pd.DataFrame()),
            'accountosv': (self._process_analisys_like(self.accountosv_results),
                          pd.concat(self.accountosv_checks) if self.accountosv_checks else pd.DataFrame()),
            'generalosv': (self._process_analisys_like(self.generalosv_results),
                          pd.concat(self.generalosv_checks) if self.generalosv_checks else pd.DataFrame())
        }
    
    def _concat_and_sort(self, results: List[pd.DataFrame], processor_type: str) -> pd.DataFrame:
        """Объединить и отсортировать результаты"""
        if not results:
            return pd.DataFrame()
        return sort_columns(pd.concat(results), DESIRED_ORDER[self.type_register][processor_type])
    
    def _process_analisys_like(self, results: List[pd.DataFrame]) -> pd.DataFrame:
        """Обработать результаты анализов и подобных регистров"""
        if not results:
            return pd.DataFrame()
        df = sort_columns(pd.concat(results), DESIRED_ORDER[self.type_register]['upp'])
        # Предполагаем, что у всех процессоров есть метод shiftable_level
        return results[0].shiftable_level(df) if hasattr(results[0], 'shiftable_level') else df


class FileHandler:
    """Обработчик файлов с поддержкой многопроцессорности"""
    
    def __init__(self, verbose: bool = True, max_workers: Optional[int] = None):
        self.validator = ExcelValidator()
        self.processor_factory = FileProcessorFactory()
        self.verbose = verbose
        self.max_workers = max_workers or os.cpu_count()
        
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

    def _process_single_file(self, file_path: Path) -> None:
        """Обработать одиночный файл со спиннером"""
        if not self.validator.is_valid_excel(file_path):
            self.not_correct_files[file_path.name] = 'не является .xlsx'
            return
        
        try:
            # Используем tqdm как спиннер с отображением времени
            with tqdm(total=None, desc="Файл в обработке", ascii=True, 
                     bar_format='{desc}: {elapsed}') as pbar:
                with Pool(processes=1) as pool:
                    # Запускаем обработку в отдельном процессе
                    async_result = pool.apply_async(self._process_wrapper, 
                                                  (file_path, self.type_register))
                    
                    # Цикл ожидания: обновляем спиннер пока процесс не завершится
                    while not async_result.ready():
                        pbar.update()  # Обновляет время в описании
                        time.sleep(0.1)  # Небольшая задержка для плавности
                    
                    result, check = async_result.get()
            
            self.storage_processed_registers[file_path.name] = result
            self.check[file_path.name] = check
        
        except Exception as e:
            self.not_correct_files[file_path.name] = str(e)

    @staticmethod
    def _process_wrapper(file_path: Path, type_register: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Обертка для запуска в отдельном процессе"""
        processor = FileProcessorFactory.get_processor(file_path, type_register)
        return processor.process_file(file_path)


    def _process_directory(self, dir_path: Path) -> None:
        """Обработать директорию с файлами"""
        original_verbose = self.verbose
        self.verbose = False
        
        try:
            excel_files = self._get_excel_files(dir_path)
            result_collector = ResultCollector()
            result_collector.type_register = self.type_register
            
            # Многопроцессорная обработка с сбором ошибок
            with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
                future_to_file = {
                    executor.submit(self._process_single_file_parallel, file_path, self.type_register): file_path 
                    for file_path in excel_files
                }
                
                for future in tqdm(as_completed(future_to_file), total=len(excel_files), 
                                 desc="Обработка файлов"):
                    file_path = future_to_file[future]
                    try:
                        processor, result, check = future.result()
                        if result is not None:
                            result_collector.add_result(processor, result, check)
                        else:
                            # Если результат None, значит файл не был обработан
                            self.not_correct_files[file_path.name] = "Не удалось обработать файл"
                    except Exception as e:
                        self.not_correct_files[file_path.name] = str(e)
            
            self._save_combined_results(result_collector)
            
        finally:
            self.verbose = original_verbose

    @staticmethod
    def _process_single_file_parallel(file_path: Path, type_register: str) -> Tuple[FileProcessor, pd.DataFrame, pd.DataFrame]:
        """Обработать файл в параллельном процессе"""
        if not ExcelValidator.is_valid_excel(file_path):
            # Возвращаем None для невалидных файлов
            return None, None, None
        
        try:
            processor = FileProcessorFactory.get_processor(file_path, type_register)
            result, check = processor.process_file(file_path)
            return processor, result, check
        except Exception as e:
            # Пробрасываем исключение в основной процесс
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

    def _save_combined_results(self, result_collector: ResultCollector) -> None:
        """Сохранить объединенные результаты"""
        results = result_collector.get_all_results()
        
        # Проверка наличия результатов
        if not any(not df.empty for df, _ in results.values()):
            raise NoRegisterFilesFoundError(Fore.RED + 'В папке не найдены регистры 1С.')
        
        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
            temp_filename = tmp.name
        
        print('Сохраняем свод в файл...', end='\r')
        
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
        print('Обработка завершена.' + ' ' * 20)

    @staticmethod
    def _open_file(file_path: str) -> None:
        """Открыть файл в ассоциированном приложении"""
        if sys.platform == "win32":
            os.startfile(file_path)
        elif sys.platform == "darwin":
            subprocess.run(["open", file_path], check=False)
        else:
            subprocess.run(["xdg-open", file_path], check=False)

    def _save_and_open_batch_result(self) -> None:
        """Сохранить и открыть результаты пакетной обработки"""
        if not self.storage_processed_registers and not self.check:
            return
            
        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
            temp_filename = tmp.name
        
        if self.verbose:
            print('Сохраняем файл...', end='\r')
        
        with pd.ExcelWriter(temp_filename, engine='openpyxl') as writer:
            # Основные данные
            for sheet_name, df in self.storage_processed_registers.items():
                safe_name = sheet_name[:31]
                df.to_excel(writer, sheet_name=safe_name, index=False)
            
            # Данные проверки
            for sheet_name, df in self.check.items():
                safe_name = f"Проверка_{sheet_name}"[:31]
                df.to_excel(writer, sheet_name=safe_name, index=False)
        
        if self.verbose:
            print('Открываем файл...', end='\r')
        
        self._open_file(temp_filename)
        
        if self.verbose:
            print('Обработка завершена.' + ' ' * 20)