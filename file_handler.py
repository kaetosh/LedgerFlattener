# -*- coding: utf-8 -*-
"""
Created on Mon Aug 25 10:44:30 2025

@author: a.karabedyan
"""

import sys, os, subprocess, tempfile
import pandas as pd


from pathlib import Path
from tqdm import tqdm
from colorama import init, Fore
from typing import List, Literal

from register_processors.class_processor import FileProcessor, DESIRED_ORDER

from register_processors.card_processor import (Card_UPPFileProcessor,
                                                Card_NonUPPFileProcessor)

from register_processors.posting_processor import (Posting_UPPFileProcessor,
                                                   Posting_NonUPPFileProcessor)

from support_functions import (fix_1c_excel_case,
                               sort_columns,
                               write_df_in_chunks)

from custom_errors import (RegisterProcessingError,
                           NoRegisterFilesFoundError,
                           NoExcelFilesFoundError)

init(autoreset=True)



class ExcelValidator:
    @staticmethod
    def is_valid_excel(file_path: Path) -> bool:
        return file_path.suffix.lower() == '.xlsx'



class FileProcessorFactory:
    """Фабрика для создания обработчиков файлов"""
    
    REGISTER_PATTERNS = {
        'card': [
            {
                'pattern': {'дата', 'документ', 'операция'},
                'processor': Card_UPPFileProcessor
            },
            {
                'pattern': {'период', 'аналитика дт', 'аналитика кт'},
                'processor': Card_NonUPPFileProcessor
            }
        ],
        'posting': [
            {
                'pattern': {'дата', 'документ', 'содержание', 'дт', 'кт', 'сумма'},
                'processor': Posting_UPPFileProcessor
            },
            {
                'pattern': {'период', 'аналитика дт', 'аналитика кт'},
                'processor': Posting_NonUPPFileProcessor
            }
        ]
    }

    @staticmethod
    def get_processor(file_path: Path, type_registr: Literal['posting', 'card']) -> FileProcessor:
        fixed_data = fix_1c_excel_case(file_path)
        df = pd.read_excel(fixed_data, header=None, nrows=50)
        
        # Преобразуем все данные в нижний регистр
        str_df = df.map(lambda x: str(x).strip().lower() if pd.notna(x) else '')
        
        for pattern_config in FileProcessorFactory.REGISTER_PATTERNS[type_registr]:
            for _, row in str_df.iterrows():
                row_set = set(row)
                if pattern_config['pattern'].issubset(row_set):
                    return pattern_config['processor']()
        
        raise RegisterProcessingError(f"Файл {file_path.name} не является корректным регистром {type_registr} из 1С.\n")

class FileHandler:
    def __init__(self, verbose: bool = True):
        self.validator = ExcelValidator()
        self.processor_factory = FileProcessorFactory()
        self.verbose = verbose
        self.not_correct_files = []
        self.storage_processed_registers = {}
    
    def handle_input(self, input_path: Path, type_registr: Literal['posting', 'card']) -> None:
        self.type_registr = type_registr
        if input_path.is_file():
            print('Принят файл...', end='\r')
            self._process_single_file(input_path)
        elif input_path.is_dir():
            print('Принята папка...', end='\r')
            self._process_directory(input_path)
    

    def _process_single_file(self, file_path: Path) -> None:
        if not self.validator.is_valid_excel(file_path):
            self.not_correct_files.append(file_path.name)
            return
        
        try:
            processor = self.processor_factory.get_processor(file_path, self.type_registr)
            if self.verbose:
                print('Файл в обработке...', end='\r')
            result = processor.process_file(file_path)
            self.storage_processed_registers[file_path.name] = result
        except RegisterProcessingError:
            self.not_correct_files.append(file_path.name)
    
    def _process_directory(self, dir_path: Path) -> None:
        original_verbose = self.verbose
        self.verbose = False
        
        try:
            excel_files = self._get_excel_files(dir_path)
            
            upp_results = []
            non_upp_results = []

            for file_path in tqdm(excel_files, leave=False, desc="Обработка файлов"):
                try:
                    
                    processor = self.processor_factory.get_processor(file_path, self.type_registr)
                    result = processor.process_file(file_path)

                    if isinstance(processor, (Card_UPPFileProcessor, Posting_UPPFileProcessor)):
                        upp_results.append(result)
                    else:
                        non_upp_results.append(result)
                except Exception:
                    self.not_correct_files.append(file_path.name)
            
            # Обработка результатов
            print('Упорядочиваем столбцы в своде...', end='\r')
            df_pivot_upp = sort_columns(
                pd.concat(upp_results), 
                DESIRED_ORDER[self.type_registr]['upp']
            ) if upp_results else pd.DataFrame()
            
            df_pivot_non_upp = sort_columns(
                pd.concat(non_upp_results), 
                DESIRED_ORDER[self.type_registr]['not_upp']
            ) if non_upp_results else pd.DataFrame()
            
            if not upp_results and not non_upp_results:
                raise NoRegisterFilesFoundError(Fore.RED + 'В папке не найдены регистры 1С.')
            
            self._save_combined_results(df_pivot_upp, df_pivot_non_upp)
        finally:
            self.verbose = original_verbose
    
    @staticmethod
    def _get_excel_files(dir_path: Path) -> List[Path]:
        files = [f for f in dir_path.iterdir() if f.is_file() and f.suffix.lower() == '.xlsx']
        if not files:
            raise NoExcelFilesFoundError(Fore.RED + "В папке нет файлов Excel.")
        return files

    def _save_and_open_batch_result(self) -> None:
        if not self.storage_processed_registers:
            return
            
        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
            temp_filename = tmp.name
        
        if self.verbose:
            print('Сохраняем файл...   ', end='\r')
        with pd.ExcelWriter(temp_filename, engine='openpyxl') as writer:
            for sheet_name, df in self.storage_processed_registers.items():
                safe_name = sheet_name[:31]
                df.to_excel(writer, sheet_name=safe_name, index=False)
        if self.verbose:
            print('Открываем файл...   ', end='\r')
        if sys.platform == "win32":
            os.startfile(temp_filename)
        elif sys.platform == "darwin":
            subprocess.run(["open", temp_filename])
        else:
            subprocess.run(["xdg-open", temp_filename])
        if self.verbose:
            print('Обработка завершена.   ')

    @staticmethod
    def _save_combined_results(df_upp: pd.DataFrame, df_non_upp: pd.DataFrame) -> None:
        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
            temp_filename = tmp.name
        print('Сохраняем свод в файл...         ', end='\r')
        with pd.ExcelWriter(temp_filename, engine='openpyxl') as writer:
            if not df_upp.empty:
                write_df_in_chunks(writer, df_upp, 'UPP')
            if not df_non_upp.empty:
                write_df_in_chunks(writer, df_non_upp, 'Non_UPP')
        print('Открываем сводный файл...          ', end='\r')
        if sys.platform == "win32":
            os.startfile(temp_filename)
        elif sys.platform == "darwin":
            subprocess.run(["open", temp_filename])
        else:
            subprocess.run(["xdg-open", temp_filename])
        print('Обработка завершена.               ')
    
