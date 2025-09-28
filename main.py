import questionary
from questionary import Style as QuestionaryStyle
from colorama import init, Fore, Style
from typing import List
from pathlib import Path
import shlex
import pandas as pd

from file_handler import FileHandler
from support_functions import (print_instruction_color,
                               validate_paths,
                               clear_console,
                               print_start_text)
pd.set_option('display.max_columns', None)  # Показывать все столбцы
pd.set_option('display.expand_frame_repr', False)  # Не переносить на несколько строк
pd.set_option('display.width', None)  # Не ограничивать ширину вывода
pd.set_option('display.max_colwidth', None)  # Показывать полное содержимое ячеек

# Инициализация colorama для цветного вывода
init(autoreset=True)

class UserInterface:
    @staticmethod
    def get_input() -> List[Path]:
        print(Fore.YELLOW + "\nПеретащите файл регистра (.xlsx) или папку, нажмите Enter или ctrl+c для выхода:")
        input_str = input().strip().replace('\\', '/')
        paths = [Path(p) for p in shlex.split(input_str)]
        return validate_paths(paths)

def main():

    ui = UserInterface()
    # file_handler = FileHandler(max_workers=1)
    file_handler = FileHandler()
    
    # Создаем кастомный стиль
    custom_style = QuestionaryStyle([
        ('qmark', 'fg:#FF9D00 bold'),       # Символ вопроса
        ('question', 'fg:#FF9D00 bold'),    # Текст вопроса
        ('answer', 'fg:#5F87FF bold'),      # Выбранный ответ
        ('pointer', 'fg:#5F87FF bold'),     # Указатель (>>)
        ('highlighted', 'fg:#5F87FF bold'), # Подсвеченный элемент
        ('selected', 'fg:#5F87FF'),         # Выбранный элемент
        ('separator', 'fg:#6C6C6C'),        # Разделитель
        ('instruction', 'fg:#6C6C6C'),      # Инструкция
        ('text', 'fg:#FFFFFF'),             # Текст вариантов
    ])
    while True:
        clear_console()
        print_start_text()
        # Выбор типа обработки

        choice = questionary.select(
            "Выберите вид регистра для обработки:",
            choices=[
                {"name": "1. Отчет по проводкам", "value": "posting"},
                {"name": "2. Карточка счета", "value": "card"},
                {"name": "3. Анализ счета", "value": "analisys"},
                {"name": "4. Обороты счета", "value": "turnover"},
                {"name": "5. ОСВ счета", "value": "accountosv"},
                {"name": "6. ОСВ общая", "value": "generalosv"},
                {"name": "   Выход", "value": "exit"}
            ],
            style=custom_style,  # Применяем кастомный стиль
            instruction = '/используйте клавиши со стрелками вверх или вниз и нажмите Enter/',
            pointer = '=>',
            qmark = '',
            # default = "exit"
        ).ask()
        
        if choice == "exit" or choice is None:
            break
        
        try:
            # Выбор файла
            print_instruction_color(choice)
            
            input_paths = ui.get_input()
            for input_path in input_paths:
                try:
                    file_handler.handle_input(input_path, choice)
                except Exception as error_description:
                    # import traceback
                    # traceback.print_exc()
                    print(f"{error_description}  ")
                    if input_path.is_file():
                        # file_handler.not_correct_files.append(input_path.name)
                        file_handler.not_correct_files[input_path.name] = error_description
            
            if file_handler.storage_processed_registers:
                file_handler._save_and_open_batch_result()
            
        except KeyboardInterrupt:
            # break
            continue
        except Exception as e:
            # import traceback
            # traceback.print_exc()
            print(f"{e}")
        finally:
            # Вывод информации о неправильных файлах
            # file_handler.print_processing_summary()
            if file_handler.not_correct_files:
                print(Fore.RED + 'Файлы не распознаны как регистры 1С или возникли ошибки:')
                for file_name in file_handler.not_correct_files:
                    print(Fore.RED + f"  - {file_name} - {file_handler.not_correct_files[file_name]}")
                file_handler.not_correct_files.clear()
            
            # Очистка хранилища
            if file_handler.storage_processed_registers:
                file_handler.storage_processed_registers.clear()
            
            if file_handler.check:
                file_handler.check.clear()
        
        # Предложение продолжить
        continue_choice = questionary.select(
            "Хотите обработать еще? Вернуться к выбору регистра ctrl+c",
            choices=[
                {"name": "Да", "value": "yes"},
                {"name": "Нет", "value": "no"}
            ],
            style=custom_style,  # Применяем кастомный стиль
            qmark = '',
            instruction = '/используйте клавиши со стрелками вверх или вниз и нажмите Enter/',
            pointer = '=>'
        ).ask()
        
        if continue_choice == "no":
            print(f"{Fore.GREEN}До свидания!{Style.RESET_ALL}")
            break

if __name__ == "__main__":
    main()
    # The End