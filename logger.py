import abc
import datetime
import logging
import os
import pathlib

from config import AppConfig


class Logger(abc.ABC):
    """
    Базовый класс для логирования
    """

    @property
    @abc.abstractmethod
    def logger_name(self):
        """
        Свойство, возвращает имя для логирования
        """
        pass

    @property
    @abc.abstractmethod
    def log_folder(self):
        """
        Свойство, возвращает каталог для логирования
        """
        pass

    @property
    def log_file(self):
        """
        Свойство, возвращает наименование файла для логирования
        """
        return f"{datetime.datetime.now().strftime('%Y_%m_%d__%H_%M_%S')}.log"

    file_format = '%(asctime)-5s %(name)-15s %(levelname)-8s %(message)s'
    console_format = '%(asctime)-5s %(name)-15s %(levelname)-8s %(message)s'
    date_format = '%d-%m-%t %H:%M'

    log_to_console = True
    log_to_file = True

    def __init__(self):
        """
        Инициализация логирования
        """
        self._prepare_logger()

    def _prepare_logger(self):
        """
        Запрос экземпляра через фабрику и настройка
        Добавление обработчиков
        """
        self._logger = logging.getLogger(self.logger_name)
        self._logger.setLevel(logging.DEBUG)

        if self.log_to_console:
            self._logger.addHandler(self._console_handler())

        if self.log_to_file:
            self._logger.addHandler(self._file_handler())

    def _console_handler(self):
        """
        Коннфигурирование обработчика для консоли
        """
        handler = logging.StreamHandler()
        handler.setLevel(logging.INFO)
        handler.setFormatter(
            logging.Formatter(self.console_format)
        )
        return handler

    def _file_handler(self):
        """
        Коннфигурирование обработчика для файлов
        """
        self._create_folder()
        _file = self.log_folder + os.sep + self.log_file
        handler = logging.FileHandler(filename=_file)
        handler.setLevel(logging.DEBUG)
        handler.setFormatter(
            logging.Formatter(self.file_format)
        )
        return handler

    def _create_folder(self):
        """
        Создание каталога для логирования при необходимости
        """
        pathlib.Path(self.log_folder).mkdir(parents=True, exist_ok=True)

    def clean(self):
        """
        Сброс логирования
        Необходимо при перезапуске расчета
        """
        self._logger.handlers.clear()
        if logging.Logger.manager.loggerDict.get(self.logger_name, False):
            del logging.Logger.manager.loggerDict[self.logger_name]

    def debug(self, *args, **kwargs):
        """
        Логирование уровня debug
        """
        self._logger.debug(*args, **kwargs)

    def info(self, *args, **kwargs):
        """
        Логирование уровня info
        """
        self._logger.info(*args, **kwargs)

    def warning(self, *args, **kwargs):
        """
        Логирование уровня warning
        """
        self._logger.warning(*args, **kwargs)

    def error(self, *args, **kwargs):
        """
        Логирование уровня error
        """
        self._logger.error(*args, **kwargs)


class ManagerLogger(Logger):
    """
    Обеспечивает логирование на уровне менеджера
    """

    @property
    def logger_name(self):
        """
        Свойство, возвращает имя для логирования
        """
        return 'manage_log'

    @property
    def log_folder(self):
        """
        Свойство, возвращает каталог для логирования
        """
        return AppConfig.get_log_dir() + os.sep + 'log' + os.sep + 'manager'


class ParserLogger(Logger):
    """
    Обеспечивает логирование на уровне парсера
    """

    @property
    def logger_name(self):
        """
        Свойство, возвращает имя для логирования
        """
        return 'parser_log'

    @property
    def log_folder(self):
        """
        Свойство, возвращает каталог для логирования
        """
        return AppConfig.get_log_dir() + os.sep + 'log' + os.sep + 'parser'

