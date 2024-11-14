import os
import sys


def get_run_prefix() -> str:
    """
    Получает префикс для подключения исходя из переменной окружения.
    """
    if os.environ.get('PRODUCTION'):
        return 'prod'
    elif os.environ.get('DEVELOPMENT'):
        return 'dev'
    else:
        return 'local'


class AppConfig:
    """
    Хранилище переменных конфигурации приложения
    """

    # Корневая директория приложения
    __ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

    # Директория с логами
    __LOG_DIR = os.path.dirname(os.path.abspath(sys.modules['__main__'].__name__))

    # Префикс для подключения
    __BASE_CONFIG_PREFIX = get_run_prefix()

    # Провайдер данных по умолчанию
    __BASE_PROVIDER_NAME = 'postgresql+psycopg'

    # Хост базы данных
    __DB_HOST = os.environ.get('DB_HOST')

    # Порт базы данных
    __DB_PORT = os.environ.get('DB_PORT')

    # Имя базы данных
    __DB_NAME = os.environ.get('VK_DB_NAME')

    # Идентификатор пользователя базы данных
    __USER_ID = os.environ.get('DB_USER')

    # Пароль пользователя базы данных
    __USER_PWD = os.environ.get('DB_PASS')

    # Версия vk api
    __VK_API_VERSION = "5.199"

    # Сервисный ключ доступа к vk api
    __VK_ACCESS_TOKEN = os.environ.get('VK_SERVICE_ACCESS_TOKEN')

    @classmethod
    def get_root_dir(cls):
        """
        Возвращает значение переменной класса ROOT_DIR
        """
        return cls.__ROOT_DIR

    @classmethod
    def get_log_dir(cls):
        """
        Возвращает значение переменной класса LOG_DIR
        """
        return cls.__LOG_DIR

    @classmethod
    def get_base_config_prefix(cls):
        """
        Возвращает значение переменной класса BASE_CONFIG_PREFIX
        """
        return cls.__BASE_CONFIG_PREFIX

    @classmethod
    def get_base_provider_name(cls):
        """
        Возвращает значение переменной класса BASE_PROVIDER_NAME
        """
        return cls.__BASE_PROVIDER_NAME

    @classmethod
    def get_db_host(cls):
        """
        Возвращает значение переменной класса __DB_HOST
        """
        return cls.__DB_HOST

    @classmethod
    def get_db_port(cls):
        """
        Возвращает значение переменной класса __DB_PORT
        """
        return cls.__DB_PORT

    @classmethod
    def get_db_name(cls):
        """
        Возвращает значение переменной класса __DB_NAME
        """
        return cls.__DB_NAME

    @classmethod
    def get_user_id(cls):
        """
        Возвращает значение переменной класса __USER_ID
        """
        return cls.__USER_ID

    @classmethod
    def get_user_pwd(cls):
        """
        Возвращает значение переменной класса __USER_PWD
        """
        return cls.__USER_PWD

    @classmethod
    def get_vk_api_version(cls):
        """
        Возвращает значение переменной класса __VK_API_VERSION
        """
        return cls.__VK_API_VERSION

    @classmethod
    def get_vk_access_token(cls):
        """
        Возвращает значение переменной класса __VK_ACCESS_TOKEN
        """
        return cls.__VK_ACCESS_TOKEN


print(f'Base provider name: <{AppConfig.get_base_provider_name()}>')
print(f'Base prefix: <{AppConfig.get_base_config_prefix()}>')
