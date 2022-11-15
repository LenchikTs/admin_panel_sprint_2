import abc
import json
from typing import Any, Optional


class BaseStorage(abc.ABC):
    @abc.abstractmethod
    def save_state(self, state: dict) -> None:
        """Сохранить состояние в постоянное хранилище"""

    @abc.abstractmethod
    def retrieve_state(self) -> dict:
        """Загрузить состояние локально из постоянного хранилища"""


class JsonFileStorage(BaseStorage):
    """Класс для работы с сохранением и загрузкой данных в файл в JSON формате."""
    def __init__(self, file_path: Optional[str] = None):
        self.file_path = file_path

    def retrieve_state(self) -> dict:
        try:
            with open(self.file_path, 'r') as f:
                try:
                    data = json.load(f)
                    return data
                except:
                    return dict()
        except FileNotFoundError:
            return dict()

    def save_state(self, state: dict) -> None:
        with open(self.file_path, 'w') as f:
            data = json.dumps(state)
            f.write(data)


class State:
    """
    Класс для хранения состояния при работе с данными, чтобы постоянно не перечитывать данные с начала.
    Релизовано сохранение состояния в файл.
    """

    def __init__(self, storage: BaseStorage):
        self.storage = storage

    def set_state(self, key: str, value: Any) -> None:
        """Установить состояние для определённого ключа"""
        if not key:
            return None

        self.storage.save_state({key: value})

    def get_state(self, key: str) -> Any:
        """Получить состояние по определённому ключу"""
        state = self.storage.retrieve_state()
        return state.get(key)


def get_status(filepath):
    """Функция, отвечающая за создание состояния и сохраняющая данные в переданный файл.
    :param filepath: Путь к файлу
    :return: текущее состояние"""
    storage = JsonFileStorage(filepath)
    state = State(storage)
    return state
