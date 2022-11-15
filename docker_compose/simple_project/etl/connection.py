import logging
import os
import time
from functools import wraps

import psycopg2
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor
from elasticsearch import Elasticsearch
from load_data import PostgresLoader, ElasticSearchSaver
from status_check import get_status


def load_from_pysql(connection: Elasticsearch, pg_conn: _connection, status):
    """
    Основной метод загрузки данных из Postgres в Elasticsearch

    :param connection: Соединение с Elasticsearch
    :param pg_conn: Соединение с Postgres
    :param status: Экземпляр класса, отвечающий за получение/загрузку состояния.
    """
    postgres_loader = PostgresLoader(pg_conn)
    es_saver = ElasticSearchSaver(connection)
    es_saver.save_data(postgres_loader, status)


def backoff(start_sleep_time=0.1, factor=2, border_sleep_time=10):
    """
    Функция для повторного выполнения функции через некоторое время, если возникла ошибка.
    Использует наивный экспоненциальный рост времени повтора (factor) до граничного времени ожидания (border_sleep_time)

    Формула:
        t = start_sleep_time * 2^(n) if t < border_sleep_time
        t = border_sleep_time if t >= border_sleep_time
    :param start_sleep_time: начальное время повтора
    :param factor: во сколько раз нужно увеличить время ожидания
    :param border_sleep_time: граничное время ожидания
    :return: результат выполнения функции
    """

    def func_wrapper(func):
        @wraps(func)
        def inner(*args, **kwargs):
            delay = start_sleep_time
            num = 1
            while True:
                try:
                    return func()
                    delay = start_sleep_time
                    time.sleep(border_sleep_time)
                except Exception as err:
                    logging.error("err", exc_info=True)
                    time.sleep(delay)
                    num += 1
                    if num > 10:
                        break
                    if delay >= border_sleep_time:
                        delay = border_sleep_time
                    else:
                        delay = min(delay * factor, border_sleep_time)
            logging.info('timeout')

        return inner

    return func_wrapper

@backoff()
def connection():
    """Функция отвечающая за подключение к базам данных"""
    dsl = {'dbname': os.environ.get('DB_NAME'), 'user': os.environ.get('DB_USER'),
           'password': os.environ.get('DB_PASSWORD'), 'host': os.environ.get('DB_HOST'),
           'port': os.environ.get('DB_PORT')}
    with psycopg2.connect(**dsl, cursor_factory=DictCursor) as pg_conn, \
            Elasticsearch(os.environ.get('ELASTIC_PORT')) as es:
        status = get_status('status.json')
        load_from_pysql(es, pg_conn, status)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, filename="logs.log", filemode="w",
                        format="%(asctime)s %(levelname)s %(message)s")
    # Добавила паузы по 10 сек. При обрыве подключения после 10 попытки скрипт завершается.
    connection()
