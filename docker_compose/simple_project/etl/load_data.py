import uuid
import datetime

from psycopg2.extensions import connection as _connection
from elasticsearch import Elasticsearch

from data_queries import *
from status_check import State

from pydantic import BaseModel


class Movies(BaseModel):
    """Pydantic model для валидирования конфигурации"""
    id: uuid.UUID
    title: str
    description: str = None
    rating: float = None
    actors: list
    actors_names: list
    writers: list
    writers_names: list
    director: str
    genre: list
    modified: datetime.datetime | None = None


class Person(BaseModel):
    """Pydantic model для валидирования конфигурации"""
    id: uuid.UUID
    full_name: str
    role: str
    film_ids: list
    modified: datetime.datetime | None = None


class Genre(BaseModel):
    """Pydantic model для валидирования конфигурации"""
    id: uuid.UUID
    name: str
    modified: datetime.datetime | None = None


class PostgresLoader:
    """
    Класс, отвечающий за соединение с бд Postgresql и сохранение данных для каждой таблицы.

    Параметры:
    pg_conn(_connection): соединение с бд
    """

    def __init__(self, pg_conn: _connection):
        self.conn = pg_conn
        self.curs = self.conn.cursor()

    def collect_movies(self, stmt, num=200):
        """
        Функция принимающая запрос. Помещает данные в класс Movies и возвращает генератор с фильмами.
        :param stmt: запрос для Postgresql
        :return: Генератор на 200 фильмов.
        """
        def get_data_from_row(row):
            info = {}
            row = dict(row)
            actors_names_list = []
            actors_list = []
            writers_names_list = []
            writers_list = []
            director = ''
            genres_list = []
            info['id'] = row['id']
            info['title'] = row['title']
            info['description'] = row['description']
            info['rating'] = row['rating']
            info['modified'] = row['modified']

            for person in row['persons']:
                if person['person_role'] == 'actor':
                    actors_names_list.append(person['person_name'])
                    actors_list.append({
                        'id': person['person_id'],
                        'name': person['person_name']
                    })
                elif person['person_role'] == 'writer':
                    writers_names_list.append(person['person_name'])
                    writers_list.append({
                        'id': person['person_id'],
                        'name': person['person_name']
                    })
                elif person['person_role'] == 'director':
                    director = person['person_name']
            for genre in row['genres']:
                genres_list.append(genre)
            info['genres_list'] = genres_list
            info['director'] = director
            info['writers_list'] = writers_list
            info['writers_names_list'] = writers_names_list
            info['actors_list'] = actors_list
            info['actors_names_list'] = actors_names_list
            return info

        self.curs.execute(stmt)
        while data := self.curs.fetchmany(num):
            movies_list = []
            for row in data:
                info_data = get_data_from_row(row)
                row_data = {
                    'id': info_data.get('id'),
                    'title': info_data.get('title'),
                    'description': info_data.get('description'),
                    'rating': info_data.get('rating'),
                    'actors': info_data.get('actors_list'),
                    'actors_names': info_data.get('actors_names_list'),
                    'writers': info_data.get('writers_list'),
                    'writers_names': info_data.get('writers_names_list'),
                    'director': info_data.get('director'),
                    'genre': info_data.get('genres_list'),
                    'modified': info_data.get('modified')
                }
                movies_list.append(Movies(**row_data))
            yield movies_list

    def collect_persons(self, stmt, num=200):
        """
        Функция принимающая запрос. Помещает данные в класс Movies и возвращает генератор с персонами.
        :param stmt: запрос для Postgresql
        :return: Генератор на 200 персон.
        """
        self.curs.execute(stmt)
        while data := self.curs.fetchmany(num):
            movies_list = []
            for row in data:
                row = dict(row)

                films_list = []
                id = row['id']
                full_name = row['full_name']
                role = row['role']
                modified = row['modified']

                for film in row['films']:
                    films_list.append(film['films'])

                row_data = {
                    'id': id,
                    'full_name': full_name,
                    'role': role,
                    'film_ids': films_list,
                    'modified': modified
                }
                movies_list.append(Person(**row_data))
            yield movies_list

    def collect_genres(self, stmt, num=200):
        """
        Функция, принимающая запрос. Помещает данные в класс Movies и возвращает генератор с жанрами.
        :param stmt: запрос для Postgresql
        :return: Генератор на 200 жанров.
        """
        self.curs.execute(stmt)
        while data := self.curs.fetchmany(num):
            movies_list = []
            for row in data:
                row = dict(row)

                id = row['id']
                name = row['name']
                modified = row['modified']

                row_data = {
                    'id': id,
                    'name': name,
                    'modified': modified
                }
                movies_list.append(Genre(**row_data))
            yield movies_list

    def get_list_update_filmwork(self, status):
        """
            Функция с созданием запроса по фильмам.
            :param status: Требуется для фильтра даты.
            :return: передача в функцию для дальнейшего сбора данных
                """
        date = status.get_state('datetime')
        fw_modified = "WHERE fw.modified > '{}'".format(date) if date else ''
        stmt = get_list_update_filmwork_query(fw_modified)
        return self.collect_movies(stmt)

    def get_list_update_person(self, status):
        """
            Функция с созданием запроса по персонам.
            :param status: Требуется для фильтра даты.
            :return: передача в функцию для дальнейшего сбора данных
                """
        date = status.get_state('datetime')
        p_modified = "WHERE p.modified > '{}'".format(date) if date else ''
        stmt = get_list_update_person_query(p_modified)
        return self.collect_movies(stmt)

    def get_list_update_genre(self, status):
        """
        Функция с созданием запроса по жанрам.
        :param status: Требуется для фильтра даты.
        :return: передача в функцию для дальнейшего сбора данных
        """
        date = status.get_state('datetime')
        g_modified = "WHERE g.modified > '{}'".format(date) if date else ''
        stmt = get_list_update_genre_query(g_modified)
        return self.collect_movies(stmt)

    def get_list_update_persons(self, status):
        """
            Функция с созданием запроса по person.
            :param status: Требуется для фильтра даты.
            :return: передача в функцию для дальнейшего сбора данных
                """
        date = status.get_state('datetime')
        person_modified = "WHERE person.modified > '{}'".format(date) if date else ''
        stmt = get_list_update_persons_query(person_modified)
        return self.collect_persons(stmt)

    def get_list_update_genres(self, status):
        """
            Функция с созданием запроса по genre.
            :param status: Требуется для фильтра даты.
            :return: передача в функцию для дальнейшего сбора данных
                """
        date = status.get_state('datetime')
        genre_modified = "WHERE genre.modified > '{}'".format(date) if date else ''
        stmt = get_list_update_genres_query(genre_modified)
        return self.collect_genres(stmt)


def prepare_to_elastic_film(movie):
    """
    Функция, возвращающая список с подготовленными данными по одному фильму под формат Elasticsearch.
    :param movie: Фильм.
    :return:
    """
    single_body_list = []
    first_line = {'index': {'_index': 'movies', '_id': movie.id}}

    single_body_list.append(first_line)
    entry = {
        'actors': movie.actors,
        'actors_names': movie.actors_names,
        'description': movie.description,
        'director': movie.director,
        'genre': movie.genre,
        'id': movie.id,
        'imdb_rating': movie.rating,
        'title': movie.title,
        'writers': movie.writers,
        'writers_names': movie.writers_names
    }
    single_body_list.append(entry)
    return single_body_list


def prepare_to_elastic_person(movie):
    """
    Функция, возвращающая список с подготовленными данными по одному фильму под формат Elasticsearch.
    :param movie: Фильм.
    :return:
    """
    single_body_list = []
    first_line = {'index': {'_index': 'person', '_id': movie.id}}

    single_body_list.append(first_line)
    entry = {
        'film_ids': movie.film_ids,
        'id': movie.id,
        'full_name': movie.full_name,
        'role': movie.role,
    }
    single_body_list.append(entry)
    return single_body_list


def prepare_to_elastic_genre(movie):
    """
    Функция, возвращающая список с подготовленными данными по одному фильму под формат Elasticsearch.
    :param movie: Фильм.
    :return:
    """
    single_body_list = []
    first_line = {'index': {'_index': 'genre', '_id': movie.id}}

    single_body_list.append(first_line)
    entry = {
        'id': movie.id,
        'name': movie.name
    }
    single_body_list.append(entry)
    return single_body_list


class ElasticSearchSaver:
    """Класс, отвечающий за соединение с бд Elasticsearch и сохранение данных для каждой таблицы.

        :param connection: Соединение с Elasticsearch
    """

    def __init__(self, connection: Elasticsearch):
        self.conn = connection
        self.last_datetime = None

    def executeFunc(self, name):
        if hasattr(self, name):
            return getattr(self, name)
        return None

    def while_data(self, func_name, postgres_loader, status):
        body_list_films = 1
        while body_list_films:
            if func_name == 'film':
                data_list = [
                    postgres_loader.get_list_update_filmwork(status),
                    postgres_loader.get_list_update_person(status),
                    postgres_loader.get_list_update_genre(status)
                ]
            elif func_name == 'person':
                data_list = [
                    postgres_loader.get_list_update_persons(status),
                ]
            else:
                data_list = [
                    postgres_loader.get_list_update_genres(status),
                ]

            for query in data_list:
                for list_movies in query:
                    body_list_films = []
                    for movie in list_movies:
                        if movie.modified and movie.modified.astimezone() > self.last_datetime.astimezone():
                            self.last_datetime = movie.modified
                        body_list_films.extend(self.executeFunc("prepare_to_elastic_{}".format(func_name))(movie))
                    if body_list_films:
                        self.conn.bulk(body=body_list_films)

    def save_data(self, postgres_loader: PostgresLoader, status: State):
        """
        Функция для сохранения методом bulk обработанной информации с Postgresql. Также при успешном выполнении
        сохраняет состояние в файл status.json.

        :param postgres_loader: Экземпляр класса PostgresLoader
        :param status: Текущие состояние на момент подключения к БД.
        """
        self.last_datetime = datetime.datetime(2, 1, 1)  # Мин время
        func_names = ['film', 'person', 'genre']

        for f_name in func_names:
            self.while_data(f_name, postgres_loader, status)

        if self.last_datetime != datetime.datetime(2, 1, 1):
            status.set_state('datetime', str(self.last_datetime))
