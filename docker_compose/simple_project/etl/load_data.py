import uuid
import datetime

from psycopg2.extensions import connection as _connection
from elasticsearch import Elasticsearch
from status_check import State

from pydantic import BaseModel


class Movies(BaseModel):
    """Pydantic model для валидирования конфигурации"""
    id: uuid.UUID = uuid.uuid4
    title: str
    description: str = None
    rating: float = None
    actors: list
    actors_names: list
    writers: list
    writers_names: list
    director: str
    genre: list
    modified: datetime.datetime = None


class Person(BaseModel):
    """Pydantic model для валидирования конфигурации"""
    id: uuid.UUID = uuid.uuid4
    full_name: str
    role: str
    film_ids: list
    modified: datetime.datetime = None


class Genre(BaseModel):
    """Pydantic model для валидирования конфигурации"""
    id: uuid.UUID = uuid.uuid4
    name: str
    modified: datetime.datetime = None


class PostgresLoader:
    """
    Класс, отвечающий за соединение с бд Postgresql и сохранение данных для каждой таблицы.

    Параметры:
    pg_conn(_connection): соединение с бд
    """

    def __init__(self, pg_conn: _connection):
        self.conn = pg_conn
        self.curs = self.conn.cursor()

    def collect_movies(self, stmt):
        """
        Функция принимающая запрос. Помещает данные в класс Movies и возвращает генератор с фильмами.
        :param stmt: запрос для Postgresql
        :return: Генератор на 200 фильмов.
        """
        self.curs.execute(stmt)
        data = [1]
        while len(data) > 0:
            movies_list = []
            data = self.curs.fetchmany(200)
            for row in data:
                row = dict(row)
                actors_names_list = []
                actors_list = []
                writers_names_list = []
                writers_list = []
                director = ''
                genres_list = []
                id = row['id']
                title = row['title']
                description = row['description']
                rating = row['rating']
                modified = row['modified']

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

                row_data = {
                    'id': id,
                    'title': title,
                    'description': description,
                    'rating': rating,
                    'actors': actors_list,
                    'actors_names': actors_names_list,
                    'writers': writers_list,
                    'writers_names': writers_names_list,
                    'director': director,
                    'genre': genres_list,
                    'modified': modified
                }
                movies_list.append(Movies(**row_data))
            yield movies_list

    def collect_persons(self, stmt):
        """
        Функция принимающая запрос. Помещает данные в класс Movies и возвращает генератор с персонами.
        :param stmt: запрос для Postgresql
        :return: Генератор на 200 персон.
        """
        self.curs.execute(stmt)
        data = [1]
        while len(data) > 0:
            movies_list = []
            data = self.curs.fetchmany(200)
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

    def collect_genres(self, stmt):
        """
        Функция, принимающая запрос. Помещает данные в класс Movies и возвращает генератор с жанрами.
        :param stmt: запрос для Postgresql
        :return: Генератор на 200 жанров.
        """
        self.curs.execute(stmt)
        data = [1]
        while len(data) > 0:
            movies_list = []
            data = self.curs.fetchmany(200)
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
        stmt = """SELECT
                   fw.id,
                   fw.title,
                   fw.description,
                   fw.rating,
                   fw.type,
                   fw.created,
                   fw.modified,
                   COALESCE (
                       json_agg(
                           DISTINCT jsonb_build_object(
                               'person_role', pfw.role,
                               'person_id', p.id,
                               'person_name', p.full_name
                           )
                       ) FILTER (WHERE p.id is not null),
                       '[]'
                   ) as persons,
                   array_agg(DISTINCT g.name) as genres
                FROM content.film_work fw
                LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
                LEFT JOIN content.person p ON p.id = pfw.person_id
                LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
                LEFT JOIN content.genre g ON g.id = gfw.genre_id
                {}
                GROUP BY fw.id
                ORDER BY fw.modified;""".format(fw_modified)
        return self.collect_movies(stmt)

    def get_list_update_person(self, status):
        """
            Функция с созданием запроса по персонам.
            :param status: Требуется для фильтра даты.
            :return: передача в функцию для дальнейшего сбора данных
                """
        date = status.get_state('datetime')
        p_modified = "WHERE p.modified > '{}'".format(date) if date else ''
        stmt = """SELECT
           fw.id,
           fw.title,
           fw.description,
           fw.rating,
           fw.type,
           fw.created,
           max(p.modified) as modified,
           COALESCE (
               json_agg(
                   DISTINCT jsonb_build_object(
                       'person_role', pfw.role,
                       'person_id', p.id,
                       'person_name', p.full_name
                   )
               ) FILTER (WHERE p.id is not null),
               '[]'
           ) as persons,
           array_agg(DISTINCT g.name) as genres
        FROM content.film_work fw
        LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
        LEFT JOIN content.person p ON p.id = pfw.person_id
        LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
        LEFT JOIN content.genre g ON g.id = gfw.genre_id
        {}
        GROUP BY fw.id
        ORDER BY fw.modified;""".format(p_modified)
        return self.collect_movies(stmt)

    def get_list_update_genre(self, status):
        """
        Функция с созданием запроса по жанрам.
        :param status: Требуется для фильтра даты.
        :return: передача в функцию для дальнейшего сбора данных
        """
        date = status.get_state('datetime')
        g_modified = "WHERE g.modified > '{}'".format(date) if date else ''
        stmt = """SELECT
           fw.id,
           fw.title,
           fw.description,
           fw.rating,
           fw.type,
           fw.created,
           max(g.modified) as modified,
           COALESCE (
               json_agg(
                   DISTINCT jsonb_build_object(
                       'person_role', pfw.role,
                       'person_id', p.id,
                       'person_name', p.full_name
                   )
               ) FILTER (WHERE p.id is not null),
               '[]'
           ) as persons,
           array_agg(DISTINCT g.name) as genres
        FROM content.film_work fw
        LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
        LEFT JOIN content.person p ON p.id = pfw.person_id
        LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
        LEFT JOIN content.genre g ON g.id = gfw.genre_id
        {}
        GROUP BY fw.id
        ORDER BY fw.modified;""".format(g_modified)
        return self.collect_movies(stmt)

    def get_list_update_persons(self, status):
        """
            Функция с созданием запроса по person.
            :param status: Требуется для фильтра даты.
            :return: передача в функцию для дальнейшего сбора данных
                """
        date = status.get_state('datetime')
        person_modified = "WHERE person.modified > '{}'".format(date) if date else ''
        stmt = """Select person.id, person.full_name, pfw.role,
                    
                     COALESCE (
                       json_agg(
                           DISTINCT jsonb_build_object(
                               'films', fw.id
                           )
                       ) FILTER (WHERE fw.id is not null),
                       '[]'
                   ) as films,
    array_agg(DISTINCT fw.id) as films123,
                    max(person.modified) as modified
                    from content.person
                    left join content.person_film_work pfw on person.id = pfw.person_id
                    left join content.film_work fw on pfw.film_work_id = fw.id
                    {}
                    group by person.id, pfw.role""".format(person_modified)
        return self.collect_persons(stmt)

    def get_list_update_genres(self, status):
        """
            Функция с созданием запроса по genre.
            :param status: Требуется для фильтра даты.
            :return: передача в функцию для дальнейшего сбора данных
                """
        date = status.get_state('datetime')
        genre_modified = "WHERE genre.modified > '{}'".format(date) if date else ''
        stmt = """Select genre.id, genre.name,
                    max(genre.modified) as modified
                    from content.genre
                    {}
                    group by genre.id""".format(genre_modified)
        return self.collect_genres(stmt)


def prepare_to_elastic(movie):
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

    def save_data(self, postgres_loader: PostgresLoader, status: State):
        """
        Функция для сохранения методом bulk обработанной информации с Postgresql. Также при успешном выполнении
        сохраняет состояние в файл status.json.

        :param postgres_loader: Экземпляр класса PostgresLoader
        :param status: Текущие состояние на момент подключения к БД.
        """
        body_list_films = 1
        body_list_persons = 1
        body_list_genres = 1
        last_datetime = datetime.datetime(2, 1, 1)  # Мин время

        while body_list_films:
            data_list = [
                postgres_loader.get_list_update_filmwork(status),
                postgres_loader.get_list_update_person(status),
                postgres_loader.get_list_update_genre(status)
            ]

            for query in data_list:
                for list_movies in query:
                    body_list_films = []
                    for movie in list_movies:
                        if movie.modified and movie.modified.astimezone() > last_datetime.astimezone():
                            last_datetime = movie.modified
                        body_list_films.extend(prepare_to_elastic(movie))
                    if body_list_films:
                        self.conn.bulk(body=body_list_films)

        while body_list_persons:
            data_list = [
                postgres_loader.get_list_update_persons(status),
            ]

            for query in data_list:
                for list_movies in query:
                    body_list_persons = []
                    for movie in list_movies:
                        if movie.modified and movie.modified.astimezone() > last_datetime.astimezone():
                            last_datetime = movie.modified
                        body_list_persons.extend(prepare_to_elastic_person(movie))
                    if body_list_persons:
                        self.conn.bulk(body=body_list_persons)

        while body_list_genres:
            data_list = [
                postgres_loader.get_list_update_genres(status),
            ]

            for query in data_list:
                for list_movies in query:
                    body_list_genres = []
                    for movie in list_movies:
                        if movie.modified and movie.modified.astimezone() > last_datetime.astimezone():
                            last_datetime = movie.modified
                        body_list_genres.extend(prepare_to_elastic_genre(movie))
                    if body_list_genres:
                        self.conn.bulk(body=body_list_genres)

        if last_datetime != datetime.datetime(2, 1, 1):
            status.set_state('datetime', str(last_datetime))
