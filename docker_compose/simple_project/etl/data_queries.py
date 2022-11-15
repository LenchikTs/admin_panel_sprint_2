def get_list_update_filmwork_query(fw_modified):
    """
        Функция с созданием запроса по фильмам.
        :return: передача в функцию для дальнейшего сбора данных
            """
    return """SELECT
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

def get_list_update_person_query(p_modified):
    """
        Функция с созданием запроса по персонам.
        :return: передача в функцию для дальнейшего сбора данных
            """
    return """SELECT
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

def get_list_update_genre_query(g_modified):
    """
    Функция с созданием запроса по жанрам.
    :return: передача в функцию для дальнейшего сбора данных
    """
    return """SELECT
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

def get_list_update_persons_query(person_modified):
    """
        Функция с созданием запроса по person.
        :return: передача в функцию для дальнейшего сбора данных
            """
    return """Select person.id, person.full_name, pfw.role,

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

def get_list_update_genres_query(genre_modified):
    """
        Функция с созданием запроса по genre.
        :return: передача в функцию для дальнейшего сбора данных
            """
    return """Select genre.id, genre.name,
                max(genre.modified) as modified
                from content.genre
                {}
                group by genre.id""".format(genre_modified)

