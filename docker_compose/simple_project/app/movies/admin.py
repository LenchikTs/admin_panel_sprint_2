from django.contrib import admin
from .models import Genre, Filmwork, GenreFilmwork, PersonFilmwork, Person


@admin.register(Genre)
class GenreAdmin(admin.ModelAdmin):
    # pass

    # Отображение полей в списке
    list_display = ('name',)

    # Фильтрация в списке
    list_filter = ('name',)

    # Поиск по полям
    search_fields = ('description', 'id')

class GenreFilmworkInline(admin.TabularInline):
    model = GenreFilmwork
    raw_id_fields = ('genre',)

@admin.register(Person)
class PersonAdmin(admin.ModelAdmin):
    # pass
    # Отображение полей в списке
    list_display = ('full_name', )

    # Фильтрация в списке
    list_filter = ('full_name', )

    # Поиск по полям
    search_fields = ('full_name', 'id')


class PersonFilmworkInline(admin.TabularInline):
    model = PersonFilmwork
    raw_id_fields = ('person',)


@admin.register(Filmwork)
class FilmworkAdmin(admin.ModelAdmin):
    inlines = (GenreFilmworkInline, PersonFilmworkInline) #

    # Отображение полей в списке
    list_display = ('title', 'type', 'creation_date', 'rating',)

    # Фильтрация в списке
    list_filter = ('type', 'creation_date')

    # Поиск по полям
    search_fields = ('title', 'description', 'id')

