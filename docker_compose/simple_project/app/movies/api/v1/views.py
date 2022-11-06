from django.http import HttpResponse
from django.views import View
from django.contrib.postgres.aggregates import ArrayAgg
from django.db.models import Q
from django.views.generic.detail import BaseDetailView
from django.http import JsonResponse
from django.views.generic.list import BaseListView

from movies.models import Filmwork, PersonFilmworkRole

def api(request):
    return HttpResponse("My best API")

class MoviesApiMixin:
    model = Filmwork
    http_method_names = ['get']  # Список методов, которые реализует обработчик

    def get_queryset(self, **kwargs):
        all_films = self.model.objects.prefetch_related('genres', 'persons')
        # print(connection.queries)
        return all_films

    def render_to_response(self, context, **response_kwargs):
        return JsonResponse(context)

class MoviesListApi(BaseListView, MoviesApiMixin):
    model = Filmwork
    http_method_names = ['get']  # Список методов, которые реализует обработчик
    paginate_by = 50
    def get_context_data(self, *, object_list=None, **kwargs):
        queryset = MoviesApiMixin.get_queryset(self)
        results = list()

        paginator, page, queryset, is_paginated = self.paginate_queryset(
            queryset,
            self.paginate_by)

        for i in range(0, len(queryset)):
            genres = queryset[i].genres.all()
            persons = queryset[i].persons.all()

            item = dict()
            item['id'] = queryset[i].id
            item['title'] = queryset[i].title
            item['description'] = queryset[i].description
            item['creation_date'] = queryset[i].creation_date
            item['rating'] = float(queryset[i].rating) if queryset[i].rating else None
            item['type'] = queryset[i].type
            item['genres'] = [genre.name for genre in genres]
            item['actors'] = [person.full_name for person in
                              persons.filter(personfilmwork__role=PersonFilmworkRole.ACTOR)]
            item['directors'] = [person.full_name for person in
                                 persons.filter(personfilmwork__role=PersonFilmworkRole.DIRECTOR)]
            item['writers'] = [person.full_name for person in
                               persons.filter(personfilmwork__role=PersonFilmworkRole.SCENARIST)]
            results.append(item)

        context = {
            'count': paginator.count,
            'total_pages': paginator.num_pages,
            'prev': page.previous_page_number() if page.has_previous() else None,
            'next': page.next_page_number() if page.has_next() else None,
            'results': results,
        }

        return context

class MoviesDetailApi(MoviesApiMixin, BaseDetailView):
    model = Filmwork
    http_method_names = ['get']

    def get_context_data(self, *, object_list=None, **kwargs):
        queryset = MoviesApiMixin.get_queryset(self)
        obj = BaseDetailView.get_object(self, queryset)
        genres = obj.genres.all()
        persons = obj.persons.all()

        item = dict()
        item['id'] = obj.id
        item['title'] = obj.title
        item['description'] = obj.description
        item['creation_date'] = obj.creation_date
        item['rating'] = float(obj.rating) if obj.rating else None
        item['type'] = obj.type
        item['genres'] = [genre.name for genre in genres]
        item['actors'] = [person.full_name for person in
                              persons.filter(personfilmwork__role=PersonFilmworkRole.ACTOR)]
        item['directors'] = [person.full_name for person in
                                 persons.filter(personfilmwork__role=PersonFilmworkRole.DIRECTOR)]
        item['writers'] = [person.full_name for person in
                               persons.filter(personfilmwork__role=PersonFilmworkRole.SCENARIST)]
        context = item

        return context
