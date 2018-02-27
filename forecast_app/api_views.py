from django.contrib.auth.mixins import UserPassesTestMixin
from django.contrib.auth.models import User
from django.http import JsonResponse, HttpResponseForbidden
from rest_framework import generics
from rest_framework.decorators import api_view
from rest_framework.generics import get_object_or_404
from rest_framework.response import Response
from rest_framework.reverse import reverse

from forecast_app.models import Project, ForecastModel, Forecast
from forecast_app.serializers import ProjectSerializer, UserSerializer, ForecastModelSerializer, ForecastSerializer


@api_view(['GET'])
def api_root(request, format=None):
    return Response({
        'projects': reverse('api-project-list', request=request, format=format),
    })


# was ListCreateAPIView -> def perform_create(self, serializer): serializer.save(owner=self.request.user)
class ProjectList(generics.ListAPIView):
    queryset = Project.objects.all()
    serializer_class = ProjectSerializer


class ProjectDetail(UserPassesTestMixin, generics.RetrieveAPIView):
    queryset = Project.objects.all()
    serializer_class = ProjectSerializer
    raise_exception = True  # o/w does HTTP_302_FOUND (redirect) https://docs.djangoproject.com/en/1.11/topics/auth/default/#django.contrib.auth.mixins.AccessMixin.raise_exception


    def test_func(self):  # return True if the current user can access the view
        project = self.get_object()
        return project.is_user_allowed_to_view(self.request.user)


@api_view(['GET'])
def template_data(request, project_pk):
    """
    :return: the Project's template data as JSON. note that the actual data is wrapped by metadata
    """
    project = get_object_or_404(Project, pk=project_pk)
    if not project.is_user_allowed_to_view(request.user):
        return HttpResponseForbidden()

    # note: I tried to use a rest_framework.response.Response, which is supposed to support pretty printing on the
    # client side via something like:
    #   curl -H 'Accept: application/json; indent=4' http://127.0.0.p1:8000/api/project/1/template_data/
    # but when I tried this, returned a delimited string instead of JSON:
    #   return Response(JSONRenderer().render(location_target_dict))
    # via https://stackoverflow.com/questions/23195210/how-to-get-pretty-output-from-rest-framework-serializer
    metadata_dict = ProjectSerializer(project, context={'request': request}).data
    location_target_dict = project.get_location_target_dict()
    return JsonResponse({'metadata': metadata_dict,
                         'data': location_target_dict})


@api_view(['GET'])
def forecast_data(request, forecast_pk):
    """
    :return: the Project's template data as JSON. note that the actual data is wrapped by metadata
    """
    forecast = get_object_or_404(Forecast, pk=forecast_pk)
    if not forecast.forecast_model.project.is_user_allowed_to_view(request.user):
        return HttpResponseForbidden()

    metadata_dict = ForecastSerializer(forecast, context={'request': request}).data
    location_target_dict = forecast.get_location_target_dict()
    return JsonResponse({'metadata': metadata_dict,
                         'data': location_target_dict})


class UserList(generics.ListCreateAPIView):
    queryset = User.objects.all()
    serializer_class = UserSerializer


class UserDetail(generics.RetrieveAPIView):
    queryset = User.objects.all()
    serializer_class = UserSerializer


class ForecastModelDetail(UserPassesTestMixin, generics.RetrieveAPIView):
    queryset = ForecastModel.objects.all()
    serializer_class = ForecastModelSerializer
    raise_exception = True  # o/w does HTTP_302_FOUND (redirect) https://docs.djangoproject.com/en/1.11/topics/auth/default/#django.contrib.auth.mixins.AccessMixin.raise_exception


    def test_func(self):  # return True if the current user can access the view
        forecast_model = self.get_object()
        return forecast_model.project.is_user_allowed_to_view(self.request.user)


class ForecastDetail(UserPassesTestMixin, generics.RetrieveAPIView):
    queryset = Forecast.objects.all()
    serializer_class = ForecastSerializer
    raise_exception = True  # o/w does HTTP_302_FOUND (redirect) https://docs.djangoproject.com/en/1.11/topics/auth/default/#django.contrib.auth.mixins.AccessMixin.raise_exception


    def test_func(self):  # return True if the current user can access the view
        forecast = self.get_object()
        return forecast.forecast_model.project.is_user_allowed_to_view(self.request.user)
