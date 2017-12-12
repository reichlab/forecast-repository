from django.contrib.auth.models import User
from django.http import JsonResponse
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
        'users': reverse('api-user-list', request=request, format=format),
        'projects': reverse('api-project-list', request=request, format=format),
    })


# was ListCreateAPIView -> def perform_create(self, serializer): serializer.save(owner=self.request.user)
class ProjectList(generics.ListAPIView):
    queryset = Project.objects.all()
    serializer_class = ProjectSerializer
    # permission_classes = (permissions.IsAuthenticatedOrReadOnly,)


class ProjectDetail(generics.RetrieveAPIView):
    queryset = Project.objects.all()
    serializer_class = ProjectSerializer
    # permission_classes = (permissions.IsAuthenticatedOrReadOnly, IsOwnerOrReadOnly,)


@api_view(['GET'])
def template_data(request, project_pk):
    """
    :return: the Project's template data as JSON
    """
    project = get_object_or_404(Project, pk=project_pk)
    location_target_dict = project.get_location_target_dict()

    # note: I tried to use a rest_framework.response.Response, which is supposed to support pretty printing on the
    # client side via something like:
    #   curl -H 'Accept: application/json; indent=4' http://127.0.0.p1:8000/api/project/1/template_data/
    # but when I tried this, returned a delimited string instead of JSON:
    #   return Response(JSONRenderer().render(location_target_dict))
    # via https://stackoverflow.com/questions/23195210/how-to-get-pretty-output-from-rest-framework-serializer
    return JsonResponse(location_target_dict)


@api_view(['GET'])
def forecast_data(request, forecast_pk):
    """
    :return: the Project's template data as JSON
    """
    forecast = get_object_or_404(Forecast, pk=forecast_pk)
    location_target_dict = forecast.get_location_target_dict()
    return JsonResponse(location_target_dict)


class UserList(generics.ListCreateAPIView):
    queryset = User.objects.all()
    serializer_class = UserSerializer


class UserDetail(generics.RetrieveAPIView):
    queryset = User.objects.all()
    serializer_class = UserSerializer


class ForecastModelDetail(generics.RetrieveAPIView):
    queryset = ForecastModel.objects.all()
    serializer_class = ForecastModelSerializer


class ForecastDetail(generics.RetrieveAPIView):
    queryset = Forecast.objects.all()
    serializer_class = ForecastSerializer
