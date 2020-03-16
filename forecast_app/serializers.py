from django.contrib.auth.models import User
from rest_framework import serializers
from rest_framework.reverse import reverse

from forecast_app.models import Project, Target, TimeZero, ForecastModel, Forecast
from forecast_app.models.project import Unit
from forecast_app.models.upload_file_job import UploadFileJob
from forecast_app.views import forecast_models_owned_by_user, projects_and_roles_for_user
from utils.utilities import YYYY_MM_DD_DATE_FORMAT


class UnitSerializer(serializers.ModelSerializer):
    class Meta:
        model = Unit
        fields = ('id', 'url', 'name',)
        extra_kwargs = {
            'url': {'view_name': 'api-unit-detail'},
        }


class TargetSerializer(serializers.ModelSerializer):
    type = serializers.SerializerMethodField()


    class Meta:
        model = Target
        fields = ('id', 'url', 'name', 'description', 'type', 'is_step_ahead', 'step_ahead_increment', 'unit')
        extra_kwargs = {
            'url': {'view_name': 'api-target-detail'},
        }


    def get_type(self, target):
        return target.type_as_str()


class TimeZeroSerializer(serializers.HyperlinkedModelSerializer):
    # customize these to use our standard format
    timezero_date = serializers.DateField(format=YYYY_MM_DD_DATE_FORMAT, input_formats=[YYYY_MM_DD_DATE_FORMAT])
    data_version_date = serializers.DateField(format=YYYY_MM_DD_DATE_FORMAT, input_formats=[YYYY_MM_DD_DATE_FORMAT])


    class Meta:
        model = TimeZero
        fields = ('id', 'url', 'timezero_date', 'data_version_date', 'is_season_start', 'season_name')
        extra_kwargs = {
            'url': {'view_name': 'api-timezero-detail'},
        }


class ProjectSerializer(serializers.HyperlinkedModelSerializer):
    time_interval_type = serializers.SerializerMethodField()
    truth = serializers.SerializerMethodField()
    score_data = serializers.SerializerMethodField()

    models = serializers.HyperlinkedRelatedField(view_name='api-model-detail', many=True, read_only=True)
    units = serializers.HyperlinkedRelatedField(view_name='api-unit-detail', many=True, read_only=True)
    targets = serializers.HyperlinkedRelatedField(view_name='api-target-detail', many=True, read_only=True)
    timezeros = serializers.HyperlinkedRelatedField(view_name='api-timezero-detail', many=True, read_only=True)


    class Meta:
        model = Project
        fields = ('id', 'url', 'owner', 'is_public', 'name', 'description', 'home_url', 'time_interval_type',
                  'visualization_y_label', 'core_data', 'truth', 'model_owners', 'score_data', 'models', 'units',
                  'targets', 'timezeros')
        extra_kwargs = {
            'url': {'view_name': 'api-project-detail'},
            'owner': {'view_name': 'api-user-detail'},
            'model_owners': {'view_name': 'api-user-detail'},
        }


    def get_time_interval_type(self, project):
        return project.time_interval_type_as_str()


    def get_truth(self, project):
        request = self.context['request']
        return reverse('api-truth-detail', args=[project.pk], request=request)


    def get_score_data(self, project):
        request = self.context['request']
        return reverse('api-score-data', args=[project.pk], request=request)


class TruthSerializer(serializers.ModelSerializer):
    project = serializers.SerializerMethodField()
    truth_data = serializers.SerializerMethodField()


    class Meta:
        model = Project
        fields = ('id', 'url', 'project', 'truth_csv_filename', 'truth_data')
        extra_kwargs = {
            'url': {'view_name': 'api-truth-detail'},
        }


    def get_project(self, project):
        request = self.context['request']
        return reverse('api-project-detail', args=[project.pk], request=request)


    def get_truth_data(self, project):
        request = self.context['request']
        return reverse('api-truth-data', args=[project.pk], request=request)


class UserSerializer(serializers.ModelSerializer):
    owned_models = serializers.SerializerMethodField()
    projects_and_roles = serializers.SerializerMethodField()


    class Meta:
        model = User
        fields = ('id', 'url', 'username', 'owned_models', 'projects_and_roles')
        extra_kwargs = {
            'url': {'view_name': 'api-user-detail'},
        }


    def get_owned_models(self, user):
        request = self.context['request']
        return [reverse('api-model-detail', args=[forecast_model.pk], request=request) for forecast_model in
                forecast_models_owned_by_user(user)]


    def get_projects_and_roles(self, user):
        request = self.context['request']
        return [{'project': reverse('api-project-detail', args=[project.pk], request=request),
                 'is_project_owner': role == 'Project Owner',
                 'is_model_owner': role == 'Model Owner'}
                for project, role in projects_and_roles_for_user(user)]


class UploadFileJobSerializer(serializers.ModelSerializer):
    user = serializers.HyperlinkedRelatedField(view_name='api-user-detail', read_only=True)
    input_json = serializers.JSONField()  # per https://github.com/dmkoch/django-jsonfield/issues/188
    output_json = serializers.JSONField()  # ""


    class Meta:
        model = UploadFileJob
        fields = ('id', 'url', 'status', 'user', 'created_at', 'updated_at', 'failure_message', 'filename',
                  'input_json', 'output_json')
        extra_kwargs = {
            'url': {'view_name': 'api-upload-file-job-detail'},
        }


class ForecastModelSerializer(serializers.ModelSerializer):
    owner = serializers.HyperlinkedRelatedField(view_name='api-user-detail', read_only=True)
    project = serializers.HyperlinkedRelatedField(view_name='api-project-detail', read_only=True)
    forecasts = serializers.HyperlinkedRelatedField(view_name='api-forecast-detail', many=True, read_only=True)


    class Meta:
        model = ForecastModel
        fields = ('id', 'url', 'project', 'owner', 'name', 'abbreviation', 'description', 'home_url', 'aux_data_url',
                  'forecasts')
        extra_kwargs = {
            'url': {'view_name': 'api-model-detail'},
        }


class ForecastSerializer(serializers.ModelSerializer):
    forecast_model = serializers.HyperlinkedRelatedField(view_name='api-model-detail', read_only=True)
    time_zero = serializers.HyperlinkedRelatedField(view_name='api-timezero-detail', read_only=True)
    forecast_data = serializers.SerializerMethodField()


    class Meta:
        model = Forecast
        fields = ('id', 'url', 'forecast_model', 'source', 'time_zero', 'created_at', 'forecast_data')
        extra_kwargs = {
            'url': {'view_name': 'api-forecast-detail'},
        }


    def get_forecast_data(self, forecast):
        request = self.context['request']
        return reverse('api-forecast-data', args=[forecast.pk], request=request)
