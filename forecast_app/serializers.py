from django.contrib.auth.models import User
from rest_framework import serializers
from rest_framework.reverse import reverse

from forecast_app.models import Project, Target, TimeZero, ForecastModel, Forecast
from forecast_app.models.upload_file_job import UploadFileJob
from forecast_app.views import forecast_models_owned_by_user, projects_and_roles_for_user, \
    timezero_forecast_pairs_for_forecast_model


class TargetSerializer(serializers.ModelSerializer):
    class Meta:
        model = Target
        fields = ('name', 'description')


class TimeZeroSerializer(serializers.ModelSerializer):
    class Meta:
        model = TimeZero
        fields = ('timezero_date', 'data_version_date')


class ProjectSerializer(serializers.HyperlinkedModelSerializer):
    owner = serializers.HyperlinkedRelatedField(view_name='api-user-detail', read_only=True)
    config_dict = serializers.SerializerMethodField()
    template = serializers.SerializerMethodField()
    truth = serializers.SerializerMethodField()

    models = serializers.HyperlinkedRelatedField(view_name='api-model-detail', many=True, read_only=True)
    targets = TargetSerializer(many=True, read_only=True)  # nested, no urls
    timezeros = TimeZeroSerializer(many=True, read_only=True)  # nested, no urls


    class Meta:
        model = Project
        fields = ('id', 'url', 'owner', 'is_public', 'name', 'description', 'home_url', 'core_data', 'config_dict',
                  'template', 'truth', 'model_owners', 'models', 'targets', 'timezeros')
        extra_kwargs = {
            'url': {'view_name': 'api-project-detail'},
            'model_owners': {'view_name': 'api-user-detail'},
        }


    def get_config_dict(self, project):
        return project.config_dict


    def get_template(self, project):
        request = self.context['request']
        return reverse('api-template-detail', args=[project.pk], request=request)


    def get_truth(self, project):
        request = self.context['request']
        return reverse('api-truth-detail', args=[project.pk], request=request)


class TemplateSerializer(serializers.ModelSerializer):
    project = serializers.SerializerMethodField()
    template_data = serializers.SerializerMethodField()


    class Meta:
        model = Project
        fields = ('id', 'url', 'project', 'csv_filename', 'template_data')
        extra_kwargs = {
            'url': {'view_name': 'api-template-detail'},
        }


    def get_project(self, project):
        request = self.context['request']
        return reverse('api-project-detail', args=[project.pk], request=request)


    def get_template_data(self, project):
        request = self.context['request']
        return reverse('api-template-data', args=[project.pk], request=request)


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
    output_json = serializers.JSONField()  # per https://github.com/dmkoch/django-jsonfield/issues/188


    class Meta:
        model = UploadFileJob
        fields = ('id', 'url', 'status', 'user', 'created_at', 'updated_at', 'is_failed', 'failure_message', 'filename',
                  'input_json', 'output_json')
        extra_kwargs = {
            'url': {'view_name': 'api-upload-file-job-detail'},
        }


class ForecastModelSerializer(serializers.ModelSerializer):
    owner = serializers.HyperlinkedRelatedField(view_name='api-user-detail', read_only=True)
    project = serializers.HyperlinkedRelatedField(view_name='api-project-detail', read_only=True)
    forecasts = serializers.SerializerMethodField()


    class Meta:
        model = ForecastModel
        fields = ('id', 'url', 'project', 'owner', 'name', 'description', 'home_url', 'aux_data_url', 'forecasts')
        extra_kwargs = {
            'url': {'view_name': 'api-model-detail'},
        }


    def get_forecasts(self, forecast_model):
        request = self.context['request']
        return [{'timezero_date': timezero.timezero_date,
                 'data_version_date': timezero.data_version_date,
                 'forecast': reverse('api-forecast-detail', args=[forecast.pk], request=request) if forecast else None}
                for timezero, forecast in timezero_forecast_pairs_for_forecast_model(forecast_model)]


class ForecastSerializer(serializers.ModelSerializer):
    forecast_model = serializers.HyperlinkedRelatedField(view_name='api-model-detail', read_only=True)
    time_zero = TimeZeroSerializer()  # nested, no urls
    forecast_data = serializers.SerializerMethodField()


    class Meta:
        model = Forecast
        fields = ('id', 'url', 'forecast_model', 'csv_filename', 'time_zero', 'forecast_data')
        extra_kwargs = {
            'url': {'view_name': 'api-forecast-detail'},
        }


    def get_forecast_data(self, forecast):
        request = self.context['request']
        return reverse('api-forecast-data', args=[forecast.pk], request=request)
