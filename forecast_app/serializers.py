from django.contrib.auth.models import User
from rest_framework import serializers
from rest_framework.fields import CharField
from rest_framework.reverse import reverse

from forecast_app.models import Project, Target, TimeZero, ForecastModel, Forecast
from forecast_app.models.job import Job
from forecast_app.models.project import Unit
from forecast_app.models.target import reference_date_type_for_id
from forecast_app.views import forecast_models_owned_by_user, projects_and_roles_for_user
from utils.project_truth import oracle_model_for_project
from utils.utilities import YYYY_MM_DD_DATE_FORMAT


class UnitSerializer(serializers.ModelSerializer):
    class Meta:
        model = Unit
        fields = ('id', 'url', 'name', 'abbreviation')
        extra_kwargs = {
            'url': {'view_name': 'api-unit-detail'},
        }


class TargetSerializer(serializers.ModelSerializer):
    type = serializers.SerializerMethodField()  # convert int to str
    reference_date_type = serializers.SerializerMethodField()  # optional. convert into to str
    range = serializers.SerializerMethodField()  # optional
    cats = serializers.SerializerMethodField()  # ""


    class Meta:
        model = Target

        # all fields, including optional ones that are removed by `to_representation()`
        fields = ('id', 'url', 'name', 'type', 'description', 'outcome_variable', 'is_step_ahead',
                  'numeric_horizon', 'reference_date_type', 'range', 'cats')

        extra_kwargs = {
            'url': {'view_name': 'api-target-detail'},
        }


    def to_representation(self, target):
        # clear and re-cache the `self.fields` @cached_property for possible re-use by ListSerializer (many=True).
        # (recall that a single Serializer instance is re-used to generate all data in the ListSerializer queryset, but
        # we need to re-generate fields each time due to their being dynamic). per
        # https://stackoverflow.com/questions/50290390/list-serializer-with-dynamic-fields-in-django-rest-framework .
        try:
            del self.fields
        except AttributeError:
            pass
        _ = self.fields  # re-cache. "`fields` is evaluated lazily"

        # remove optional fields per https://stackoverflow.com/questions/17552380/django-rest-framework-serializing-optional-fields .
        # we do this by first calling super() to get the representation from which we remove the optional fields from,
        # which calls the SerializerMethodField get_*() functions below. by convention, those functions return None to
        # indicate that they are optional (None is never a valid field value), and return the field value o/w. however,
        # in the case of 'numeric_horizon' and 'reference_date_type' we simply test target.is_step_ahead b/c it's so
        # simple
        representation = super().to_representation(target)  # OrderedDict. initiates get_*() function calls below

        fields_to_remove = []  # filled next
        if not target.is_step_ahead:  # remove optional numeric_horizon and reference_date_type
            fields_to_remove.extend(('numeric_horizon', 'reference_date_type',))

        if representation['range'] is None:  # remove optional range
            fields_to_remove.append('range')

        if representation['cats'] is None:  # remove optional cats
            fields_to_remove.append('cats')

        if fields_to_remove:
            self._remove_fields_from_representation(representation, fields_to_remove)

        # done
        return representation


    def _remove_fields_from_representation(self, representation, remove_fields):
        # per https://stackoverflow.com/questions/17552380/django-rest-framework-serializing-optional-fields
        for remove_field in remove_fields:
            try:
                representation.pop(remove_field)
            except KeyError:
                pass


    def get_type(self, target):
        return target.type_as_str()  # convert int to str


    def get_reference_date_type(self, target):
        if (not target.is_step_ahead) or (target.reference_date_type is None):
            return None  # indicate unused value (field removed above)

        return reference_date_type_for_id(target.reference_date_type).name


    def get_range(self, target):
        target_ranges_qs = target.ranges  # target.value_i, target.value_f
        if target_ranges_qs.count() == 0:
            return None  # indicate unused value (field removed above)

        data_type = target.data_types()[0]  # the first is the preferred one
        value_column = 'value_i' if data_type == Target.INTEGER_DATA_TYPE else 'value_f'
        target_ranges = target_ranges_qs.values_list(value_column, flat=True)
        target_ranges = sorted(target_ranges)
        return [target_ranges[0], target_ranges[1]]


    def get_cats(self, target):
        data_type = target.data_types()[0]  # the first is the preferred one
        cats_values = target.cats_values()
        if target.type == Target.BINARY_TARGET_TYPE:
            # skip implicit binary, which is added automatically
            return None  # indicate unused value (field removed above)
        elif (not cats_values) and (target.type in [Target.NOMINAL_TARGET_TYPE, Target.DATE_TARGET_TYPE]):
            # handle the case of required cats list that must have come in but was empty
            return []

        if data_type == Target.DATE_DATA_TYPE:
            cats_values = [cat_date.strftime(YYYY_MM_DD_DATE_FORMAT) for cat_date in cats_values]
        return sorted(cats_values)


class TimeZeroSerializer(serializers.HyperlinkedModelSerializer):
    # customize to use our standard format
    timezero_date = serializers.DateField(format=YYYY_MM_DD_DATE_FORMAT, input_formats=[YYYY_MM_DD_DATE_FORMAT])
    data_version_date = serializers.DateField(format=YYYY_MM_DD_DATE_FORMAT, input_formats=[YYYY_MM_DD_DATE_FORMAT])


    class Meta:
        model = TimeZero

        # always include these fields:
        fields = ('id', 'url', 'timezero_date', 'data_version_date', 'is_season_start',)

        # optionally/dynamically include these fields:
        # fields = ('season_name')

        extra_kwargs = {
            'url': {'view_name': 'api-timezero-detail'},
        }


    def to_representation(self, timezero):
        # clear and re-cache the `self.fields` @cached_property for possible re-use by ListSerializer (many=True).
        # (recall that a single Serializer instance is re-used to generate all data in the ListSerializer queryset, but
        # we need to re-generate fields each time due to their being dynamic). implementation per
        # https://stackoverflow.com/questions/50290390/list-serializer-with-dynamic-fields-in-django-rest-framework
        try:
            del self.fields
        except AttributeError:
            pass
        self.fields

        self.add_optional_fields(timezero)
        return super().to_representation(timezero)


    def add_optional_fields(self, timezero):
        # dynamically add optional fields - see https://www.django-rest-framework.org/api-guide/serializers/#dynamically-modifying-fields .
        # note: we exclude 'niceties' like allow_null, help_text, required, style, etc.

        # add season_name
        if timezero.is_season_start:
            self.fields['season_name'] = CharField()


class ProjectSerializer(serializers.HyperlinkedModelSerializer):
    time_interval_type = serializers.SerializerMethodField()
    truth = serializers.SerializerMethodField()

    models = serializers.SerializerMethodField()  # HyperlinkedRelatedField did not allow excluding non-oracle models
    units = serializers.HyperlinkedRelatedField(view_name='api-unit-detail', many=True, read_only=True)
    targets = serializers.HyperlinkedRelatedField(view_name='api-target-detail', many=True, read_only=True)
    timezeros = serializers.HyperlinkedRelatedField(view_name='api-timezero-detail', many=True, read_only=True)


    class Meta:
        model = Project
        fields = ('id', 'url', 'owner', 'is_public', 'name', 'description', 'home_url', 'logo_url', 'core_data',
                  'time_interval_type', 'visualization_y_label', 'truth', 'model_owners', 'models', 'units', 'targets',
                  'timezeros',)
        extra_kwargs = {
            'url': {'view_name': 'api-project-detail'},
            'owner': {'view_name': 'api-user-detail'},
            'model_owners': {'view_name': 'api-user-detail'},
        }


    def get_models(self, project):
        # per [Possibility to filter HyperlinkedIdentityField with many=True with queryset](https://github.com/encode/django-rest-framework/issues/3932)
        request = self.context['request']
        models = []
        for forecast_model in project.models.filter(is_oracle=False):
            models.append(reverse('api-model-detail', args=[forecast_model.pk], request=request))
        return models


    def get_time_interval_type(self, project):
        return project.time_interval_type_as_str()


    def get_truth(self, project):
        request = self.context['request']
        return reverse('api-truth-detail', args=[project.pk], request=request)


class TruthSerializer(serializers.ModelSerializer):
    project = serializers.SerializerMethodField()
    source = serializers.SerializerMethodField()
    created_at = serializers.SerializerMethodField()
    issued_at = serializers.SerializerMethodField()


    class Meta:
        model = Project
        fields = ('id', 'url', 'project', 'source', 'created_at', 'issued_at',)
        extra_kwargs = {
            'url': {'view_name': 'api-truth-detail'},
        }


    def get_project(self, project):
        request = self.context['request']
        return reverse('api-project-detail', args=[project.pk], request=request)


    def get_source(self, project):
        oracle_model = oracle_model_for_project(project)
        last_truth_forecast = oracle_model.forecasts.last() if oracle_model else None
        return last_truth_forecast.source if last_truth_forecast else None


    def get_created_at(self, project):
        oracle_model = oracle_model_for_project(project)
        last_truth_forecast = oracle_model.forecasts.last() if oracle_model else None
        return last_truth_forecast.created_at.isoformat() if last_truth_forecast else None


    def get_issued_at(self, project):
        oracle_model = oracle_model_for_project(project)
        last_truth_forecast = oracle_model.forecasts.last() if oracle_model else None
        return last_truth_forecast.issued_at.isoformat() if last_truth_forecast else None


class UserSerializer(serializers.ModelSerializer):
    owned_models = serializers.SerializerMethodField()
    projects_and_roles = serializers.SerializerMethodField()


    class Meta:
        model = User
        fields = ('id', 'url', 'username', 'owned_models', 'projects_and_roles',)
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


class JobSerializer(serializers.ModelSerializer):
    user = serializers.HyperlinkedRelatedField(view_name='api-user-detail', read_only=True)
    input_json = serializers.JSONField()  # per https://github.com/dmkoch/django-jsonfield/issues/188
    output_json = serializers.JSONField()  # ""


    class Meta:
        model = Job
        fields = ('id', 'url', 'status', 'user', 'created_at', 'updated_at', 'failure_message',
                  'input_json', 'output_json',)
        extra_kwargs = {
            'url': {'view_name': 'api-job-detail'},
        }


class ForecastModelSerializer(serializers.ModelSerializer):
    owner = serializers.HyperlinkedRelatedField(view_name='api-user-detail', read_only=True)
    project = serializers.HyperlinkedRelatedField(view_name='api-project-detail', read_only=True)
    forecasts = serializers.HyperlinkedRelatedField(view_name='api-forecast-detail', many=True, read_only=True)


    class Meta:
        model = ForecastModel
        fields = ('id', 'url', 'project', 'owner', 'name', 'abbreviation', 'team_name', 'description',
                  'contributors', 'license', 'notes', 'citation', 'methods', 'home_url', 'aux_data_url',
                  'forecasts',)
        extra_kwargs = {
            'url': {'view_name': 'api-model-detail'},
        }


class ForecastSerializer(serializers.ModelSerializer):
    forecast_model = serializers.HyperlinkedRelatedField(view_name='api-model-detail', read_only=True)
    time_zero = TimeZeroSerializer()
    forecast_data = serializers.SerializerMethodField()


    class Meta:
        model = Forecast
        fields = ('id', 'url', 'forecast_model', 'source', 'time_zero', 'created_at', 'issued_at', 'notes',
                  'forecast_data',)
        extra_kwargs = {
            'url': {'view_name': 'api-forecast-detail'},
        }


    def get_forecast_data(self, forecast):
        request = self.context['request']
        return reverse('api-forecast-data', args=[forecast.pk], request=request)
