import json

from django import forms
from django.contrib.auth.forms import PasswordChangeForm
from django.contrib.auth.models import User
from django.core.exceptions import ValidationError

from forecast_app.models import ForecastModel
from utils.project_queries import validate_forecasts_query, validate_truth_query
from .models.project import Project


class QueryForm(forms.Form):
    """
    A form that can query forecasts or truth. Fields and buttons:
        Query: [JSON text area]
        [Cancel] | [Submit]

    additional info on page:
        <link to docs>
        <button to fill in an example>
    """
    query = forms.CharField(help_text='Enter the JSON query to execute')


    def __init__(self, project, query_type, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.project = project
        self.query_type = query_type
        for field_name, field in self.fields.items():
            field.widget.attrs['class'] = 'form-control'

        self.fields['query'].widget = forms.Textarea(attrs={'class': 'form-control'})


    def clean_query(self):
        from .views import QueryType  # avoid circular imports

        cleaned_query_data = self.cleaned_data['query']
        try:
            query_json = json.loads(cleaned_query_data)
            if isinstance(query_json, dict):  # must be a JSON object
                validation_fcn = {QueryType.FORECASTS: validate_forecasts_query,
                                  QueryType.TRUTH: validate_truth_query,
                                  }[self.query_type]
                error_messages, _ = validation_fcn(self.project, query_json)
                if error_messages:  # invalid query
                    raise ValidationError(error_messages)
                else:
                    return cleaned_query_data
            else:
                raise ValidationError(f"Query was not a JSON object (was a python {type(query_json).__name__!r}, "
                                      f"not 'dict')")
        except json.decoder.JSONDecodeError as jde:
            raise ValidationError(f"Query was not valid JSON: {jde!r}")


class ProjectForm(forms.ModelForm):
    class Meta:
        model = Project

        fields = ('name', 'is_public', 'time_interval_type', 'description',
                  'home_url', 'core_data', 'logo_url', 'model_owners',)


    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        for field_name, field in self.fields.items():
            if field_name in ['is_public', 'model_owners']:
                continue

            field.widget.attrs['class'] = 'form-control'

        self.fields['name'].widget = forms.TextInput(
            attrs={'class': 'form-control'})


class UserModelForm(forms.ModelForm):
    class Meta:
        model = User

        fields = ('email', 'first_name', 'last_name',)


    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        for field_name, field in self.fields.items():
            field.widget.attrs['class'] = 'form-control'


class UserPasswordChangeForm(PasswordChangeForm):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        for field_name, field in self.fields.items():
            field.widget.attrs['class'] = 'form-control'


class ForecastModelForm(forms.ModelForm):
    class Meta:
        model = ForecastModel

        fields = ('name', 'abbreviation', 'team_name', 'description', 'contributors', 'license', 'notes',
                  'citation', 'methods', 'home_url', 'aux_data_url',)


    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        for field_name, field in self.fields.items():
            field.widget.attrs['class'] = 'form-control'

        self.fields['name'].widget = forms.TextInput(
            attrs={'class': 'form-control'})
        self.fields['abbreviation'].widget = forms.TextInput(
            attrs={'class': 'form-control'})
        self.fields['team_name'].widget = forms.TextInput(
            attrs={'class': 'form-control'})
        self.fields['license'].widget = forms.Select(choices=ForecastModel.LICENSE_CHOICES,
                                                     attrs={'class': 'form-control'})
