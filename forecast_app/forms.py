import json

from django import forms
from django.contrib.auth.forms import PasswordChangeForm
from django.contrib.auth.models import User
from django.core.exceptions import ValidationError

from forecast_app.models import ForecastModel
from .models.project import Project


class QueryForm(forms.Form):
    """
    form:
        Query: [JSON text area]
        Type: [v] Forecasts, [ ] Scores
        [Cancel] | [Submit]

    additional info:
        <link to docs>
        <button to fill in an example>
    """
    FORECAST_TYPE = 'forecasts'
    SCORE_TYPE = 'scores'
    TYPE_CHOICES = ((FORECAST_TYPE, 'Forecasts'), (SCORE_TYPE, 'Scores'))

    query = forms.CharField()
    query_type = forms.ChoiceField(choices=TYPE_CHOICES, widget=forms.HiddenInput())


    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        for field_name, field in self.fields.items():
            field.widget.attrs['class'] = 'form-control'

        self.fields['query'].widget = forms.Textarea(attrs={'class': 'form-control'})


    def clean_query(self):
        # the minimum validation is that `query` must be readable as a JSON object
        data = self.cleaned_data['query']
        try:
            query_json = json.loads(data)
            if isinstance(query_json, dict):
                return data  # return the cleaned data
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
