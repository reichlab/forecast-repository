from django import forms
from django.contrib.auth.forms import PasswordChangeForm
from django.contrib.auth.models import User

from forecast_app.models import ForecastModel
from .models.project import Project


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
