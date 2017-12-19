from django import forms

from forecast_app.models import ForecastModel
from .models.project import Project


class ProjectForm(forms.ModelForm):
    class Meta:
        model = Project

        fields = ('name', 'is_public', 'description', "home_url", 'core_data', 'config_dict', 'model_owners')


class ForecastModelForm(forms.ModelForm):
    class Meta:
        model = ForecastModel

        fields = ('name', 'description', "home_url", "aux_data_url")
