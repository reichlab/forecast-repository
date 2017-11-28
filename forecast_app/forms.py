from django import forms

from forecast_app.models import ForecastModel
from .models.project import Project


class ProjectForm(forms.ModelForm):
    class Meta:
        model = Project

        fields = ('name', 'description', 'url', 'core_data', 'config_dict', 'model_owners')


class ForecastModelForm(forms.ModelForm):
    class Meta:
        model = ForecastModel

        fields = ('name', 'description', 'url', 'auxiliary_data')
