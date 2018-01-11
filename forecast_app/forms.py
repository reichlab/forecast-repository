from django import forms

from forecast_app.models import ForecastModel
from .models.project import Project


class ProjectForm(forms.ModelForm):
    class Meta:
        model = Project

        fields = ('name', 'is_public', 'description', "home_url", 'core_data', 'config_dict', 'model_owners')


    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        for field_name, field in self.fields.items():
            if field_name in ['is_public', 'model_owners']:
                continue

            field.widget.attrs['class'] = 'form-control'

        self.fields['description'].widget = forms.Textarea(attrs={'class': 'form-control'})


class ForecastModelForm(forms.ModelForm):
    class Meta:
        model = ForecastModel

        fields = ('name', 'description', "home_url", "aux_data_url")


    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        for field_name, field in self.fields.items():
            field.widget.attrs['class'] = 'form-control'

        self.fields['description'].widget = forms.Textarea(attrs={'class': 'form-control'})
