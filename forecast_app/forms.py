from django import forms

from .models.project import Project


class ProjectForm(forms.ModelForm):
    class Meta:
        model = Project

        fields = ('name', 'description', 'url', 'core_data', 'config_dict')  # todo others
