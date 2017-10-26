from django.contrib import admin
from django.contrib.admin.widgets import AdminTextareaWidget
from django.urls import reverse
from django.utils.html import format_html

from forecast_app.models.forecast import Forecast
from forecast_app.models.forecast_model import ForecastModel
from forecast_app.models.project import Project
from forecast_app.models.project import Target
from forecast_app.models.project import TimeZero


#
# minimal registration of Models
#

# todo admin.site.register(ForecastData) ?
# admin.site.register(Project)
admin.site.register(TimeZero)
admin.site.register(Target)


# admin.site.register(ForecastModel)
# admin.site.register(Forecast)


#
# Project admin
#

class ForecastModelInline(admin.TabularInline):
    model = ForecastModel
    fields = ('name', 'description', 'admin_link')
    readonly_fields = ('admin_link',)
    classes = ('collapse',)
    extra = 0


    def admin_link(self, instance):
        url = reverse('admin:{}_{}_change'.format(instance._meta.app_label, instance._meta.model_name),
                      args=(instance.id,))
        return format_html('<a href="{}">Link</a>', url)


    admin_link.short_description = 'admin'


class TargetInline(admin.TabularInline):
    model = Target
    classes = ('collapse',)
    extra = 1


class TimeZeroInline(admin.TabularInline):
    model = TimeZero
    ordering = ('timezero_date',)
    classes = ('collapse',)
    extra = 0


@admin.register(Project)
class ProjectAdmin(admin.ModelAdmin):
    """
    """

    inlines = [ForecastModelInline, TargetInline, TimeZeroInline]

    readonly_fields = ('csv_filename_and_form',)

    list_display = ('name', 'truncated_description', 'num_models', 'num_forecasts', 'num_rows')

    fields = ('name', 'description', 'url', 'core_data', 'config_dict', 'csv_filename_and_form')


    def csv_filename_and_form(self, project):
        # todo use a proper Django form

        # return format_html('{} <button>Delete</button>', project.csv_filename) if project.csv_filename \
        #     else format_html('<small>[no template]</small> <button>Upload</button>')

        return format_html(
            format_html('{} <form><button>Preview</button> <button>Delete</button></form>', project.csv_filename)
            if project.csv_filename else
            format_html('<small>[no template]</small> <form><input type="file"> <button>Upload</button></form></form>'))


    csv_filename_and_form.short_description = 'csv template'


    def truncated_description(self, project):
        max_descr_len = 60
        return project.description[:max_descr_len] + ('...' if len(project.description) > max_descr_len else '')


    truncated_description.short_description = 'description'


    def num_models(self, project):
        return project.get_summary_counts()[0]


    num_models.short_description = 'models'


    def num_forecasts(self, project):
        return project.get_summary_counts()[1]


    num_forecasts.short_description = 'forecasts'


    def num_rows(self, project):
        return "{:,}".format(project.get_summary_counts()[2])


    num_rows.short_description = 'rows'


    def get_form(self, request, project=None, **kwargs):
        # make the description widget larger
        form = super().get_form(request, project, **kwargs)
        form.base_fields['description'].widget = AdminTextareaWidget()
        return form


#
# ForecastModel admin
#

class ForecastInline(admin.TabularInline):
    model = Forecast
    ordering = ('time_zero__timezero_date',)
    extra = 1


@admin.register(ForecastModel)
class ForecastModelAdmin(admin.ModelAdmin):
    inlines = [ForecastInline]
    fields = ('project', 'name', 'description', 'url', 'auxiliary_data')


    def get_form(self, request, obj=None, **kwargs):
        # make the description widget larger
        form = super().get_form(request, obj, **kwargs)
        form.base_fields['description'].widget = AdminTextareaWidget()
        return form


#
# Forecast admin
#

@admin.register(Forecast)
class ForecastAdmin(admin.ModelAdmin):
    readonly_fields = ('csv_filename',)
