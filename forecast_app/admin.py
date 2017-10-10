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
admin.site.register(Forecast)


#
# Project admin
#

class ForecastModelInline(admin.TabularInline):
    model = ForecastModel
    fields = ('name', 'description', 'admin_link')
    readonly_fields = ('admin_link',)
    extra = 0

    def admin_link(self, instance):
        url = reverse('admin:{}_{}_change'.format(instance._meta.app_label, instance._meta.model_name),
                      args=(instance.id,))
        return format_html('<a href="{}">{}</a>', url, str(instance))


class TargetInline(admin.TabularInline):
    model = Target
    extra = 1


class TimeZeroInline(admin.TabularInline):
    model = TimeZero
    ordering = ('timezero_date',)
    extra = 0


@admin.register(Project)
class ProjectAdmin(admin.ModelAdmin):
    inlines = [ForecastModelInline, TargetInline, TimeZeroInline]

    def get_form(self, request, obj=None, **kwargs):
        # make the description widget larger
        form = super(ProjectAdmin, self).get_form(request, obj, **kwargs)
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
        form = super(ForecastModelAdmin, self).get_form(request, obj, **kwargs)
        form.base_fields['description'].widget = AdminTextareaWidget()
        return form
